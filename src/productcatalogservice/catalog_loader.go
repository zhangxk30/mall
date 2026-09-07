// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/alloydbconn"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/XSAM/otelsql"
	"github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// The MySQL connection pool is process-wide: loadCatalogFromMySQL runs on every
// cache miss, and re-opening a pool per call would leak connections and
// re-register the instrumented driver each time.
var (
	mysqlDBOnce sync.Once
	mysqlDB     *sql.DB
	mysqlDBErr  error
)

// mysqlSpanAttributes tags MySQL spans so the APM backend recognizes them as
// database calls and renders a named MySQL node on the topology graph.
//
//   - db.system / db.system.name: both conventions are emitted because the two
//     coexist across OTel versions and backends differ in which one they read.
//   - server.address / server.port: WITHOUT these the APM topology shows the
//     node as "unknown", since it can't tell which instance was called.
//     otelsql.AttributesFromDSN is opt-in — it is never applied automatically.
//     It extracts only host/port and never the DSN credentials.
//   - db.namespace: the database name, so orders/shipping/productcatalog are
//     distinguishable on the same instance.
func mysqlSpanAttributes(dsn, dbName string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.DBSystemMySQL,
		attribute.String("db.system.name", "mysql"),
	}
	attrs = append(attrs, otelsql.AttributesFromDSN(dsn)...)
	if dbName != "" {
		attrs = append(attrs, semconv.DBNamespace(dbName))
	}
	return attrs
}

func loadCatalog(ctx context.Context, catalog *pb.ListProductsResponse) error {
	catalogMutex.Lock()
	defer catalogMutex.Unlock()

	if os.Getenv("MYSQL_HOST") != "" {
		return loadCatalogFromMySQL(ctx, catalog)
	}

	if os.Getenv("ALLOYDB_CLUSTER_NAME") != "" {
		return loadCatalogFromAlloyDB(ctx, catalog)
	}

	return loadCatalogFromLocalFile(catalog)
}

func loadCatalogFromLocalFile(catalog *pb.ListProductsResponse) error {
	log.Info("loading catalog from local products.json file...")

	catalogJSON, err := os.ReadFile("products.json")
	if err != nil {
		log.Warnf("failed to open product catalog json file: %v", err)
		return err
	}

	if err := jsonpb.Unmarshal(bytes.NewReader(catalogJSON), catalog); err != nil {
		log.Warnf("failed to parse the catalog JSON: %v", err)
		return err
	}

	log.Info("successfully parsed product catalog json")
	return nil
}

func getSecretPayload(project, secret, version string) (string, error) {
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Warnf("failed to create SecretManager client: %v", err)
		return "", err
	}
	defer client.Close()

	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/%s", project, secret, version),
	}

	// Call the API.
	result, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		log.Warnf("failed to access SecretVersion: %v", err)
		return "", err
	}

	return string(result.Payload.Data), nil
}

func loadCatalogFromAlloyDB(ctx context.Context, catalog *pb.ListProductsResponse) error {
	log.Info("loading catalog from AlloyDB...")

	projectID := os.Getenv("PROJECT_ID")
	region := os.Getenv("REGION")
	pgClusterName := os.Getenv("ALLOYDB_CLUSTER_NAME")
	pgInstanceName := os.Getenv("ALLOYDB_INSTANCE_NAME")
	pgDatabaseName := os.Getenv("ALLOYDB_DATABASE_NAME")
	pgTableName := os.Getenv("ALLOYDB_TABLE_NAME")
	pgSecretName := os.Getenv("ALLOYDB_SECRET_NAME")

	pgPassword, err := getSecretPayload(projectID, pgSecretName, "latest")
	if err != nil {
		return err
	}

	dialer, err := alloydbconn.NewDialer(ctx)
	if err != nil {
		log.Warnf("failed to set-up dialer connection: %v", err)
		return err
	}
	cleanup := func() error { return dialer.Close() }
	defer cleanup()

	dsn := fmt.Sprintf(
		"user=%s password=%s dbname=%s sslmode=disable",
		"postgres", pgPassword, pgDatabaseName,
	)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Warnf("failed to parse DSN config: %v", err)
		return err
	}

	pgInstanceURI := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/instances/%s", projectID, region, pgClusterName, pgInstanceName)
	config.ConnConfig.DialFunc = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		return dialer.Dial(ctx, pgInstanceURI)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Warnf("failed to set-up pgx pool: %v", err)
		return err
	}
	defer pool.Close()

	query := "SELECT id, name, description, picture, price_usd_currency_code, price_usd_units, price_usd_nanos, categories FROM " + pgTableName
	rows, err := pool.Query(ctx, query)
	if err != nil {
		log.Warnf("failed to query database: %v", err)
		return err
	}
	defer rows.Close()

	catalog.Products = catalog.Products[:0]
	for rows.Next() {
		product := &pb.Product{}
		product.PriceUsd = &pb.Money{}

		var categories string
		err = rows.Scan(&product.Id, &product.Name, &product.Description,
			&product.Picture, &product.PriceUsd.CurrencyCode, &product.PriceUsd.Units,
			&product.PriceUsd.Nanos, &categories)
		if err != nil {
			log.Warnf("failed to scan query result row: %v", err)
			return err
		}
		categories = strings.ToLower(categories)
		product.Categories = strings.Split(categories, ",")

		catalog.Products = append(catalog.Products, product)
	}

	log.Info("successfully parsed product catalog from AlloyDB")
	return nil
}

// getMySQLDB lazily opens the process-wide instrumented connection pool.
func getMySQLDB(ctx context.Context) (*sql.DB, error) {
	mysqlDBOnce.Do(func() {
		port := os.Getenv("MYSQL_PORT")
		if port == "" {
			port = "3306"
		}

		cfg := mysql.NewConfig()
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("%s:%s", os.Getenv("MYSQL_HOST"), port)
		cfg.User = os.Getenv("MYSQL_USER")
		cfg.Passwd = os.Getenv("MYSQL_PASSWORD")
		cfg.DBName = os.Getenv("MYSQL_DATABASE")
		cfg.ParseTime = true
		cfg.Timeout = 5 * time.Second

		// otelsql.Open is a drop-in replacement for sql.Open that wraps the
		// driver so every query emits a span. Query text is recorded as
		// db.query.text by default.
		dsn := cfg.FormatDSN()
		db, err := otelsql.Open("mysql", dsn,
			otelsql.WithAttributes(mysqlSpanAttributes(dsn, cfg.DBName)...),
			// The MySQL driver returns driver.ErrSkip to decline a fast path,
			// and database/sql then retries via prepare+exec. That is normal
			// control flow, but otelsql would otherwise mark those spans as
			// errors and the APM would show spurious failures.
			otelsql.WithSpanOptions(otelsql.SpanOptions{DisableErrSkip: true}))
		if err != nil {
			mysqlDBErr = err
			return
		}
		db.SetMaxOpenConns(10)
		db.SetConnMaxLifetime(3 * time.Minute)

		if err := db.PingContext(ctx); err != nil {
			db.Close()
			mysqlDBErr = err
			return
		}
		mysqlDB = db
	})
	return mysqlDB, mysqlDBErr
}

func loadCatalogFromMySQL(ctx context.Context, catalog *pb.ListProductsResponse) error {
	log.Info("loading catalog from MySQL...")

	table := os.Getenv("MYSQL_TABLE")
	if table == "" {
		table = "products"
	}

	db, err := getMySQLDB(ctx)
	if err != nil {
		log.Warnf("failed to connect to MySQL: %v", err)
		return err
	}

	query := "SELECT id, name, description, picture, price_currency_code, price_units, price_nanos, categories FROM " + table
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Warnf("failed to query database: %v", err)
		return err
	}
	defer rows.Close()

	catalog.Products = catalog.Products[:0]
	for rows.Next() {
		product := &pb.Product{}
		product.PriceUsd = &pb.Money{}

		var categories string
		err = rows.Scan(&product.Id, &product.Name, &product.Description,
			&product.Picture, &product.PriceUsd.CurrencyCode, &product.PriceUsd.Units,
			&product.PriceUsd.Nanos, &categories)
		if err != nil {
			log.Warnf("failed to scan query result row: %v", err)
			return err
		}
		categories = strings.ToLower(categories)
		product.Categories = strings.Split(categories, ",")

		catalog.Products = append(catalog.Products, product)
	}

	log.Info("successfully parsed product catalog from MySQL")
	return nil
}
