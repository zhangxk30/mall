// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice/genproto"
	"github.com/XSAM/otelsql"
	"github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
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

// orderStore persists placed orders to MySQL. A nil db means MySQL wasn't
// configured; SaveOrder becomes a no-op so checkout keeps working without it.
type orderStore struct {
	db *sql.DB
}

func newOrderStore() (*orderStore, error) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		log.Info("MYSQL_HOST not set, orders will not be persisted")
		return &orderStore{}, nil
	}

	port := os.Getenv("MYSQL_PORT")
	if port == "" {
		port = "3306"
	}

	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	cfg.User = os.Getenv("MYSQL_USER")
	cfg.Passwd = os.Getenv("MYSQL_PASSWORD")
	cfg.DBName = os.Getenv("MYSQL_DATABASE")
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second

	// otelsql.Open is a drop-in replacement for sql.Open that wraps the driver
	// so every query/exec/transaction emits a span. Query text is recorded as
	// db.query.text by default; all SQL here uses ? placeholders, so parameter
	// values are never included.
	dsn := cfg.FormatDSN()
	db, err := otelsql.Open("mysql", dsn,
		otelsql.WithAttributes(mysqlSpanAttributes(dsn, cfg.DBName)...),
		// The MySQL driver returns driver.ErrSkip to decline a fast path, and
		// database/sql then retries via prepare+exec. That is normal control
		// flow, but otelsql would otherwise mark those spans as errors and the
		// APM would show spurious failures on every write.
		otelsql.WithSpanOptions(otelsql.SpanOptions{DisableErrSkip: true}))
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(3 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}

	log.Info("connected to MySQL for order persistence")
	return &orderStore{db: db}, nil
}

// SaveOrder writes the order and its line items in a single transaction.
func (s *orderStore) SaveOrder(ctx context.Context, order *pb.OrderResult, userID, userCurrency, email string, total *pb.Money) error {
	if s == nil || s.db == nil {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	addr := order.GetShippingAddress()
	shippingCost := order.GetShippingCost()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO orders (
			order_id, user_id, user_currency, email,
			total_currency_code, total_units, total_nanos,
			shipping_tracking_id, shipping_cost_units, shipping_cost_nanos,
			shipping_street_address, shipping_city, shipping_state, shipping_country, shipping_zip_code
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		order.GetOrderId(), userID, userCurrency, email,
		total.GetCurrencyCode(), total.GetUnits(), total.GetNanos(),
		order.GetShippingTrackingId(), shippingCost.GetUnits(), shippingCost.GetNanos(),
		addr.GetStreetAddress(), addr.GetCity(), addr.GetState(), addr.GetCountry(), addr.GetZipCode(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert order: %w", err)
	}

	for _, item := range order.GetItems() {
		cost := item.GetCost()
		_, err = tx.ExecContext(ctx, `
			INSERT INTO order_items (order_id, product_id, quantity, cost_currency_code, cost_units, cost_nanos)
			VALUES (?, ?, ?, ?, ?, ?)`,
			order.GetOrderId(), item.GetItem().GetProductId(), item.GetItem().GetQuantity(),
			cost.GetCurrencyCode(), cost.GetUnits(), cost.GetNanos(),
		)
		if err != nil {
			return fmt.Errorf("failed to insert order item %q: %w", item.GetItem().GetProductId(), err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit order transaction: %w", err)
	}
	return nil
}
