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
	"sync"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/shippingservice/genproto"
	"github.com/XSAM/otelsql"
	"github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// leakedConnsPerOrder is how many MySQL connections leakConnections strands
// on every ShipOrder call. There is intentionally no environment-variable
// switch for this — it fires unconditionally, the same way a real connection
// leak would in production code that forgot a Close(). Tune this constant
// (and rebuild) to control how many orders it takes to exhaust
// max_connections on the shared MySQL instance.
const leakedConnsPerOrder = 2

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

// shipmentStore persists shipments to MySQL. A nil db means MySQL wasn't
// configured; SaveShipment becomes a no-op so ShipOrder keeps working
// without it.
type shipmentStore struct {
	db  *sql.DB
	cfg *mysql.Config
}

// leakedPools keeps the leaked *sql.DB values reachable so the Go runtime
// cannot collect them. database/sql sets no finalizers, so a dropped pool's
// sockets would stay open anyway, but holding a reference makes the leak
// explicit and keeps it out of reach of any future GC-based cleanup.
var (
	leakedPoolsMu sync.Mutex
	leakedPools   []*sql.DB
)

func newShipmentStore() (*shipmentStore, error) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		log.Info("MYSQL_HOST not set, shipments will not be persisted")
		return &shipmentStore{}, nil
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
	// so every query/exec emits a span. Query text is recorded as db.query.text
	// by default; all SQL here uses ? placeholders, so parameter values are
	// never included.
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

	log.Info("connected to MySQL for shipment persistence")
	return &shipmentStore{db: db, cfg: cfg}, nil
}

// leakConnections opens a fresh connection pool and deliberately never closes
// it, forcing MySQL to hold onto the connections until this process exits.
//
// This reproduces one of the most common real-world database outages: a code
// path that builds a per-request pool instead of reusing the process-wide one
// and forgets the corresponding Close(). Each call strands
// leakedConnsPerOrder connections, so the server's connection count climbs
// step by step until max_connections is reached and every service sharing the
// instance starts getting "Too many connections".
//
// Restarting the process releases everything, because the sockets are owned by
// the process rather than by any object the runtime tracks.
func (s *shipmentStore) leakConnections(ctx context.Context) {
	if s == nil || s.cfg == nil {
		return
	}

	dsn := s.cfg.FormatDSN()
	db, err := otelsql.Open("mysql", dsn,
		otelsql.WithAttributes(mysqlSpanAttributes(dsn, s.cfg.DBName)...),
		otelsql.WithSpanOptions(otelsql.SpanOptions{DisableErrSkip: true}))
	if err != nil {
		log.Warnf("leak: failed to open extra MySQL pool: %v", err)
		return
	}

	// Force the pool to actually establish (and keep) the connections. Without
	// pinning MaxIdleConns the pool would close idle sockets again and the leak
	// would not accumulate.
	db.SetMaxOpenConns(leakedConnsPerOrder)
	db.SetMaxIdleConns(leakedConnsPerOrder)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	opened := 0
	conns := make([]*sql.Conn, 0, leakedConnsPerOrder)
	for i := 0; i < leakedConnsPerOrder; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			log.Warnf("leak: failed to open connection %d: %v", i+1, err)
			break
		}
		conns = append(conns, c)
		opened++
	}
	// Returning the *sql.Conn handles to the pool leaves the underlying sockets
	// established and idle — held by the pool that is never closed.
	for _, c := range conns {
		_ = c.Close()
	}

	leakedPoolsMu.Lock()
	leakedPools = append(leakedPools, db)
	total := len(leakedPools) * leakedConnsPerOrder
	leakedPoolsMu.Unlock()

	log.Warnf("leak: stranded %d MySQL connections (leaked pools=%d, approx total leaked=%d)",
		opened, len(leakedPools), total)
}

func (s *shipmentStore) SaveShipment(ctx context.Context, trackingID string, addr *pb.Address, itemCount int) error {
	if s == nil || s.db == nil {
		return nil
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO shipments (tracking_id, street_address, city, state, country, zip_code, item_count)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		trackingID, addr.GetStreetAddress(), addr.GetCity(), addr.GetState(), addr.GetCountry(), addr.GetZipCode(), itemCount,
	)
	if err != nil {
		return fmt.Errorf("failed to insert shipment %q: %w", trackingID, err)
	}
	return nil
}
