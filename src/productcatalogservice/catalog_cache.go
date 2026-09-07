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
	"encoding/json"
	"os"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

const (
	catalogCacheKey = "catalog:products"
	defaultCacheTTL = 60 * time.Second
)

// catalogCache caches the product catalog in Redis as a cache-aside layer in
// front of the MySQL/AlloyDB/local-file data sources. A nil client (REDIS_ADDR
// unset) means caching is disabled; Get/Set become no-ops.
type catalogCache struct {
	client *redis.Client
	ttl    time.Duration
}

func newCatalogCache() *catalogCache {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		log.Info("REDIS_ADDR not set, catalog caching disabled")
		return &catalogCache{}
	}

	ttl := defaultCacheTTL
	if s := os.Getenv("CATALOG_CACHE_TTL"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			ttl = d
		} else {
			log.Warnf("failed to parse CATALOG_CACHE_TTL %q, using default %v: %v", s, defaultCacheTTL, err)
		}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	// Emit a span per Redis command so cache hits/misses are visible in APM and
	// Redis shows up as a node on the topology graph. WithDBStatement records
	// the command (e.g. "get catalog:products"), matching the SQL-text policy
	// used for MySQL.
	if err := redisotel.InstrumentTracing(client,
		redisotel.WithDBStatement(true),
	); err != nil {
		log.Warnf("failed to instrument Redis tracing: %v", err)
	}

	log.Infof("caching product catalog in Redis at %s (ttl=%v)", addr, ttl)
	return &catalogCache{
		client: client,
		ttl:    ttl,
	}
}

func (c *catalogCache) enabled() bool {
	return c != nil && c.client != nil
}

func (c *catalogCache) Get(ctx context.Context) ([]*pb.Product, bool) {
	if !c.enabled() {
		return nil, false
	}

	val, err := c.client.Get(ctx, catalogCacheKey).Bytes()
	if err != nil {
		if err != redis.Nil {
			log.Warnf("failed to read catalog cache: %v", err)
		}
		return nil, false
	}

	var products []*pb.Product
	if err := json.Unmarshal(val, &products); err != nil {
		log.Warnf("failed to unmarshal cached catalog: %v", err)
		return nil, false
	}
	return products, true
}

func (c *catalogCache) Set(ctx context.Context, products []*pb.Product) {
	if !c.enabled() {
		return
	}

	body, err := json.Marshal(products)
	if err != nil {
		log.Warnf("failed to marshal catalog for cache: %v", err)
		return
	}
	if err := c.client.Set(ctx, catalogCacheKey, body, c.ttl).Err(); err != nil {
		log.Warnf("failed to write catalog cache: %v", err)
	}
}
