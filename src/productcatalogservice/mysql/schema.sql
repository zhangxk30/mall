-- Schema for productcatalogservice's MySQL-backed product catalog.
-- Column names/types map directly to the fields read by loadCatalogFromMySQL
-- in ../catalog_loader.go.

CREATE TABLE IF NOT EXISTS products (
    id VARCHAR(32) NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    picture VARCHAR(255),
    price_currency_code VARCHAR(8),
    price_units BIGINT,
    price_nanos INT,
    categories VARCHAR(255)
);
