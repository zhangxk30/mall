-- Schema for checkoutservice's order persistence.
-- Column names/types map directly to what order_store.go writes/reads.
-- `status` is updated by orderprocessorservice after consuming the
-- corresponding Kafka message (PLACED -> PROCESSED).

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64) NOT NULL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    user_currency VARCHAR(8),
    email VARCHAR(255),
    total_currency_code VARCHAR(8),
    total_units BIGINT,
    total_nanos INT,
    shipping_tracking_id VARCHAR(64),
    shipping_cost_units BIGINT,
    shipping_cost_nanos INT,
    shipping_street_address VARCHAR(255),
    shipping_city VARCHAR(128),
    shipping_state VARCHAR(128),
    shipping_country VARCHAR(128),
    shipping_zip_code INT,
    status VARCHAR(32) NOT NULL DEFAULT 'PLACED',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_orders_user_id (user_id),
    INDEX idx_orders_status (status)
);

CREATE TABLE IF NOT EXISTS order_items (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(32) NOT NULL,
    quantity INT NOT NULL,
    cost_currency_code VARCHAR(8),
    cost_units BIGINT,
    cost_nanos INT,
    INDEX idx_order_items_order_id (order_id)
);
