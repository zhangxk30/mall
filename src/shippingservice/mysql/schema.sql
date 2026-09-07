-- Schema for shippingservice's shipment persistence.
-- Column names/types map directly to what shipment_store.go writes.

CREATE TABLE IF NOT EXISTS shipments (
    tracking_id VARCHAR(64) NOT NULL PRIMARY KEY,
    street_address VARCHAR(255),
    city VARCHAR(128),
    state VARCHAR(128),
    country VARCHAR(128),
    zip_code INT,
    item_count INT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_shipments_created_at (created_at)
);
