-- Data Platform - Warehouse Schema Initialization
-- Creates all schemas and core tables for the medallion architecture

-- ─── Create Schemas ───
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS seeds;

-- ─── Raw Layer Tables ───

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50),
    status VARCHAR(20) DEFAULT 'pending',
    amount DECIMAL(12, 2),
    quantity INTEGER DEFAULT 1,
    currency VARCHAR(3) DEFAULT 'USD',
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    _source_file VARCHAR(500),
    _ingestion_timestamp TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    price DECIMAL(12, 2),
    cost DECIMAL(12, 2),
    sku VARCHAR(50),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP,
    _ingestion_timestamp TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    region VARCHAR(50),
    country VARCHAR(50),
    tier VARCHAR(20) DEFAULT 'standard',
    created_at TIMESTAMP,
    _ingestion_timestamp TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.events (
    event_id VARCHAR(100) PRIMARY KEY,
    event_type VARCHAR(50),
    source VARCHAR(100),
    timestamp TIMESTAMP,
    payload JSONB,
    metadata JSONB,
    _ingestion_timestamp TIMESTAMP DEFAULT NOW()
);

-- ─── Watermark Tracking ───
CREATE TABLE IF NOT EXISTS metadata.watermarks (
    pipeline_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    watermark_value VARCHAR(100),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (pipeline_name, table_name)
);

-- ─── Pipeline Run Log ───
CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP DEFAULT NOW(),
    end_time TIMESTAMP,
    rows_processed INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB DEFAULT '{}'
);

-- ─── Indexes ───
CREATE INDEX IF NOT EXISTS idx_orders_customer ON raw.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON raw.orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created ON raw.orders(created_at);
CREATE INDEX IF NOT EXISTS idx_products_category ON raw.products(category);
CREATE INDEX IF NOT EXISTS idx_customers_email ON raw.customers(email);
CREATE INDEX IF NOT EXISTS idx_events_type ON raw.events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON raw.events(timestamp);
