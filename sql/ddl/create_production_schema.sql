CREATE SCHEMA IF NOT EXISTS production;

-- ========== PRODUCTION TABLES ==========

CREATE TABLE IF NOT EXISTS production.customers (
  customer_sk SERIAL PRIMARY KEY,
  customer_id VARCHAR(20) UNIQUE NOT NULL,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  email VARCHAR(255) NOT NULL,
  phone VARCHAR(50),
  registration_date DATE,
  city VARCHAR(100),
  state VARCHAR(100),
  country VARCHAR(100),
  age_group VARCHAR(20),
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.products (
  product_sk SERIAL PRIMARY KEY,
  product_id VARCHAR(20) UNIQUE NOT NULL,
  product_name VARCHAR(255) NOT NULL,
  category VARCHAR(100),
  sub_category VARCHAR(100),
  price DECIMAL(12,2) NOT NULL,
  cost DECIMAL(12,2) NOT NULL,
  brand VARCHAR(255),
  stock_quantity INTEGER,
  supplier_id VARCHAR(20),
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.transactions (
  transaction_sk SERIAL PRIMARY KEY,
  transaction_id VARCHAR(20) UNIQUE NOT NULL,
  customer_id VARCHAR(20) NOT NULL,
  transaction_date DATE NOT NULL,
  transaction_time TIME,
  payment_method VARCHAR(50),
  shipping_address TEXT,
  total_amount DECIMAL(12,2) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.transaction_items (
  item_sk SERIAL PRIMARY KEY,
  item_id VARCHAR(20) UNIQUE NOT NULL,
  transaction_id VARCHAR(20) NOT NULL,
  product_id VARCHAR(20) NOT NULL,
  quantity INTEGER NOT NULL,
  unit_price DECIMAL(12,2) NOT NULL,
  discount_percentage DECIMAL(5,2) DEFAULT 0,
  line_total DECIMAL(12,2) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========== CONSTRAINTS ==========

ALTER TABLE production.transactions
  ADD CONSTRAINT fk_production_transactions_customer
  FOREIGN KEY (customer_id)
  REFERENCES production.customers(customer_id);

ALTER TABLE production.transaction_items
  ADD CONSTRAINT fk_production_items_transaction
  FOREIGN KEY (transaction_id)
  REFERENCES production.transactions(transaction_id);

ALTER TABLE production.transaction_items
  ADD CONSTRAINT fk_production_items_product
  FOREIGN KEY (product_id)
  REFERENCES production.products(product_id);

-- ========== INDEXES ==========

CREATE INDEX IF NOT EXISTS idx_production_customers_customer_id
  ON production.customers (customer_id);

CREATE INDEX IF NOT EXISTS idx_production_products_product_id
  ON production.products (product_id, category);

CREATE INDEX IF NOT EXISTS idx_production_transactions_customer_date
  ON production.transactions (customer_id, transaction_date);

CREATE INDEX IF NOT EXISTS idx_production_transaction_items_txn
  ON production.transaction_items (transaction_id);

CREATE INDEX IF NOT EXISTS idx_production_transaction_items_product
  ON production.transaction_items (product_id);
