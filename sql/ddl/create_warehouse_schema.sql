CREATE SCHEMA IF NOT EXISTS warehouse;

-- ========== DIMENSION TABLES ==========

-- Slowly Changing Dimension Type 2: Customer
CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
  customer_sk SERIAL PRIMARY KEY,
  customer_id VARCHAR(20) NOT NULL,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(255),
  phone VARCHAR(50),
  city VARCHAR(100),
  state VARCHAR(100),
  country VARCHAR(100),
  age_group VARCHAR(20),
  registration_date DATE,
  is_current BOOLEAN DEFAULT TRUE,
  effective_date DATE NOT NULL,
  end_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_customers_id
  ON warehouse.dim_customers (customer_id, is_current);

-- Slowly Changing Dimension Type 2: Product
CREATE TABLE IF NOT EXISTS warehouse.dim_products (
  product_sk SERIAL PRIMARY KEY,
  product_id VARCHAR(20) NOT NULL,
  product_name VARCHAR(255),
  category VARCHAR(100),
  sub_category VARCHAR(100),
  price DECIMAL(12,2),
  cost DECIMAL(12,2),
  brand VARCHAR(255),
  is_current BOOLEAN DEFAULT TRUE,
  effective_date DATE NOT NULL,
  end_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_products_id
  ON warehouse.dim_products (product_id, is_current);

-- Date Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
  date_sk SERIAL PRIMARY KEY,
  date_id DATE NOT NULL UNIQUE,
  day_of_month INT,
  day_of_week INT,
  day_name VARCHAR(20),
  month INT,
  month_name VARCHAR(20),
  quarter INT,
  year INT,
  week_of_year INT,
  is_weekend BOOLEAN,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_date_id
  ON warehouse.dim_date (date_id);

-- Payment Method Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_payment_method (
  payment_method_sk SERIAL PRIMARY KEY,
  payment_method_id VARCHAR(50) NOT NULL UNIQUE,
  payment_method_name VARCHAR(100),
  category VARCHAR(50),
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========== FACT TABLE ==========

CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
  sales_fact_sk BIGSERIAL PRIMARY KEY,
  transaction_id VARCHAR(20) NOT NULL,
  item_id VARCHAR(20) NOT NULL,
  customer_sk INT NOT NULL REFERENCES warehouse.dim_customers(customer_sk),
  product_sk INT NOT NULL REFERENCES warehouse.dim_products(product_sk),
  date_sk INT NOT NULL REFERENCES warehouse.dim_date(date_sk),
  payment_method_sk INT REFERENCES warehouse.dim_payment_method(payment_method_sk),
  quantity INT NOT NULL,
  unit_price DECIMAL(12,2) NOT NULL,
  discount_percentage DECIMAL(5,2) DEFAULT 0,
  line_total DECIMAL(12,2) NOT NULL,
  transaction_total DECIMAL(12,2),
  shipping_address TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer
  ON warehouse.fact_sales (customer_sk);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product
  ON warehouse.fact_sales (product_sk);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date
  ON warehouse.fact_sales (date_sk);

CREATE INDEX IF NOT EXISTS idx_fact_sales_transaction
  ON warehouse.fact_sales (transaction_id);

-- ========== AGGREGATE TABLES ==========

CREATE TABLE IF NOT EXISTS warehouse.agg_daily_sales (
  agg_sk SERIAL PRIMARY KEY,
  date_sk INT NOT NULL REFERENCES warehouse.dim_date(date_sk),
  total_sales DECIMAL(18,2),
  total_units_sold INT,
  transaction_count INT,
  customer_count INT,
  avg_transaction_value DECIMAL(12,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agg_daily_sales_date
  ON warehouse.agg_daily_sales (date_sk);

CREATE TABLE IF NOT EXISTS warehouse.agg_product_sales (
  agg_sk SERIAL PRIMARY KEY,
  product_sk INT NOT NULL REFERENCES warehouse.dim_products(product_sk),
  total_sales DECIMAL(18,2),
  total_units_sold INT,
  transaction_count INT,
  avg_discount DECIMAL(5,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agg_product_sales_product
  ON warehouse.agg_product_sales (product_sk);

CREATE TABLE IF NOT EXISTS warehouse.agg_customer_lifetime (
  agg_sk SERIAL PRIMARY KEY,
  customer_sk INT NOT NULL REFERENCES warehouse.dim_customers(customer_sk),
  total_spent DECIMAL(18,2),
  total_units_purchased INT,
  transaction_count INT,
  first_purchase_date DATE,
  last_purchase_date DATE,
  avg_transaction_value DECIMAL(12,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agg_customer_lifetime_customer
  ON warehouse.agg_customer_lifetime (customer_sk);
