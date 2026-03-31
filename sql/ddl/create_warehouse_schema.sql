CREATE TABLE warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    full_name VARCHAR(255),
    email VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    age_group VARCHAR(20),
    customer_segment VARCHAR(50),
    registration_date DATE,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

CREATE TABLE warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    price_range VARCHAR(50),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

CREATE TABLE warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    week_of_year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

CREATE TABLE warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(50),
    payment_type VARCHAR(50)
);

CREATE TABLE warehouse.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    payment_method_key INTEGER NOT NULL,
    transaction_id VARCHAR(20),
    quantity INTEGER,
    unit_price DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    line_total DECIMAL(12,2),
    profit DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES warehouse.dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES warehouse.dim_customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES warehouse.dim_products(product_key),
    FOREIGN KEY (payment_method_key) REFERENCES warehouse.dim_payment_method(payment_method_key)
);

CREATE TABLE warehouse.agg_daily_sales (
    date_key INTEGER PRIMARY KEY,
    total_transactions INTEGER,
    total_revenue DECIMAL(15,2),
    total_profit DECIMAL(15,2),
    unique_customers INTEGER
);

CREATE TABLE warehouse.agg_product_performance (
    product_key INTEGER PRIMARY KEY,
    total_quantity_sold INTEGER,
    total_revenue DECIMAL(15,2),
    total_profit DECIMAL(15,2),
    avg_discount_percentage DECIMAL(5,2)
);

CREATE TABLE warehouse.agg_customer_metrics (
    customer_key INTEGER PRIMARY KEY,
    total_transactions INTEGER,
    total_spent DECIMAL(15,2),
    avg_order_value DECIMAL(15,2),
    last_purchase_date DATE
);
