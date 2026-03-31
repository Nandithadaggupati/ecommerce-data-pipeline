DROP TABLE IF EXISTS production.transaction_items CASCADE;
DROP TABLE IF EXISTS production.transactions CASCADE;
DROP TABLE IF EXISTS production.products CASCADE;
DROP TABLE IF EXISTS production.customers CASCADE;

CREATE TABLE production.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(50),
    registration_date DATE,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    age_group VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE production.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    sub_category VARCHAR(100),
    price DECIMAL(12,2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(12,2) NOT NULL CHECK (cost >= 0),
    brand VARCHAR(100),
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
    supplier_id VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE production.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_time TIME NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    shipping_address VARCHAR(500),
    total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES production.customers(customer_id)
);

CREATE TABLE production.transaction_items (
    item_id VARCHAR(20) PRIMARY KEY,
    transaction_id VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(12,2) NOT NULL CHECK (unit_price >= 0),
    discount_percentage DECIMAL(5,2) NOT NULL CHECK (discount_percentage >= 0 AND discount_percentage <= 100),
    line_total DECIMAL(12,2) NOT NULL CHECK (line_total >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES production.transactions(transaction_id),
    FOREIGN KEY (product_id) REFERENCES production.products(product_id)
);

CREATE INDEX idx_transactions_customer ON production.transactions(customer_id);
CREATE INDEX idx_transactions_date ON production.transactions(transaction_date);
CREATE INDEX idx_transaction_items_transaction ON production.transaction_items(transaction_id);
CREATE INDEX idx_transaction_items_product ON production.transaction_items(product_id);
