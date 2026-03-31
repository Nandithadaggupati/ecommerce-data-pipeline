-- Query to find NULL values in mandatory columns
SELECT 'customers' AS table_name, 'email' AS column_name, COUNT(*) AS null_count FROM staging.customers WHERE email IS NULL
UNION ALL
SELECT 'products', 'price', COUNT(*) FROM staging.products WHERE price IS NULL
UNION ALL
SELECT 'transactions', 'customer_id', COUNT(*) FROM staging.transactions WHERE customer_id IS NULL;

-- Query to find duplicate primary keys
SELECT 'customers' AS table_name, COUNT(customer_id) - COUNT(DISTINCT customer_id) AS duplicates FROM staging.customers
UNION ALL
SELECT 'products', COUNT(product_id) - COUNT(DISTINCT product_id) FROM staging.products;

-- Query to find orphan records (LEFT JOIN checking for NULL)
SELECT 'transactions_customers' AS relationship, COUNT(t.transaction_id) AS orphan_count 
FROM staging.transactions t LEFT JOIN staging.customers c ON t.customer_id = c.customer_id WHERE c.customer_id IS NULL
UNION ALL
SELECT 'items_transactions', COUNT(i.item_id)
FROM staging.transaction_items i LEFT JOIN staging.transactions t ON i.transaction_id = t.transaction_id WHERE t.transaction_id IS NULL
UNION ALL
SELECT 'items_products', COUNT(i.item_id)
FROM staging.transaction_items i LEFT JOIN staging.products p ON i.product_id = p.product_id WHERE p.product_id IS NULL;

-- Query to validate calculated fields
SELECT 'line_total_calculation' AS check_name, COUNT(*) AS violation_count
FROM staging.transaction_items
WHERE ABS(line_total - (quantity * unit_price * (1 - discount_percentage/100))) > 0.05;

-- Query to check range constraints
SELECT 'negative_prices' AS check_name, COUNT(*) AS constraint_violations FROM staging.products WHERE price < 0
UNION ALL
SELECT 'negative_quantity', COUNT(*) FROM staging.transaction_items WHERE quantity <= 0;
