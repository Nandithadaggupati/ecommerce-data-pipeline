-- Query 1: Data Freshness Check
SELECT 'staging.customers' AS table_name, MAX(loaded_at) AS latest_record FROM staging.customers
UNION ALL
SELECT 'production.customers', MAX(created_at) FROM production.customers
UNION ALL
SELECT 'warehouse.fact_sales', MAX(created_at) FROM warehouse.fact_sales;

-- Query 2: Volume Trend Analysis
SELECT 
    transaction_date, 
    COUNT(*) as daily_transactions 
FROM production.transactions 
GROUP BY transaction_date 
ORDER BY transaction_date DESC 
LIMIT 30;

-- Query 3: Data Quality Issues
SELECT 'orphan_transactions', COUNT(*) 
FROM production.transactions t LEFT JOIN production.customers c ON t.customer_id = c.customer_id WHERE c.customer_id IS NULL;

-- Query 4: Database Statistics
SELECT relname AS table_name, n_live_tup AS row_count 
FROM pg_stat_user_tables;
