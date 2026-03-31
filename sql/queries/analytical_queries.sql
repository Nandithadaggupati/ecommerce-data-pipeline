-- Query 1: Top 10 Products by Revenue
SELECT
    p.product_name,
    p.category,
    SUM(f.line_total) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    AVG(f.unit_price) AS avg_price
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 2: Monthly Sales Trend
SELECT 
    d.year || '-' || LPAD(d.month::text, 2, '0') AS year_month,
    SUM(f.line_total) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    SUM(f.line_total) / COUNT(DISTINCT f.transaction_id) AS average_order_value,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Query 3: Customer Segmentation Analysis
WITH customer_totals AS (
    SELECT customer_key, SUM(line_total) AS total_spent, COUNT(DISTINCT transaction_id) AS tx_count
    FROM warehouse.fact_sales
    GROUP BY customer_key
),
segmented AS (
    SELECT 
        CASE 
            WHEN total_spent < 1000 THEN '$0-$1,000'
            WHEN total_spent < 5000 THEN '$1,000-$5,000'
            WHEN total_spent < 10000 THEN '$5,000-$10,000'
            ELSE '$10,000+'
        END AS spending_segment,
        total_spent,
        tx_count
    FROM customer_totals
)
SELECT 
    spending_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    SUM(total_spent) / SUM(tx_count) AS avg_transaction_value
FROM segmented
GROUP BY spending_segment
ORDER BY total_revenue DESC;

-- Query 4: Category Performance
SELECT 
    p.category,
    SUM(f.line_total) AS total_revenue,
    SUM(f.profit) AS total_profit,
    (SUM(f.profit) / NULLIF(SUM(f.line_total), 0)) * 100 AS profit_margin_pct,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Query 5: Payment Method Distribution
WITH totals AS (
    SELECT SUM(line_total) AS grand_total_revenue, COUNT(DISTINCT transaction_id) AS grand_total_tx
    FROM warehouse.fact_sales
)
SELECT 
    m.payment_method_name AS payment_method,
    COUNT(DISTINCT f.transaction_id) AS transaction_count,
    SUM(f.line_total) AS total_revenue,
    (COUNT(DISTINCT f.transaction_id)::DECIMAL / t.grand_total_tx) * 100 AS pct_of_transactions,
    (SUM(f.line_total) / t.grand_total_revenue) * 100 AS pct_of_revenue
FROM warehouse.fact_sales f
JOIN warehouse.dim_payment_method m ON f.payment_method_key = m.payment_method_key
CROSS JOIN totals t
GROUP BY m.payment_method_name, t.grand_total_tx, t.grand_total_revenue
ORDER BY total_revenue DESC;

-- Query 6: Geographic Analysis
SELECT 
    c.state,
    SUM(f.line_total) AS total_revenue,
    COUNT(DISTINCT f.customer_key) AS total_customers,
    SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.customer_key), 0) AS avg_revenue_per_customer
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.state
ORDER BY total_revenue DESC;

-- Query 7: Customer Lifetime Value (CLV)
SELECT 
    c.customer_id,
    c.full_name,
    SUM(f.line_total) AS total_spent,
    COUNT(DISTINCT f.transaction_id) AS transaction_count,
    EXTRACT(DAY FROM CURRENT_DATE - c.registration_date) AS days_since_registration,
    SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.transaction_id), 0) AS avg_order_value
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_id, c.full_name, c.registration_date
ORDER BY total_spent DESC
LIMIT 100;

-- Query 8: Product Profitability Analysis
SELECT 
    p.product_name,
    p.category,
    SUM(f.profit) AS total_profit,
    (SUM(f.profit) / NULLIF(SUM(f.line_total), 0)) * 100 AS profit_margin,
    SUM(f.line_total) AS revenue,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category
ORDER BY total_profit DESC
LIMIT 100;

-- Query 9: Day of Week Sales Pattern
WITH daily_stats AS (
    SELECT 
        d.day_name,
        d.date_key,
        SUM(f.line_total) AS daily_revenue,
        COUNT(DISTINCT f.transaction_id) AS daily_transactions
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
    GROUP BY d.day_name, d.date_key
)
SELECT 
    day_name,
    AVG(daily_revenue) AS avg_daily_revenue,
    AVG(daily_transactions) AS avg_daily_transactions,
    SUM(daily_revenue) AS total_revenue
FROM daily_stats
GROUP BY day_name
ORDER BY sum(daily_revenue) DESC;

-- Query 10: Discount Impact Analysis
WITH discount_buckets AS (
    SELECT 
        *,
        CASE
            WHEN (discount_amount / NULLIF((quantity * unit_price), 0)) * 100 = 0 THEN '0%'
            WHEN (discount_amount / NULLIF((quantity * unit_price), 0)) * 100 <= 10 THEN '1-10%'
            WHEN (discount_amount / NULLIF((quantity * unit_price), 0)) * 100 <= 25 THEN '11-25%'
            WHEN (discount_amount / NULLIF((quantity * unit_price), 0)) * 100 <= 50 THEN '26-50%'
            ELSE '50%+'
        END AS discount_range,
        (discount_amount / NULLIF((quantity * unit_price), 0)) * 100 AS discount_pct
    FROM warehouse.fact_sales
)
SELECT 
    discount_range,
    AVG(discount_pct) AS avg_discount_pct,
    SUM(quantity) AS total_quantity_sold,
    SUM(line_total) AS total_revenue,
    AVG(line_total) AS avg_line_total
FROM discount_buckets
GROUP BY discount_range
ORDER BY 
    CASE discount_range
        WHEN '0%' THEN 1
        WHEN '1-10%' THEN 2
        WHEN '11-25%' THEN 3
        WHEN '26-50%' THEN 4
        ELSE 5
    END;
