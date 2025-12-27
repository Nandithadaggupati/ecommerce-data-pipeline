-- ========== ANALYTICS QUERIES (10+) ==========
-- All queries run on warehouse schema only
-- These power the BI dashboards and provide business insights

-- Q1: Daily Sales Trend (KPI)
SELECT
    dd.date_id,
    dd.day_name,
    dd.month_name,
    SUM(ads.total_sales) as daily_revenue,
    SUM(ads.total_units_sold) as total_units,
    ads.transaction_count,
    ads.avg_transaction_value
FROM warehouse.agg_daily_sales ads
INNER JOIN warehouse.dim_date dd ON ads.date_sk = dd.date_sk
GROUP BY dd.date_id, dd.day_name, dd.month_name, ads.transaction_count, ads.avg_transaction_value
ORDER BY dd.date_id DESC;

-- Q2: Top 10 Products by Revenue
SELECT
    dp.product_sk,
    dp.product_id,
    dp.product_name,
    dp.category,
    aps.total_sales,
    aps.total_units_sold,
    aps.transaction_count,
    ROUND((aps.total_sales / dp.price), 2) as profitability_ratio
FROM warehouse.agg_product_sales aps
INNER JOIN warehouse.dim_products dp ON aps.product_sk = dp.product_sk
WHERE dp.is_current = TRUE
ORDER BY aps.total_sales DESC
LIMIT 10;

-- Q3: Customer Segmentation by Lifetime Value
SELECT
    acl.customer_sk,
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.age_group,
    dc.country,
    acl.total_spent,
    acl.transaction_count,
    acl.avg_transaction_value,
    CASE
        WHEN acl.total_spent >= 5000 THEN 'VIP'
        WHEN acl.total_spent >= 2000 THEN 'Premium'
        WHEN acl.total_spent >= 500 THEN 'Regular'
        ELSE 'New'
    END as customer_segment
FROM warehouse.agg_customer_lifetime acl
INNER JOIN warehouse.dim_customers dc ON acl.customer_sk = dc.customer_sk
WHERE dc.is_current = TRUE
ORDER BY acl.total_spent DESC;

-- Q4: Category Performance Analysis
SELECT
    dp.category,
    COUNT(DISTINCT fs.product_sk) as product_count,
    SUM(fs.line_total) as category_revenue,
    SUM(fs.quantity) as units_sold,
    COUNT(DISTINCT fs.transaction_id) as transactions,
    COUNT(DISTINCT fs.customer_sk) as unique_customers,
    ROUND(AVG(fs.line_total), 2) as avg_line_value,
    ROUND(AVG(fs.discount_percentage), 2) as avg_discount_pct
FROM warehouse.fact_sales fs
INNER JOIN warehouse.dim_products dp ON fs.product_sk = dp.product_sk
WHERE dp.is_current = TRUE
GROUP BY dp.category
ORDER BY category_revenue DESC;

-- Q5: Payment Method Distribution
SELECT
    dpm.payment_method_name,
    dpm.category,
    COUNT(DISTINCT fs.transaction_id) as transaction_count,
    SUM(fs.line_total) as total_revenue,
    COUNT(DISTINCT fs.customer_sk) as customer_count,
    ROUND(100.0 * COUNT(DISTINCT fs.transaction_id) / 
        (SELECT COUNT(DISTINCT transaction_id) FROM warehouse.fact_sales), 2) as pct_of_total
FROM warehouse.fact_sales fs
LEFT JOIN warehouse.dim_payment_method dpm ON fs.payment_method_sk = dpm.payment_method_sk
GROUP BY dpm.payment_method_name, dpm.category
ORDER BY transaction_count DESC;

-- Q6: Month-over-Month Revenue Growth
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    SUM(ads.total_sales) as monthly_revenue,
    LAG(SUM(ads.total_sales)) OVER (ORDER BY dd.year, dd.month) as prev_month_revenue,
    ROUND(
        100.0 * (SUM(ads.total_sales) - LAG(SUM(ads.total_sales)) OVER (ORDER BY dd.year, dd.month)) /
        LAG(SUM(ads.total_sales)) OVER (ORDER BY dd.year, dd.month),
        2
    ) as mom_growth_pct
FROM warehouse.agg_daily_sales ads
INNER JOIN warehouse.dim_date dd ON ads.date_sk = dd.date_sk
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- Q7: Customer Geographic Distribution
SELECT
    dc.country,
    dc.state,
    COUNT(DISTINCT dc.customer_sk) as customer_count,
    SUM(acl.total_spent) as total_revenue,
    ROUND(AVG(acl.avg_transaction_value), 2) as avg_transaction_value,
    COUNT(DISTINCT acl.customer_sk) as active_customers
FROM warehouse.agg_customer_lifetime acl
INNER JOIN warehouse.dim_customers dc ON acl.customer_sk = dc.customer_sk
WHERE dc.is_current = TRUE
GROUP BY dc.country, dc.state
ORDER BY total_revenue DESC;

-- Q8: Product Discount Impact
SELECT
    fs.product_sk,
    dp.product_name,
    dp.category,
    ROUND(AVG(fs.discount_percentage), 2) as avg_discount,
    COUNT(DISTINCT fs.transaction_id) as transactions_with_discount,
    SUM(CASE WHEN fs.discount_percentage > 0 THEN 1 ELSE 0 END) as discount_transaction_count,
    SUM(fs.line_total) as total_revenue,
    ROUND(
        100.0 * SUM(CASE WHEN fs.discount_percentage > 0 THEN 1 ELSE 0 END) /
        COUNT(DISTINCT fs.transaction_id),
        2
    ) as discount_penetration_pct
FROM warehouse.fact_sales fs
INNER JOIN warehouse.dim_products dp ON fs.product_sk = dp.product_sk
WHERE dp.is_current = TRUE
GROUP BY fs.product_sk, dp.product_name, dp.category
HAVING AVG(fs.discount_percentage) > 0
ORDER BY avg_discount DESC;

-- Q9: Customer Purchase Frequency and Recency
SELECT
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    acl.transaction_count,
    acl.first_purchase_date,
    acl.last_purchase_date,
    (CURRENT_DATE - acl.last_purchase_date) as days_since_last_purchase,
    acl.total_spent,
    acl.avg_transaction_value,
    CASE
        WHEN (CURRENT_DATE - acl.last_purchase_date) <= 30 THEN 'Active'
        WHEN (CURRENT_DATE - acl.last_purchase_date) <= 90 THEN 'Warm'
        WHEN (CURRENT_DATE - acl.last_purchase_date) <= 180 THEN 'Cool'
        ELSE 'Inactive'
    END as customer_status
FROM warehouse.agg_customer_lifetime acl
INNER JOIN warehouse.dim_customers dc ON acl.customer_sk = dc.customer_sk
WHERE dc.is_current = TRUE
ORDER BY acl.last_purchase_date DESC;

-- Q10: Weekly Sales Trend
SELECT
    dd.year,
    dd.week_of_year,
    MIN(dd.date_id) as week_start_date,
    MAX(dd.date_id) as week_end_date,
    SUM(ads.total_sales) as weekly_revenue,
    SUM(ads.total_units_sold) as weekly_units,
    SUM(ads.transaction_count) as weekly_transactions,
    ROUND(AVG(ads.avg_transaction_value), 2) as avg_txn_value
FROM warehouse.agg_daily_sales ads
INNER JOIN warehouse.dim_date dd ON ads.date_sk = dd.date_sk
GROUP BY dd.year, dd.week_of_year
ORDER BY dd.year DESC, dd.week_of_year DESC;

-- Q11: Age Group Purchase Behavior
SELECT
    dc.age_group,
    COUNT(DISTINCT dc.customer_sk) as customer_count,
    COUNT(DISTINCT fs.transaction_id) as transaction_count,
    SUM(fs.line_total) as total_revenue,
    ROUND(AVG(fs.line_total), 2) as avg_line_value,
    ROUND(AVG(fs.quantity), 2) as avg_items_per_transaction,
    COUNT(DISTINCT fs.product_sk) as unique_products_purchased
FROM warehouse.fact_sales fs
INNER JOIN warehouse.dim_customers dc ON fs.customer_sk = dc.customer_sk
WHERE dc.is_current = TRUE
GROUP BY dc.age_group
ORDER BY total_revenue DESC;

-- Q12: Top Customers by Recent Activity
SELECT
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    acl.transaction_count,
    acl.total_spent,
    acl.last_purchase_date,
    COUNT(DISTINCT fs.product_sk) as products_purchased_recently,
    SUM(CASE WHEN fs.date_sk >= (SELECT MAX(date_sk) - 30 FROM warehouse.dim_date) THEN fs.line_total ELSE 0 END) as last_30_days_spend
FROM warehouse.agg_customer_lifetime acl
INNER JOIN warehouse.dim_customers dc ON acl.customer_sk = dc.customer_sk
INNER JOIN warehouse.fact_sales fs ON acl.customer_sk = fs.customer_sk
WHERE dc.is_current = TRUE
GROUP BY dc.customer_id, dc.first_name, dc.last_name, dc.email, 
         acl.transaction_count, acl.total_spent, acl.last_purchase_date
ORDER BY acl.last_purchase_date DESC
LIMIT 20;
    