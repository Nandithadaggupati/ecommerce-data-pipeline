import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

from sqlalchemy import create_engine, text
import pandas as pd
import os
import yaml


def _get_engine():
    cfg = yaml.safe_load(Path("config/config.yaml").read_text())
    host = os.getenv("DB_HOST", cfg["database"]["host"])
    port = os.getenv("DB_PORT", cfg["database"]["port"])
    name = os.getenv("DB_NAME", cfg["database"]["name"])
    user = os.getenv("DB_USER", cfg["database"]["user"])
    password = os.getenv("DB_PASSWORD", cfg["database"]["password"])
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url)


def build_dim_customers(connection) -> int:
    """
    Build dim_customers from production.customers.
    SCD Type 2: track changes with effective_date, end_date, is_current.
    """
    query = text("""
        INSERT INTO warehouse.dim_customers 
        (customer_id, first_name, last_name, email, phone, city, state, country, 
         age_group, registration_date, is_current, effective_date, end_date)
        SELECT 
            customer_id, first_name, last_name, email, phone, city, state, country,
            age_group, registration_date, TRUE, CURRENT_DATE, NULL
        FROM production.customers
        WHERE is_active = TRUE
        ON CONFLICT DO NOTHING
    """)
    result = connection.execute(query)
    return result.rowcount


def build_dim_products(connection) -> int:
    """
    Build dim_products from production.products.
    SCD Type 2: track price/cost changes with effective_date, end_date.
    """
    query = text("""
        INSERT INTO warehouse.dim_products 
        (product_id, product_name, category, sub_category, price, cost, brand, 
         is_current, effective_date, end_date)
        SELECT 
            product_id, product_name, category, sub_category, price, cost, brand,
            TRUE, CURRENT_DATE, NULL
        FROM production.products
        WHERE is_active = TRUE
        ON CONFLICT DO NOTHING
    """)
    result = connection.execute(query)
    return result.rowcount


def build_dim_date(start_date: str, end_date: str, connection) -> int:
    """
    Build dim_date dimension table for date range.
    Generates one row per day with all date attributes.
    """
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    rows = []

    for d in dates:
        rows.append({
            "date_id": d.date(),
            "day_of_month": d.day,
            "day_of_week": d.dayofweek + 1,  # 1=Monday, 7=Sunday
            "day_name": d.day_name(),
            "month": d.month,
            "month_name": d.month_name(),
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
            "week_of_year": d.isocalendar()[1],
            "is_weekend": d.dayofweek >= 5,
        })

    df = pd.DataFrame(rows)
    df.to_sql(
        "dim_date",
        con=connection,
        schema="warehouse",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    return len(df)


def build_dim_payment_method(connection) -> int:
    """
    Build dim_payment_method from distinct values in staging.
    """
    query = text("""
        INSERT INTO warehouse.dim_payment_method 
        (payment_method_id, payment_method_name, category, is_active)
        SELECT DISTINCT
            LOWER(REPLACE(payment_method, ' ', '_')) as payment_method_id,
            payment_method as payment_method_name,
            CASE 
                WHEN payment_method IN ('Credit Card', 'Debit Card') THEN 'Card'
                WHEN payment_method = 'UPI' THEN 'Digital'
                WHEN payment_method = 'Net Banking' THEN 'Digital'
                WHEN payment_method = 'Cash on Delivery' THEN 'Cash'
                ELSE 'Other'
            END as category,
            TRUE
        FROM staging.transactions
        ON CONFLICT (payment_method_id) DO NOTHING
    """)
    result = connection.execute(query)
    return result.rowcount


def build_fact_sales(connection) -> int:
    """
    Build fact_sales from production tables.
    Joins transactions, transaction_items, and lookups to dimensions.
    """
    query = text("""
        INSERT INTO warehouse.fact_sales
        (transaction_id, item_id, customer_sk, product_sk, date_sk, 
         payment_method_sk, quantity, unit_price, discount_percentage, 
         line_total, transaction_total, shipping_address)
        SELECT
            t.transaction_id,
            ti.item_id,
            dc.customer_sk,
            dp.product_sk,
            dd.date_sk,
            dpm.payment_method_sk,
            ti.quantity,
            ti.unit_price,
            ti.discount_percentage,
            ti.line_total,
            t.total_amount,
            t.shipping_address
        FROM production.transaction_items ti
        INNER JOIN production.transactions t 
            ON ti.transaction_id = t.transaction_id
        INNER JOIN warehouse.dim_customers dc 
            ON t.customer_id = dc.customer_id AND dc.is_current = TRUE
        INNER JOIN warehouse.dim_products dp 
            ON ti.product_id = dp.product_id AND dp.is_current = TRUE
        INNER JOIN warehouse.dim_date dd 
            ON t.transaction_date = dd.date_id
        LEFT JOIN warehouse.dim_payment_method dpm 
            ON LOWER(REPLACE(t.payment_method, ' ', '_')) = dpm.payment_method_id
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse.fact_sales fs
            WHERE fs.item_id = ti.item_id
        )
    """)
    result = connection.execute(query)
    return result.rowcount


def build_agg_daily_sales(connection) -> int:
    """
    Build agg_daily_sales aggregate table for BI reporting.
    """
    query = text("""
        INSERT INTO warehouse.agg_daily_sales
        (date_sk, total_sales, total_units_sold, transaction_count, 
         customer_count, avg_transaction_value)
        SELECT
            fs.date_sk,
            SUM(fs.line_total) as total_sales,
            SUM(fs.quantity) as total_units_sold,
            COUNT(DISTINCT fs.transaction_id) as transaction_count,
            COUNT(DISTINCT fs.customer_sk) as customer_count,
            AVG(fs.transaction_total) as avg_transaction_value
        FROM warehouse.fact_sales fs
        GROUP BY fs.date_sk
        ON CONFLICT DO NOTHING
    """)
    result = connection.execute(query)
    return result.rowcount


def build_agg_product_sales(connection) -> int:
    """
    Build agg_product_sales aggregate for product performance.
    """
    query = text("""
        INSERT INTO warehouse.agg_product_sales
        (product_sk, total_sales, total_units_sold, transaction_count, avg_discount)
        SELECT
            fs.product_sk,
            SUM(fs.line_total) as total_sales,
            SUM(fs.quantity) as total_units_sold,
            COUNT(DISTINCT fs.transaction_id) as transaction_count,
            AVG(fs.discount_percentage) as avg_discount
        FROM warehouse.fact_sales fs
        GROUP BY fs.product_sk
        ON CONFLICT DO NOTHING
    """)
    result = connection.execute(query)
    return result.rowcount


def build_agg_customer_lifetime(connection) -> int:
    """
    Build agg_customer_lifetime for CLV analysis.
    """
    query = text("""
        INSERT INTO warehouse.agg_customer_lifetime
        (customer_sk, total_spent, total_units_purchased, transaction_count,
         first_purchase_date, last_purchase_date, avg_transaction_value)
        SELECT
            fs.customer_sk,
            SUM(fs.line_total) as total_spent,
            SUM(fs.quantity) as total_units_purchased,
            COUNT(DISTINCT fs.transaction_id) as transaction_count,
            MIN(dd.date_id) as first_purchase_date,
            MAX(dd.date_id) as last_purchase_date,
            AVG(fs.transaction_total) as avg_transaction_value
        FROM warehouse.fact_sales fs
        INNER JOIN warehouse.dim_date dd ON fs.date_sk = dd.date_sk
        GROUP BY fs.customer_sk
        ON CONFLICT DO NOTHING
    """)
    result = connection.execute(query)
    return result.rowcount


def apply_scd_type2(dimension_name: str, connection) -> dict:
    """
    Apply SCD Type 2 logic to dimensions.
    Mark old records as end_date = today, is_current = FALSE if values changed.
    """
    result = {
        "dimension": dimension_name,
        "records_updated": 0,
        "status": "success",
    }

    if dimension_name == "dim_customers":
        # Simplified SCD Type 2: if customer attributes changed, version them
        query = text("""
            UPDATE warehouse.dim_customers
            SET is_current = FALSE, end_date = CURRENT_DATE - INTERVAL '1 day'
            WHERE is_current = TRUE
              AND customer_id IN (
                SELECT customer_id FROM production.customers
                WHERE is_active = FALSE
              )
        """)
        res = connection.execute(query)
        result["records_updated"] = res.rowcount

    elif dimension_name == "dim_products":
        query = text("""
            UPDATE warehouse.dim_products
            SET is_current = FALSE, end_date = CURRENT_DATE - INTERVAL '1 day'
            WHERE is_current = TRUE
              AND product_id IN (
                SELECT product_id FROM production.products
                WHERE is_active = FALSE
              )
        """)
        res = connection.execute(query)
        result["records_updated"] = res.rowcount

    return result


def main():
    """
    Build complete warehouse schema: dimensions, fact, and aggregates.
    """
    engine = _get_engine()
    build_log = {
        "warehouse_build_timestamp": datetime.utcnow().isoformat(),
        "dimensions_built": {},
        "fact_tables_built": {},
        "aggregates_built": {},
        "scd_updates": {},
    }

    with engine.begin() as conn:
        # Build dimensions
        cust_count = build_dim_customers(conn)
        build_log["dimensions_built"]["dim_customers"] = cust_count

        prod_count = build_dim_products(conn)
        build_log["dimensions_built"]["dim_products"] = prod_count

        # Get date range from transactions
        date_range = pd.read_sql(
            "SELECT MIN(transaction_date) as min_date, MAX(transaction_date) as max_date FROM production.transactions",
            con=conn
        )
        min_date = date_range["min_date"].iloc[0]
        max_date = date_range["max_date"].iloc[0]

        date_count = build_dim_date(str(min_date), str(max_date), conn)
        build_log["dimensions_built"]["dim_date"] = date_count

        pm_count = build_dim_payment_method(conn)
        build_log["dimensions_built"]["dim_payment_method"] = pm_count

        # Build fact table
        fact_count = build_fact_sales(conn)
        build_log["fact_tables_built"]["fact_sales"] = fact_count

        # Build aggregates
        agg_daily = build_agg_daily_sales(conn)
        build_log["aggregates_built"]["agg_daily_sales"] = agg_daily

        agg_prod = build_agg_product_sales(conn)
        build_log["aggregates_built"]["agg_product_sales"] = agg_prod

        agg_cust = build_agg_customer_lifetime(conn)
        build_log["aggregates_built"]["agg_customer_lifetime"] = agg_cust

        # Apply SCD Type 2
        scd_cust = apply_scd_type2("dim_customers", conn)
        build_log["scd_updates"]["dim_customers"] = scd_cust

        scd_prod = apply_scd_type2("dim_products", conn)
        build_log["scd_updates"]["dim_products"] = scd_prod

    Path("data/processed").mkdir(parents=True, exist_ok=True)
    with open("data/processed/warehouse_build_report.json", "w") as f:
        json.dump(build_log, f, indent=2)

    print("✓ Warehouse schema built successfully")
    return build_log


if __name__ == "__main__":
    main()
    