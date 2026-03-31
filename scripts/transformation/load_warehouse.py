import pandas as pd
from sqlalchemy import create_engine, text
import json
import datetime
import os
import time

def build_dim_date(start_date: str, end_date: str, connection) -> int:
    dates_df = pd.DataFrame({"full_date": pd.date_range(start_date, end_date)})
    dates_df['date_key'] = dates_df['full_date'].dt.strftime('%Y%m%d').astype(int)
    dates_df['full_date'] = dates_df['full_date'].dt.date
    dates_df['year'] = pd.to_datetime(dates_df['full_date']).dt.year
    dates_df['quarter'] = pd.to_datetime(dates_df['full_date']).dt.quarter
    dates_df['month'] = pd.to_datetime(dates_df['full_date']).dt.month
    dates_df['day'] = pd.to_datetime(dates_df['full_date']).dt.day
    dates_df['month_name'] = pd.to_datetime(dates_df['full_date']).dt.month_name()
    dates_df['day_name'] = pd.to_datetime(dates_df['full_date']).dt.day_name()
    dates_df['week_of_year'] = pd.to_datetime(dates_df['full_date']).dt.isocalendar().week
    dates_df['is_weekend'] = pd.to_datetime(dates_df['full_date']).dt.dayofweek.isin([5, 6])
    dates_df['is_holiday'] = False
    
    connection.execute(text("TRUNCATE warehouse.dim_date CASCADE;"))
    dates_df.to_sql('dim_date', con=connection, schema='warehouse', if_exists='append', index=False, method='multi', chunksize=1000)
    return len(dates_df)

def build_dim_payment_method(connection) -> int:
    methods = [
        {"payment_method_name": "Credit Card", "payment_type": "Online"},
        {"payment_method_name": "Debit Card", "payment_type": "Online"},
        {"payment_method_name": "UPI", "payment_type": "Online"},
        {"payment_method_name": "Net Banking", "payment_type": "Online"},
        {"payment_method_name": "Cash on Delivery", "payment_type": "Offline"},
    ]
    df = pd.DataFrame(methods)
    connection.execute(text("TRUNCATE warehouse.dim_payment_method CASCADE;"))
    df.to_sql('dim_payment_method', con=connection, schema='warehouse', if_exists='append', index=False)
    return len(df)

def apply_scd_type2(dimension_name: str, connection) -> dict:
    today_date = datetime.date.today()
    if dimension_name == 'dim_customers':
        query = text("""
            WITH new_data AS (
                SELECT customer_id, first_name || ' ' || last_name AS full_name, email, city, state, country, age_group,
                CASE 
                    WHEN (SELECT COUNT(*) FROM production.transactions WHERE customer_id = c.customer_id) > 10 THEN 'VIP'
                    WHEN (SELECT COUNT(*) FROM production.transactions WHERE customer_id = c.customer_id) BETWEEN 2 AND 10 THEN 'Regular'
                    ELSE 'New'
                END AS customer_segment,
                registration_date
                FROM production.customers c
            ),
            updates AS (
                UPDATE warehouse.dim_customers w
                SET end_date = :today, is_current = FALSE
                FROM new_data n
                WHERE w.customer_id = n.customer_id AND w.is_current = TRUE
                AND (w.full_name != n.full_name OR w.email != n.email OR w.city != n.city)
                RETURNING w.customer_id
            )
            INSERT INTO warehouse.dim_customers (customer_id, full_name, email, city, state, country, age_group, customer_segment, registration_date, effective_date, end_date, is_current)
            SELECT customer_id, full_name, email, city, state, country, age_group, customer_segment, registration_date, :today, NULL, TRUE
            FROM new_data n
            WHERE n.customer_id NOT IN (SELECT customer_id FROM warehouse.dim_customers WHERE is_current = TRUE)
            OR n.customer_id IN (SELECT customer_id FROM updates);
        """)
        
    elif dimension_name == 'dim_products':
        query = text("""
            WITH new_data AS (
                SELECT product_id, product_name, category, sub_category, brand,
                CASE 
                    WHEN price < 50 THEN 'Budget'
                    WHEN price < 200 THEN 'Mid-range'
                    ELSE 'Premium'
                END AS price_range
                FROM production.products
            ),
            updates AS (
                UPDATE warehouse.dim_products w
                SET end_date = :today, is_current = FALSE
                FROM new_data n
                WHERE w.product_id = n.product_id AND w.is_current = TRUE
                AND (w.product_name != n.product_name OR w.price_range != n.price_range)
                RETURNING w.product_id
            )
            INSERT INTO warehouse.dim_products (product_id, product_name, category, sub_category, brand, price_range, effective_date, end_date, is_current)
            SELECT product_id, product_name, category, sub_category, brand, price_range, :today, NULL, TRUE
            FROM new_data n
            WHERE n.product_id NOT IN (SELECT product_id FROM warehouse.dim_products WHERE is_current = TRUE)
            OR n.product_id IN (SELECT product_id FROM updates);
        """)
    connection.execute(query, {"today": today_date})
    return {"status": "success"}

def build_dim_customers(connection) -> int:
    apply_scd_type2('dim_customers', connection)
    return connection.execute(text("SELECT COUNT(*) FROM warehouse.dim_customers WHERE is_current = TRUE")).scalar()

def build_dim_products(connection) -> int:
    apply_scd_type2('dim_products', connection)
    return connection.execute(text("SELECT COUNT(*) FROM warehouse.dim_products WHERE is_current = TRUE")).scalar()

def build_fact_sales(connection) -> int:
    # We use incremental load mechanism by identifying what is NOT in fact_sales yet
    query = text("""
        INSERT INTO warehouse.fact_sales (date_key, customer_key, product_key, payment_method_key, transaction_id, quantity, unit_price, discount_amount, line_total, profit)
        SELECT 
            TO_CHAR(t.transaction_date, 'YYYYMMDD')::INTEGER AS date_key,
            c.customer_key,
            p.product_key,
            m.payment_method_key,
            t.transaction_id,
            ti.quantity,
            ti.unit_price,
            (ti.quantity * ti.unit_price * (ti.discount_percentage/100)) AS discount_amount,
            ti.line_total,
            (ti.line_total - (prod.cost * ti.quantity)) AS profit
        FROM production.transaction_items ti
        JOIN production.transactions t ON ti.transaction_id = t.transaction_id
        JOIN warehouse.dim_customers c ON t.customer_id = c.customer_id AND c.is_current = TRUE
        JOIN warehouse.dim_products p ON ti.product_id = p.product_id AND p.is_current = TRUE
        JOIN production.products prod ON ti.product_id = prod.product_id
        JOIN warehouse.dim_payment_method m ON t.payment_method = m.payment_method_name
        WHERE t.transaction_id NOT IN (SELECT DISTINCT transaction_id FROM warehouse.fact_sales);
    """)
    connection.execute(query)
    
    # Reload aggregates completely
    connection.execute(text("TRUNCATE warehouse.agg_daily_sales;"))
    connection.execute(text("""
        INSERT INTO warehouse.agg_daily_sales
        SELECT date_key, COUNT(DISTINCT transaction_id), SUM(line_total), SUM(profit), COUNT(DISTINCT customer_key)
        FROM warehouse.fact_sales GROUP BY date_key;
    """))
    
    connection.execute(text("TRUNCATE warehouse.agg_product_performance;"))
    connection.execute(text("""
        INSERT INTO warehouse.agg_product_performance
        SELECT product_key, SUM(quantity), SUM(line_total), SUM(profit), AVG(discount_amount/(quantity*unit_price)*100)
        FROM warehouse.fact_sales 
        WHERE quantity > 0 AND unit_price > 0
        GROUP BY product_key;
    """))

    connection.execute(text("TRUNCATE warehouse.agg_customer_metrics;"))
    connection.execute(text("""
        INSERT INTO warehouse.agg_customer_metrics
        SELECT customer_key, COUNT(DISTINCT transaction_id), SUM(line_total), AVG(line_total), NULL
        FROM warehouse.fact_sales GROUP BY customer_key;
    """))
    
    return connection.execute(text("SELECT COUNT(*) FROM warehouse.fact_sales")).scalar()

def main():

    from dotenv import load_dotenv
    load_dotenv()
    
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "ecommerce_db")
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    with engine.begin() as conn:
        print("Dim Date...")
        build_dim_date('2023-01-01', '2024-12-31', conn)
        print("Dim Payment...")
        build_dim_payment_method(conn)
        print("Dim Customers...")
        build_dim_customers(conn)
        print("Dim Products...")
        build_dim_products(conn)
        print("Fact Sales...")
        build_fact_sales(conn)
        print("Done")
        
    report = {
        "dimensions_built": ["dim_date", "dim_payment_method", "dim_customers", "dim_products"],
        "fact_tables_built": ["fact_sales"],
        "aggregates_built": ["agg_daily_sales", "agg_product_performance", "agg_customer_metrics"]
    }
    with open("data/processed/warehouse_build_report.json", "w") as f:
        json.dump(report, f, indent=4)
if __name__ == '__main__':
    main()
