import pandas as pd
from sqlalchemy import create_engine, text
import json
import os
import datetime
import time

def load_csv_to_staging(csv_path: str, table_name: str, connection) -> dict:
    try:
        df = pd.read_csv(csv_path)
        rows_loaded = bulk_insert_data(df, table_name, connection)
        return {"rows_loaded": rows_loaded, "status": "success"}
    except Exception as e:
        return {"rows_loaded": 0, "status": "failed", "error_message": str(e)}

def bulk_insert_data(df: pd.DataFrame, table_name: str, connection) -> int:
    connection.execute(text(f"TRUNCATE staging.{table_name};"))
    df.to_sql(table_name, con=connection, schema='staging', if_exists='append', index=False, method='multi', chunksize=1000)
    return len(df)

def validate_staging_load(connection) -> dict:
    results = {}
    tables = ['customers', 'products', 'transactions', 'transaction_items']
    for t in tables:
        count = connection.execute(text(f"SELECT COUNT(*) FROM staging.{t}")).scalar()
        results[f"staging.{t}"] = {"rows_loaded": count, "status": "success"}
    return results

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "ecommerce_db")
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    os.makedirs("data/staging", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    start_time = time.time()
    
    summary = {
        "ingestion_timestamp": datetime.datetime.now().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0.0
    }
    
    with engine.begin() as conn:
        print("Ingesting customers...")
        summary["tables_loaded"]["staging.customers"] = load_csv_to_staging("data/raw/customers.csv", "customers", conn)
        print("Ingesting products...")
        summary["tables_loaded"]["staging.products"] = load_csv_to_staging("data/raw/products.csv", "products", conn)
        print("Ingesting transactions...")
        summary["tables_loaded"]["staging.transactions"] = load_csv_to_staging("data/raw/transactions.csv", "transactions", conn)
        print("Ingesting transaction_items...")
        summary["tables_loaded"]["staging.transaction_items"] = load_csv_to_staging("data/raw/transaction_items.csv", "transaction_items", conn)
        
        val = validate_staging_load(conn)
        print("Validation results:", val)
    
    summary["total_execution_time_seconds"] = round(time.time() - start_time, 2)
    
    with open("data/staging/ingestion_summary.json", "w") as f:
        json.dump(summary, f, indent=4)
        
    with open("logs/ingestion.log", "a") as f:
        f.write(f"{datetime.datetime.now().isoformat()} - START INGESTION\n")
        f.write(f"Summary: {json.dumps(summary)}\n")
