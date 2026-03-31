import pandas as pd
from sqlalchemy import text
import json
import datetime
import os
import time

def cleanse_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    for col in ['first_name', 'last_name', 'email', 'phone', 'city', 'state', 'country', 'age_group']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    if 'email' in df.columns: df['email'] = df['email'].str.lower()
    if 'first_name' in df.columns: df['first_name'] = df['first_name'].str.title()
    if 'last_name' in df.columns: df['last_name'] = df['last_name'].str.title()
    return df

def cleanse_product_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    for col in ['product_name', 'category', 'sub_category', 'brand']:
        if col in df.columns: df[col] = df[col].astype(str).str.strip()
    return df

def apply_business_rules(df: pd.DataFrame, rule_type: str) -> pd.DataFrame:
    if df.empty: return df
    if rule_type == 'transactions':
        if 'total_amount' in df.columns: 
            df = df[df['total_amount'] > 0]
    elif rule_type == 'transaction_items':
        if 'quantity' in df.columns and 'unit_price' in df.columns:
            df = df[(df['quantity'] > 0)]
            df.loc[:, 'line_total'] = round(df['quantity'] * df['unit_price'] * (1 - df['discount_percentage']/100), 2)
    return df

def load_to_production(df: pd.DataFrame, table_name: str, connection, strategy: str) -> dict:
    start_count = len(df)
    if 'loaded_at' in df.columns: df = df.drop(columns=['loaded_at'])
    if start_count == 0: return {"input": 0, "output": 0, "filtered": 0}
        
    try:
        if strategy == 'truncate_insert':
            connection.execute(text(f"TRUNCATE production.{table_name} CASCADE;"))
            df.to_sql(table_name, con=connection, schema='production', if_exists='append', index=False, method='multi', chunksize=1000)
            return {"input": start_count, "output": len(df), "filtered": 0}
            
        elif strategy == 'incremental':
            id_col = f"{table_name[:-1]}_id" if table_name in ['transactions'] else "item_id"
            existing = pd.read_sql(f"SELECT {id_col} FROM production.{table_name}", connection)
            df_new = df[~df[id_col].isin(existing[id_col])]
            if not df_new.empty:
                df_new.to_sql(table_name, con=connection, schema='production', if_exists='append', index=False, method='multi', chunksize=1000)
            return {"input": start_count, "output": len(df_new), "filtered": start_count - len(df_new)}
    except Exception as e:
        print(f"Error loading {table_name}: {e}")
        return {"input": start_count, "output": 0, "filtered": start_count, "rejected_reasons": {"error": str(e)}}

if __name__ == "__main__":
    from sqlalchemy import create_engine
    from dotenv import load_dotenv
    load_dotenv()
    
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "ecommerce_db")
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    os.makedirs("data/processed", exist_ok=True)
    
    summary = {
        "transformation_timestamp": datetime.datetime.now().isoformat(),
        "records_processed": {},
        "transformations_applied": ["cleanse_data", "apply_business_rules", "truncate_insert_dims", "incremental_facts"]
    }
    
    # Notice the order: Because we use TRUNCATE CASCADE for dimensions, we MUST load them first so facts can populate appropriately.
    with engine.begin() as conn:
        print("Transforming customers...")
        df_c = pd.read_sql("SELECT * FROM staging.customers", conn)
        df_c = cleanse_customer_data(df_c)
        summary["records_processed"]["customers"] = load_to_production(df_c, "customers", conn, "truncate_insert")
        
        print("Transforming products...")
        df_p = pd.read_sql("SELECT * FROM staging.products", conn)
        df_p = cleanse_product_data(df_p)
        summary["records_processed"]["products"] = load_to_production(df_p, "products", conn, "truncate_insert")
        
        print("Transforming transactions...")
        df_t = pd.read_sql("SELECT * FROM staging.transactions", conn)
        df_t = apply_business_rules(df_t, 'transactions')
        summary["records_processed"]["transactions"] = load_to_production(df_t, "transactions", conn, "incremental")
        
        print("Transforming transaction_items...")
        df_ti = pd.read_sql("SELECT * FROM staging.transaction_items", conn)
        df_ti = apply_business_rules(df_ti, 'transaction_items')
        summary["records_processed"]["transaction_items"] = load_to_production(df_ti, "transaction_items", conn, "incremental")
        
    with open("data/processed/transformation_summary.json", "w") as f:
        json.dump(summary, f, indent=4)
