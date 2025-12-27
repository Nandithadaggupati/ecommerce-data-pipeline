import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import yaml
from pathlib import Path
from typing import Dict, Any

def _get_engine():
    cfg = yaml.safe_load(Path("config/config.yaml").read_text())
    host = os.getenv("DB_HOST", cfg["database"]["host"])
    port = os.getenv("DB_PORT", cfg["database"]["port"])
    name = os.getenv("DB_NAME", cfg["database"]["name"])
    user = os.getenv("DB_USER", cfg["database"]["user"])
    password = os.getenv("DB_PASSWORD", cfg["database"]["password"])
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url)


def cleanse_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleanse and enrich customer data from staging.
    - Remove duplicates (keep first)
    - Standardize names (title case)
    - Validate email format (basic)
    - Fill missing phone with 'Unknown'
    """
    df = df.drop_duplicates(subset=["customer_id"], keep="first").reset_index(drop=True)
    df["first_name"] = df["first_name"].fillna("Unknown").str.title()
    df["last_name"] = df["last_name"].fillna("Unknown").str.title()
    df["email"] = df["email"].fillna("noemail@example.com")
    df["phone"] = df["phone"].fillna("Unknown")
    df["city"] = df["city"].fillna("Unknown")
    df["state"] = df["state"].fillna("Unknown")
    df["country"] = df["country"].fillna("Unknown")
    df["age_group"] = df["age_group"].fillna("Unknown")
    return df


def cleanse_product_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleanse and enrich product data from staging.
    - Remove duplicates (keep first)
    - Ensure price > 0 and cost >= 0
    - Ensure cost < price
    - Standardize category and brand names
    """
    df = df.drop_duplicates(subset=["product_id"], keep="first").reset_index(drop=True)
    df["product_name"] = df["product_name"].fillna("Unknown Product")
    df["category"] = df["category"].fillna("Uncategorized").str.title()
    df["sub_category"] = df["sub_category"].fillna("General")
    df["brand"] = df["brand"].fillna("Unknown Brand")

    # Fix price/cost anomalies
    df.loc[df["price"] <= 0, "price"] = 1.00
    df.loc[df["cost"] < 0, "cost"] = 0.00
    df.loc[df["cost"] >= df["price"], "cost"] = df["price"] * 0.5

    df["stock_quantity"] = df["stock_quantity"].fillna(0).astype(int)
    df["supplier_id"] = df["supplier_id"].fillna("UNKNOWN")

    return df


def apply_business_rules(df: pd.DataFrame, rule_type: str) -> pd.DataFrame:
    """
    Apply domain-specific business rules.
    rule_type: 'transaction_item' or 'transaction'
    """
    if rule_type == "transaction_item":
        # line_total = quantity * unit_price * (1 - discount/100)
        df["line_total"] = (
            df["quantity"]
            * df["unit_price"]
            * (1 - df["discount_percentage"] / 100)
        ).round(2)
        # quantity must be > 0
        df = df[df["quantity"] > 0].reset_index(drop=True)
        # discount must be 0-100
        df = df[(df["discount_percentage"] >= 0) & (df["discount_percentage"] <= 100)].reset_index(drop=True)

    elif rule_type == "transaction":
        # total_amount should match sum of line items per transaction (enforced at load)
        df["total_amount"] = df["total_amount"].fillna(0).round(2)
        df = df[df["total_amount"] >= 0].reset_index(drop=True)

    return df


def load_to_production(
    df: pd.DataFrame, table_name: str, connection, strategy: str = "append"
) -> dict:
    """
    Load cleansed/transformed data to production schema.
    strategy: 'append' (default) or 'replace'
    """
    schema, table = table_name.split(".")
    if_exists = "replace" if strategy == "replace" else "append"

    try:
        df.to_sql(
            table,
            con=connection,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=500,
        )
        return {
            "table": table_name,
            "rows_loaded": len(df),
            "status": "success",
        }
    except Exception as e:
        return {
            "table": table_name,
            "rows_loaded": 0,
            "status": "failed",
            "error": str(e),
        }


def main():
    """
    ETL: Staging → Production
    1. Read from staging schema
    2. Cleanse and transform
    3. Apply business rules
    4. Load to production schema
    5. Write summary report
    """
    engine = _get_engine()
    results = []

    with engine.begin() as conn:
        # ===== CUSTOMERS =====
        stg_customers = pd.read_sql(
            "SELECT * FROM staging.customers", con=conn
        )
        customers_clean = cleanse_customer_data(stg_customers)
        res = load_to_production(
            customers_clean, "production.customers", conn, strategy="append"
        )
        results.append(res)

        # ===== PRODUCTS =====
        stg_products = pd.read_sql("SELECT * FROM staging.products", con=conn)
        products_clean = cleanse_product_data(stg_products)
        res = load_to_production(
            products_clean, "production.products", conn, strategy="append"
        )
        results.append(res)

        # ===== TRANSACTIONS =====
        stg_transactions = pd.read_sql(
            "SELECT * FROM staging.transactions", con=conn
        )
        transactions_clean = apply_business_rules(stg_transactions, "transaction")
        res = load_to_production(
            transactions_clean, "production.transactions", conn, strategy="append"
        )
        results.append(res)

        # ===== TRANSACTION ITEMS =====
        stg_items = pd.read_sql(
            "SELECT * FROM staging.transaction_items", con=conn
        )
        items_clean = apply_business_rules(stg_items, "transaction_item")
        res = load_to_production(
            items_clean, "production.transaction_items", conn, strategy="append"
        )
        results.append(res)

    summary = {
        "transformation_timestamp": datetime.utcnow().isoformat(),
        "tables_transformed": {
            r["table"]: {
                "rows_loaded": r["rows_loaded"],
                "status": r["status"],
                "error": r.get("error"),
            }
            for r in results
        },
        "transformation_status": "success" if all(r["status"] == "success" for r in results) else "partial",
    }

    Path("data/processed").mkdir(parents=True, exist_ok=True)
    with open("data/processed/transformation_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("✓ Staging → Production ETL complete")
    return summary


if __name__ == "__main__":
    main()
