import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import yaml
from pathlib import Path

def _get_engine():
    cfg = yaml.safe_load(Path("config/config.yaml").read_text())
    host = os.getenv("DB_HOST", cfg["database"]["host"])
    port = os.getenv("DB_PORT", cfg["database"]["port"])
    name = os.getenv("DB_NAME", cfg["database"]["name"])
    user = os.getenv("DB_USER", cfg["database"]["user"])
    password = os.getenv("DB_PASSWORD", cfg["database"]["password"])
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url)

def bulk_insert_data(df: pd.DataFrame, table_name: str, connection) -> int:
    schema, table = table_name.split(".")
    df.to_sql(
        table,
        con=connection,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    return len(df)

def load_csv_to_staging(csv_path: str, table_name: str, connection) -> dict:
    df = pd.read_csv(csv_path)
    rows = bulk_insert_data(df, table_name, connection)
    return {"table": table_name, "rows_loaded": rows, "status": "success"}

def validate_staging_load(connection) -> dict:
    tables = [
        "staging.customers",
        "staging.products",
        "staging.transactions",
        "staging.transaction_items",
    ]
    counts = {}
    for t in tables:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {t}"))
        counts[t] = result.scalar()
    return counts

def main():
    engine = _get_engine()
    Path("data/staging").mkdir(parents=True, exist_ok=True)

    with engine.begin() as conn:
        # Clean previous loads
        for t in [
            "staging.transaction_items",
            "staging.transactions",
            "staging.products",
            "staging.customers",
        ]:
            conn.execute(text(f"TRUNCATE TABLE {t};"))

        results = []
        results.append(
            load_csv_to_staging("data/raw/customers.csv", "staging.customers", conn)
        )
        results.append(
            load_csv_to_staging("data/raw/products.csv", "staging.products", conn)
        )
        results.append(
            load_csv_to_staging("data/raw/transactions.csv", "staging.transactions", conn)
        )
        results.append(
            load_csv_to_staging(
                "data/raw/transaction_items.csv", "staging.transaction_items", conn
            )
        )

        counts = validate_staging_load(conn)

    summary = {
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "tables_loaded": {
            r["table"]: {"rows_loaded": r["rows_loaded"], "status": r["status"]}
            for r in results
        },
        "table_row_counts": counts,
    }

    with open("data/staging/ingestion_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

if __name__ == "__main__":
    main()
