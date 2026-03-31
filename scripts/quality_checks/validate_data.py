import json
import os
import datetime
import pandas as pd
from sqlalchemy import create_engine, text

def check_null_values(connection, schema: str) -> dict:
    tables = {
        "customers": "email",
        "products": "price",
        "transactions": "customer_id",
        "transaction_items": "product_id"
    }
    violations = 0
    details = {}
    for table, col in tables.items():
        query = text(f"SELECT COUNT(*) FROM {schema}.{table} WHERE {col} IS NULL OR {col}::text = ''")
        count = connection.execute(query).scalar()
        if count > 0:
            details[f"{table}.{col}"] = count
            violations += count
            
    return {
        "status": "passed" if violations == 0 else "failed",
        "tables_checked": list(tables.keys()),
        "null_violations": violations,
        "details": details
    }

def check_duplicates(connection, schema: str) -> dict:
    tables = ["customers", "products", "transactions", "transaction_items"]
    identifiers = ["customer_id", "product_id", "transaction_id", "item_id"]
    violations = 0
    details = {}
    for t, pk in zip(tables, identifiers):
        query = text(f"SELECT COUNT({pk}) - COUNT(DISTINCT {pk}) FROM {schema}.{t}")
        count = connection.execute(query).scalar()
        if count > 0:
            details[t] = count
            violations += count
    return {
        "status": "passed" if violations == 0 else "failed",
        "duplicates_found": violations,
        "details": details
    }

def check_referential_integrity(connection, schema: str) -> dict:
    q1 = text(f"SELECT COUNT(*) FROM {schema}.transactions t LEFT JOIN {schema}.customers c ON t.customer_id = c.customer_id WHERE c.customer_id IS NULL")
    q2 = text(f"SELECT COUNT(*) FROM {schema}.transaction_items i LEFT JOIN {schema}.transactions t ON i.transaction_id = t.transaction_id WHERE t.transaction_id IS NULL")
    q3 = text(f"SELECT COUNT(*) FROM {schema}.transaction_items i LEFT JOIN {schema}.products p ON i.product_id = p.product_id WHERE p.product_id IS NULL")
    
    o1 = connection.execute(q1).scalar()
    o2 = connection.execute(q2).scalar()
    o3 = connection.execute(q3).scalar()
    
    total = o1 + o2 + o3
    
    return {
        "status": "passed" if total == 0 else "failed",
        "orphan_records": total,
        "details": {
            "transactions_to_customers": o1,
            "items_to_transactions": o2,
            "items_to_products": o3
        }
    }

def check_data_ranges(connection, schema: str) -> dict:
    q1_price = text(f"SELECT COUNT(*) FROM {schema}.products WHERE price < 0")
    q2_qty = text(f"SELECT COUNT(*) FROM {schema}.transaction_items WHERE quantity <= 0")
    q3_disc = text(f"SELECT COUNT(*) FROM {schema}.transaction_items WHERE discount_percentage < 0 OR discount_percentage > 100")
    
    c1 = connection.execute(q1_price).scalar()
    c2 = connection.execute(q2_qty).scalar()
    c3 = connection.execute(q3_disc).scalar()
    
    total = c1 + c2 + c3
    
    return {
        "status": "passed" if total == 0 else "failed",
        "violations": total,
        "details": {
            "negative_prices": c1,
            "invalid_quantity": c2,
            "invalid_discount": c3
        }
    }

def calculate_quality_score(check_results: dict) -> float:
    # Weighted calculation
    score = 100.0
    
    nulls = check_results.get("null_checks", {}).get("null_violations", 0)
    dupes = check_results.get("duplicate_checks", {}).get("duplicates_found", 0)
    orphans = check_results.get("referential_integrity", {}).get("orphan_records", 0)
    ranges = check_results.get("range_checks", {}).get("violations", 0)
    
    # Critical impact
    score -= orphans * 5.0
    # High impact
    score -= dupes * 2.0
    # Med impact
    score -= nulls * 1.0
    score -= ranges * 1.0
    
    return max(0.0, score)

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
    
    report = {
        "check_timestamp": datetime.datetime.now().isoformat(),
        "checks_performed": {}
    }
    
    with engine.connect() as conn:
        print("Running Quality Checks...")
        report["checks_performed"]["null_checks"] = check_null_values(conn, 'staging')
        report["checks_performed"]["duplicate_checks"] = check_duplicates(conn, 'staging')
        report["checks_performed"]["referential_integrity"] = check_referential_integrity(conn, 'staging')
        report["checks_performed"]["range_checks"] = check_data_ranges(conn, 'staging')
        
    score = calculate_quality_score(report["checks_performed"])
    report["overall_quality_score"] = float(score)
    report["quality_grade"] = "A" if score >= 90 else "B" if score >= 80 else "C" if score >= 70 else "D" if score >= 60 else "F"
    
    with open("data/staging/quality_report.json", "w") as f:
        json.dump(report, f, indent=4)
        
    with open("logs/quality_checks.log", "a") as f:
        f.write(f"{datetime.datetime.now().isoformat()} - SCORE: {score} - GRADE: {report['quality_grade']}\n")
    
    print(f"Data Quality Validation Complete. Score: {score}")
