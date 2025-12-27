import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from sqlalchemy import create_engine, text
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


def check_null_values(connection, schema: str) -> dict:
    """
    Check completeness: count NULLs in key columns for all tables in the given schema.
    Returns a dict with per-table/column null counts.
    """
    results = {
        "status": "passed",
        "schema": schema,
        "null_violations": 0,
        "details": {},
    }

    # mandatory columns per table (can be extended)
    mandatory_columns = {
        "customers": ["customer_id", "email", "first_name", "last_name"],
        "products": ["product_id", "product_name", "price", "cost"],
        "transactions": ["transaction_id", "customer_id", "transaction_date"],
        "transaction_items": ["item_id", "transaction_id", "product_id", "quantity"],
    }

    for table, cols in mandatory_columns.items():
        table_key = f"{schema}.{table}"
        results["details"][table_key] = {}
        for col in cols:
            query = text(
                f"SELECT COUNT(*) FROM {schema}.{table} WHERE {col} IS NULL"
            )
            count = connection.execute(query).scalar()
            results["details"][table_key][col] = int(count)
            results["null_violations"] += int(count)

    if results["null_violations"] > 0:
        results["status"] = "failed"

    return results


def check_duplicates(connection, schema: str) -> dict:
    """
    Check uniqueness: look for duplicate primary/business keys.
    """
    results = {
        "status": "passed",
        "schema": schema,
        "duplicates_found": 0,
        "details": {},
    }

    key_columns = {
        "customers": "customer_id",
        "products": "product_id",
        "transactions": "transaction_id",
        "transaction_items": "item_id",
    }

    for table, key_col in key_columns.items():
        table_key = f"{schema}.{table}"
        query = text(
            f"""
            SELECT COUNT(*) FROM (
              SELECT {key_col}, COUNT(*) AS c
              FROM {schema}.{table}
              GROUP BY {key_col}
              HAVING COUNT(*) > 1
            ) dup
            """
        )
        dup_count = connection.execute(query).scalar()
        results["details"][table_key] = int(dup_count)
        results["duplicates_found"] += int(dup_count)

    if results["duplicates_found"] > 0:
        results["status"] = "failed"

    return results


def check_referential_integrity(connection, schema: str) -> dict:
    """
    Check FKs inside the given schema: transactions->customers, items->transactions, items->products.
    """
    results = {
        "status": "passed",
        "schema": schema,
        "orphan_records": 0,
        "details": {},
    }

    # transactions.customer_id -> customers.customer_id
    q_txn_cust = text(
        f"""
        SELECT COUNT(*) FROM {schema}.transactions t
        LEFT JOIN {schema}.customers c
          ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """
    )
    orphan_txn_cust = int(connection.execute(q_txn_cust).scalar())
    results["details"]["transactions_customer_fk"] = orphan_txn_cust
    results["orphan_records"] += orphan_txn_cust

    # transaction_items.transaction_id -> transactions.transaction_id
    q_items_txn = text(
        f"""
        SELECT COUNT(*) FROM {schema}.transaction_items ti
        LEFT JOIN {schema}.transactions t
          ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
        """
    )
    orphan_items_txn = int(connection.execute(q_items_txn).scalar())
    results["details"]["transaction_items_transaction_fk"] = orphan_items_txn
    results["orphan_records"] += orphan_items_txn

    # transaction_items.product_id -> products.product_id
    q_items_prod = text(
        f"""
        SELECT COUNT(*) FROM {schema}.transaction_items ti
        LEFT JOIN {schema}.products p
          ON ti.product_id = p.product_id
        WHERE p.product_id IS NULL
        """
    )
    orphan_items_prod = int(connection.execute(q_items_prod).scalar())
    results["details"]["transaction_items_product_fk"] = orphan_items_prod
    results["orphan_records"] += orphan_items_prod

    if results["orphan_records"] > 0:
        results["status"] = "failed"

    return results


def check_data_ranges(connection, schema: str) -> dict:
    """
    Check validity/range constraints (price > 0, quantity > 0, discount between 0-100, no future dates).
    """
    results = {
        "status": "passed",
        "schema": schema,
        "violations": 0,
        "details": {},
    }

    # products: price > 0, cost >= 0, cost < price
    q_price = text(
        f"""
        SELECT COUNT(*) FROM {schema}.products
        WHERE price <= 0 OR cost < 0 OR cost >= price
        """
    )
    prod_viol = int(connection.execute(q_price).scalar())
    results["details"]["products_price_cost"] = prod_viol
    results["violations"] += prod_viol

    # transaction_items: quantity > 0, discount between 0 and 100
    q_items = text(
        f"""
        SELECT COUNT(*) FROM {schema}.transaction_items
        WHERE quantity <= 0
           OR discount_percentage < 0
           OR discount_percentage > 100
        """
    )
    item_viol = int(connection.execute(q_items).scalar())
    results["details"]["transaction_items_quantity_discount"] = item_viol
    results["violations"] += item_viol

    # transactions: no future dates
    q_txn_date = text(
        f"""
        SELECT COUNT(*) FROM {schema}.transactions
        WHERE transaction_date > CURRENT_DATE
        """
    )
    txn_viol = int(connection.execute(q_txn_date).scalar())
    results["details"]["transactions_future_dates"] = txn_viol
    results["violations"] += txn_viol

    if results["violations"] > 0:
        results["status"] = "failed"

    return results


def calculate_quality_score(check_results: Dict[str, Any]) -> float:
    """
    Weighted score across dimensions: completeness, uniqueness, validity, referential integrity.
    Each dimension provided in check_results has its own stats.
    """
    # default weights (can align with config.yaml later)
    weights = {
        "null_checks": 0.25,
        "duplicate_checks": 0.25,
        "range_checks": 0.25,
        "referential_integrity": 0.25,
    }

    score = 0.0

    # completeness (null_checks)
    null_res = check_results.get("null_checks", {})
    null_viol = null_res.get("null_violations", 0)
    # treat 0 violations as perfect 100, any violation reduces score (simple model)
    null_score = 100.0 if null_viol == 0 else max(0.0, 100.0 - null_viol)
    score += null_score * weights["null_checks"]

    # uniqueness (duplicate_checks)
    dup_res = check_results.get("duplicate_checks", {})
    dup_viol = dup_res.get("duplicates_found", 0)
    dup_score = 100.0 if dup_viol == 0 else max(0.0, 100.0 - dup_viol)
    score += dup_score * weights["duplicate_checks"]

    # validity / ranges (range_checks)
    range_res = check_results.get("range_checks", {})
    range_viol = range_res.get("violations", 0)
    range_score = 100.0 if range_viol == 0 else max(0.0, 100.0 - range_viol)
    score += range_score * weights["range_checks"]

    # referential integrity
    ri_res = check_results.get("referential_integrity", {})
    orphan_viol = ri_res.get("orphan_records", 0)
    ri_score = 100.0 if orphan_viol == 0 else max(0.0, 100.0 - orphan_viol * 5)
    score += ri_score * weights["referential_integrity"]

    return round(score, 2)


def main():
    """
    Run all checks on the staging schema and write data/staging/quality_report.json
    """
    engine = _get_engine()
    with engine.connect() as conn:
        null_checks = check_null_values(conn, schema="staging")
        duplicate_checks = check_duplicates(conn, schema="staging")
        referential_integrity = check_referential_integrity(conn, schema="staging")
        range_checks = check_data_ranges(conn, schema="staging")

    check_results = {
        "null_checks": null_checks,
        "duplicate_checks": duplicate_checks,
        "referential_integrity": referential_integrity,
        "range_checks": range_checks,
    }

    overall_quality_score = calculate_quality_score(check_results)

    quality_grade = "A"
    if overall_quality_score < 90:
        quality_grade = "B"
    if overall_quality_score < 75:
        quality_grade = "C"
    if overall_quality_score < 60:
        quality_grade = "D"
    if overall_quality_score < 40:
        quality_grade = "F"

    report = {
        "check_timestamp": datetime.utcnow().isoformat(),
        "checks_performed": check_results,
        "overall_quality_score": overall_quality_score,
        "quality_grade": quality_grade,
    }

    Path("data/staging").mkdir(parents=True, exist_ok=True)
    with open("data/staging/quality_report.json", "w") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
