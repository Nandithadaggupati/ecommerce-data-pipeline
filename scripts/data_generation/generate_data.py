import pandas as pd
from faker import Faker
import json
from datetime import datetime
import random
import yaml
from pathlib import Path

fake = Faker()

def generate_customers(num_customers: int) -> pd.DataFrame:
    rows = []
    for i in range(1, num_customers + 1):
        cid = f"CUST{i:06d}"
        rows.append({
            "customer_id": cid,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"{cid.lower()}@example.com",
            "phone": fake.msisdn(),
            "registration_date": fake.date_between(start_date="-2y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(["18-25", "26-35", "36-45", "46-60", "60+"]),
        })
    return pd.DataFrame(rows)

def generate_products(num_products: int) -> pd.DataFrame:
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Beauty"]
    rows = []
    for i in range(1, num_products + 1):
        pid = f"PROD{i:06d}"
        category = random.choice(categories)
        price = round(random.uniform(10, 500), 2)
        cost = round(price * random.uniform(0.4, 0.8), 2)
        rows.append({
            "product_id": pid,
            "product_name": f"{category} Product {i}",
            "category": category,
            "sub_category": "General",
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(0, 1000),
            "supplier_id": f"SUP{random.randint(1, 50):04d}",
        })
    return pd.DataFrame(rows)

def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    customer_ids = customers_df["customer_id"].tolist()
    payment_methods = ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
    rows = []
    for i in range(1, num_transactions + 1):
        tid = f"TXN{i:07d}"
        cid = random.choice(customer_ids)
        date = fake.date_between(start_date="-1y", end_date="today")
        time = fake.time()
        pm = random.choice(payment_methods)
        rows.append({
            "transaction_id": tid,
            "customer_id": cid,
            "transaction_date": date,
            "transaction_time": time,
            "payment_method": pm,
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.0,
        })
    return pd.DataFrame(rows)

def generate_transaction_items(transactions_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    product_ids = products_df["product_id"].tolist()
    price_map = dict(zip(products_df["product_id"], products_df["price"]))
    rows = []
    item_counter = 1
    for _, txn in transactions_df.iterrows():
        n_items = random.randint(1, 5)
        for _ in range(n_items):
            iid = f"ITEM{item_counter:07d}"
            item_counter += 1
            pid = random.choice(product_ids)
            qty = random.randint(1, 5)
            unit_price = float(price_map[pid])
            discount = random.choice([0, 5, 10, 15, 20])
            line_total = round(qty * unit_price * (1 - discount / 100), 2)
            rows.append({
                "item_id": iid,
                "transaction_id": txn["transaction_id"],
                "product_id": pid,
                "quantity": qty,
                "unit_price": unit_price,
                "discount_percentage": discount,
                "line_total": line_total,
            })
    return pd.DataFrame(rows)

def validate_referential_integrity(customers, products, transactions, items) -> dict:
    customer_ids = set(customers["customer_id"])
    product_ids = set(products["product_id"])
    txn_ids = set(transactions["transaction_id"])

    orphan_txn_customers = sum(1 for cid in transactions["customer_id"] if cid not in customer_ids)
    orphan_items_txn = sum(1 for tid in items["transaction_id"] if tid not in txn_ids)
    orphan_items_prod = sum(1 for pid in items["product_id"] if pid not in product_ids)
    total_violations = orphan_txn_customers + orphan_items_txn + orphan_items_prod
    quality_score = 100.0 if total_violations == 0 else max(0.0, 100.0 - total_violations)

    return {
        "orphan_transactions_customers": orphan_txn_customers,
        "orphan_items_transactions": orphan_items_txn,
        "orphan_items_products": orphan_items_prod,
        "total_violations": total_violations,
        "quality_score": quality_score,
    }

def _load_config():
    cfg_path = Path("config/config.yaml")
    if cfg_path.exists():
        return yaml.safe_load(cfg_path.read_text())
    return {}

def main():
    cfg = _load_config()
    gen_cfg = cfg.get("data_generation", {})
    n_cust = gen_cfg.get("num_customers", 1000)
    n_prod = gen_cfg.get("num_products", 500)
    n_txn = gen_cfg.get("num_transactions", 10000)

    customers = generate_customers(n_cust)
    products = generate_products(n_prod)
    transactions = generate_transactions(n_txn, customers)
    items = generate_transaction_items(transactions, products)

    totals = items.groupby("transaction_id")["line_total"].sum().round(2)
    transactions["total_amount"] = transactions["transaction_id"].map(totals).fillna(0).round(2)

    Path("data/raw").mkdir(parents=True, exist_ok=True)
    customers.to_csv("data/raw/customers.csv", index=False)
    products.to_csv("data/raw/products.csv", index=False)
    transactions.to_csv("data/raw/transactions.csv", index=False)
    items.to_csv("data/raw/transaction_items.csv", index=False)

    meta = {
        "generated_at": datetime.utcnow().isoformat(),
        "record_counts": {
            "customers": len(customers),
            "products": len(products),
            "transactions": len(transactions),
            "transaction_items": len(items),
        },
        "transaction_date_min": str(transactions["transaction_date"].min()),
        "transaction_date_max": str(transactions["transaction_date"].max()),
        "referential_integrity": validate_referential_integrity(
            customers, products, transactions, items
        ),
    }
    with open("data/raw/generation_metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

if __name__ == "__main__":
    main()
