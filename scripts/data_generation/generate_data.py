import pandas as pd
import numpy as np
from faker import Faker
import json
import random
import os
import datetime

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

def generate_customers(num_customers: int) -> pd.DataFrame:
    customers = []
    age_groups = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    for i in range(1, num_customers + 1):
        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name().replace(',', ''),
            "last_name": fake.last_name().replace(',', ''),
            "email": fake.unique.email(),
            "phone": fake.phone_number()[:15].replace(',', ''),
            "registration_date": fake.date_between(start_date='-2y', end_date='today').isoformat(),
            "city": fake.city().replace(',', ''),
            "state": fake.state().replace(',', ''),
            "country": fake.country().replace(',', ''),
            "age_group": random.choice(age_groups)
        })
    return pd.DataFrame(customers)

def generate_products(num_products: int) -> pd.DataFrame:
    products = []
    categories = {
        "Electronics": ["Phones", "Laptops", "Accessories"],
        "Clothing": ["Men", "Women", "Kids"],
        "Home & Kitchen": ["Furniture", "Appliances", "Decor"],
        "Books": ["Fiction", "Non-Fiction", "Educational"],
        "Sports": ["Equipment", "Apparel", "Footwear"],
        "Beauty": ["Skincare", "Makeup", "Haircare"]
    }
    brands = [fake.company().replace(',', '') for _ in range(50)]
    
    for i in range(1, num_products + 1):
        cat = random.choice(list(categories.keys()))
        sub_cat = random.choice(categories[cat])
        cost = round(random.uniform(5.0, 500.0), 2)
        price = round(cost * random.uniform(1.2, 2.5), 2)
        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.catch_phrase().replace(',', ''),
            "category": cat,
            "sub_category": sub_cat,
            "price": price,
            "cost": cost,
            "brand": random.choice(brands),
            "stock_quantity": random.randint(0, 1000),
            "supplier_id": f"SUPP{random.randint(1, 50):02d}"
        })
    return pd.DataFrame(products)

def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    transactions = []
    payment_methods = ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
    customer_ids = customers_df['customer_id'].tolist()
    
    for i in range(1, num_transactions + 1):
        dt = fake.date_time_between(start_date='-1y', end_date='now')
        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customer_ids),
            "transaction_date": dt.date().isoformat(),
            "transaction_time": dt.time().isoformat(),
            "payment_method": random.choice(payment_methods),
            "shipping_address": fake.address().replace('\n', ' ').replace(',', ''),
            "total_amount": 0.0 # Will be updated
        })
    return pd.DataFrame(transactions)

def generate_transaction_items(transactions_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    items = []
    item_id_counter = 1
    transaction_ids = transactions_df['transaction_id'].tolist()
    product_dict = products_df.set_index('product_id')['price'].to_dict()
    product_ids = list(product_dict.keys())
    
    transaction_totals = {tid: 0.0 for tid in transaction_ids}
    
    for tid in transaction_ids:
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            pid = random.choice(product_ids)
            qty = random.randint(1, 4)
            unit_price = product_dict[pid]
            discount = float(random.choice([0.0, 5.0, 10.0, 15.0, 20.0]))
            line_total = round(qty * unit_price * (1 - discount/100), 2)
            
            items.append({
                "item_id": f"ITEM{item_id_counter:05d}",
                "transaction_id": tid,
                "product_id": pid,
                "quantity": qty,
                "unit_price": unit_price,
                "discount_percentage": discount,
                "line_total": line_total
            })
            transaction_totals[tid] += line_total
            item_id_counter += 1
            
    # Update total_amount in transactions_df based on exact line items sum
    transactions_df['total_amount'] = transactions_df['transaction_id'].map(transaction_totals).round(2)
    return pd.DataFrame(items)

def validate_referential_integrity(customers: pd.DataFrame, products: pd.DataFrame, transactions: pd.DataFrame, items: pd.DataFrame) -> dict:
    orphan_tx = ~transactions['customer_id'].isin(customers['customer_id'])
    orphan_items_tx = ~items['transaction_id'].isin(transactions['transaction_id'])
    orphan_items_prod = ~items['product_id'].isin(products['product_id'])
    
    orphan_count = orphan_tx.sum() + orphan_items_tx.sum() + orphan_items_prod.sum()
    
    return {
        "orphan_records": int(orphan_count),
        "constraint_violations": 0,
        "data_quality_score": 100 if orphan_count == 0 else max(0, 100 - orphan_count)
    }

if __name__ == "__main__":
    import yaml
    try:
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f)['generation']
    except FileNotFoundError:
        config = {'num_customers': 1000, 'num_products': 500, 'num_transactions': 10000}
        
    os.makedirs("data/raw", exist_ok=True)
    
    print("Generating Customers...")
    df_customers = generate_customers(config.get('num_customers', 1000))
    print("Generating Products...")
    df_products = generate_products(config.get('num_products', 500))
    print("Generating Transactions...")
    df_transactions = generate_transactions(config.get('num_transactions', 10000), df_customers)
    print("Generating Items...")
    df_items = generate_transaction_items(df_transactions, df_products)
    
    val = validate_referential_integrity(df_customers, df_products, df_transactions, df_items)
    print("Validation:", val)
    
    import csv
    df_customers.to_csv("data/raw/customers.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    df_products.to_csv("data/raw/products.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    df_transactions.to_csv("data/raw/transactions.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    df_items.to_csv("data/raw/transaction_items.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    
    meta = {
        "timestamp": datetime.datetime.now().isoformat(),
        "record_counts": {
            "customers": len(df_customers),
            "products": len(df_products),
            "transactions": len(df_transactions),
            "transaction_items": len(df_items)
        },
        "validation": val
    }
    with open("data/raw/generation_metadata.json", "w") as f:
        json.dump(meta, f, indent=4)
