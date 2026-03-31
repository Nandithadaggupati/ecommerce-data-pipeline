import pytest
import pandas as pd
from scripts.data_generation.generate_data import (
    generate_customers, generate_products, 
    generate_transactions, generate_transaction_items,
    validate_referential_integrity
)

def test_generate_customers():
    df = generate_customers(10)
    assert len(df) == 10
    assert 'customer_id' in df.columns
    assert 'email' in df.columns
    assert not df['email'].isnull().any()

def test_generate_products():
    df = generate_products(10)
    assert len(df) == 10
    assert 'product_id' in df.columns
    assert 'price' in df.columns
    assert (df['price'] > 0).all()
    assert (df['cost'] < df['price']).all()

def test_referential_integrity():
    # Generate small dataset
    c = generate_customers(5)
    p = generate_products(5)
    t = generate_transactions(15, c)
    i = generate_transaction_items(t, p)
    
    val = validate_referential_integrity(c, p, t, i)
    assert val['orphan_records'] == 0
    assert val['data_quality_score'] == 100
    
    # Introduce orphan
    i.loc[0, 'transaction_id'] = 'INVALID_TX'
    val_bad = validate_referential_integrity(c, p, t, i)
    assert val_bad['orphan_records'] > 0
    assert val_bad['data_quality_score'] < 100
