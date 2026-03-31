import pytest
import pandas as pd
from scripts.transformation.staging_to_production import cleanse_customer_data, cleanse_product_data, apply_business_rules

def test_cleanse_customer():
    df = pd.DataFrame({'first_name': [' JOHN ', 'jane'], 'email': ['TEST@Example.com', 'ok@mail.com']})
    res = cleanse_customer_data(df)
    assert res['first_name'][0] == 'John'
    assert res['email'][0] == 'test@example.com'

def test_business_rules():
    df = pd.DataFrame({'total_amount': [100.0, -50.0, 0.0, 20.0]})
    res = apply_business_rules(df, 'transactions')
    assert len(res) == 2
    assert (res['total_amount'] > 0).all()
