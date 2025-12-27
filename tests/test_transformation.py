"""
Unit tests for transformation modules.
Tests: cleansing, business rules, and loading logic.
"""
import pytest
import pandas as pd
from scripts.transformation.staging_to_production import (
    cleanse_customer_data,
    cleanse_product_data,
    apply_business_rules,
)


class TestCustomerCleansing:
    """Test customer data cleansing."""

    def test_remove_duplicate_customers(self):
        """Ensure duplicates are removed (first kept)."""
        df = pd.DataFrame({
            "customer_id": ["CUST001", "CUST001", "CUST002"],
            "first_name": ["John", "Jane", "Bob"],
            "last_name": ["Doe", "Doe", "Smith"],
            "email": ["john@example.com", "jane@example.com", "bob@example.com"],
            "phone": ["123", "124", "125"],
            "registration_date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "city": ["NYC", "LA", "Chicago"],
            "state": ["NY", "CA", "IL"],
            "country": ["USA", "USA", "USA"],
            "age_group": ["25-35", "35-45", "45-60"],
        })
        result = cleanse_customer_data(df)
        assert len(result) == 2, "Duplicates not removed"
        assert result[result["customer_id"] == "CUST001"]["first_name"].iloc[0] == "John", "Wrong duplicate kept"

    def test_standardize_names(self):
        """Ensure names are title-cased."""
        df = pd.DataFrame({
            "customer_id": ["CUST001"],
            "first_name": ["JOHN"],
            "last_name": ["doe"],
            "email": ["test@example.com"],
            "phone": ["123"],
            "registration_date": pd.to_datetime(["2023-01-01"]),
            "city": ["NYC"],
            "state": ["NY"],
            "country": ["USA"],
            "age_group": ["25-35"],
        })
        result = cleanse_customer_data(df)
        assert result["first_name"].iloc[0] == "John", "First name not title-cased"
        assert result["last_name"].iloc[0] == "Doe", "Last name not title-cased"

    def test_fill_missing_phone(self):
        """Ensure missing phones are filled."""
        df = pd.DataFrame({
            "customer_id": ["CUST001"],
            "first_name": ["John"],
            "last_name": ["Doe"],
            "email": ["test@example.com"],
            "phone": [None],
            "registration_date": pd.to_datetime(["2023-01-01"]),
            "city": ["NYC"],
            "state": ["NY"],
            "country": ["USA"],
            "age_group": ["25-35"],
        })
        result = cleanse_customer_data(df)
        assert result["phone"].iloc[0] == "Unknown", "Missing phone not filled"


class TestProductCleansing:
    """Test product data cleansing."""

    def test_remove_duplicate_products(self):
        """Ensure duplicate products are removed."""
        df = pd.DataFrame({
            "product_id": ["PROD001", "PROD001", "PROD002"],
            "product_name": ["Widget", "Gadget", "Tool"],
            "category": ["Electronics", "Electronics", "Tools"],
            "sub_category": ["General", "General", "General"],
            "price": [100.00, 110.00, 50.00],
            "cost": [50.00, 55.00, 25.00],
            "brand": ["BrandA", "BrandA", "BrandB"],
            "stock_quantity": [10, 20, 5],
            "supplier_id": ["SUP001", "SUP001", "SUP002"],
        })
        result = cleanse_product_data(df)
        assert len(result) == 2, "Duplicates not removed"
        assert result[result["product_id"] == "PROD001"]["price"].iloc[0] == 100.00, "Wrong duplicate kept"

    def test_fix_invalid_prices(self):
        """Ensure invalid prices are fixed."""
        df = pd.DataFrame({
            "product_id": ["PROD001", "PROD002"],
            "product_name": ["Widget", "Gadget"],
            "category": ["Electronics", "Electronics"],
            "sub_category": ["General", "General"],
            "price": [0, 100.00],
            "cost": [50.00, 50.00],
            "brand": ["BrandA", "BrandB"],
            "stock_quantity": [10, 5],
            "supplier_id": ["SUP001", "SUP002"],
        })
        result = cleanse_product_data(df)
        assert result[result["product_id"] == "PROD001"]["price"].iloc[0] > 0, "Invalid price not fixed"

    def test_fix_cost_greater_than_price(self):
        """Ensure cost < price constraint."""
        df = pd.DataFrame({
            "product_id": ["PROD001"],
            "product_name": ["Widget"],
            "category": ["Electronics"],
            "sub_category": ["General"],
            "price": [100.00],
            "cost": [150.00],  # cost > price
            "brand": ["BrandA"],
            "stock_quantity": [10],
            "supplier_id": ["SUP001"],
        })
        result = cleanse_product_data(df)
        assert result["cost"].iloc[0] < result["price"].iloc[0], "Cost not less than price"


class TestBusinessRules:
    """Test business rule application."""

    def test_transaction_item_line_total(self):
        """Ensure line_total is correctly calculated."""
        df = pd.DataFrame({
            "item_id": ["ITEM001"],
            "transaction_id": ["TXN001"],
            "product_id": ["PROD001"],
            "quantity": [2],
            "unit_price": [100.00],
            "discount_percentage": [10],
            "line_total": [0],  # Will be recalculated
        })
        result = apply_business_rules(df, "transaction_item")
        expected = 2 * 100.00 * (1 - 10 / 100)
        assert abs(result["line_total"].iloc[0] - expected) < 0.01, "Line total calculation incorrect"

    def test_invalid_discount_removed(self):
        """Ensure items with invalid discounts are removed."""
        df = pd.DataFrame({
            "item_id": ["ITEM001", "ITEM002"],
            "transaction_id": ["TXN001", "TXN001"],
            "product_id": ["PROD001", "PROD002"],
            "quantity": [2, 1],
            "unit_price": [100.00, 50.00],
            "discount_percentage": [150, 10],  # 150% invalid
            "line_total": [180.00, 45.00],
        })
        result = apply_business_rules(df, "transaction_item")
        assert len(result) == 1, "Invalid discount not removed"

    def test_zero_quantity_removed(self):
        """Ensure items with zero quantity are removed."""
        df = pd.DataFrame({
            "item_id": ["ITEM001", "ITEM002"],
            "transaction_id": ["TXN001", "TXN001"],
            "product_id": ["PROD001", "PROD002"],
            "quantity": [0, 1],
            "unit_price": [100.00, 50.00],
            "discount_percentage": [0, 0],
            "line_total": [0, 50.00],
        })
        result = apply_business_rules(df, "transaction_item")
        assert len(result) == 1, "Zero quantity not removed"

    def test_transaction_rule_fill_null_amounts(self):
        """Ensure NULL transaction amounts are filled."""
        df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002"],
            "customer_id": ["CUST001", "CUST002"],
            "transaction_date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
            "transaction_time": ["10:00:00", "11:00:00"],
            "payment_method": ["Card", "Cash"],
            "shipping_address": ["addr1", "addr2"],
            "total_amount": [100.00, None],
        })
        result = apply_business_rules(df, "transaction")
        assert result["total_amount"].notna().all(), "NULL amounts not filled"


class TestDataValidation:
    """Test data validation after transformation."""

    def test_no_nulls_in_key_columns(self):
        """Ensure key columns have no NULLs after cleansing."""
        df = pd.DataFrame({
            "customer_id": ["CUST001"],
            "first_name": [None],
            "email": ["test@example.com"],
            "phone": ["123"],
            "registration_date": pd.to_datetime(["2023-01-01"]),
            "city": ["NYC"],
            "state": ["NY"],
            "country": ["USA"],
            "age_group": ["25-35"],
            "last_name": ["Doe"],
        })
        result = cleanse_customer_data(df)
        assert result["first_name"].notna().all(), "NULLs in first_name after cleansing"

    def test_no_duplicates_after_cleansing(self):
        """Ensure no duplicates after cleansing."""
        df = pd.DataFrame({
            "customer_id": ["CUST001", "CUST001"],
            "first_name": ["John", "Jane"],
            "last_name": ["Doe", "Doe"],
            "email": ["john@example.com", "jane@example.com"],
            "phone": ["123", "124"],
            "registration_date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
            "city": ["NYC", "LA"],
            "state": ["NY", "CA"],
            "country": ["USA", "USA"],
            "age_group": ["25-35", "35-45"],
        })
        result = cleanse_customer_data(df)
        assert result["customer_id"].duplicated().sum() == 0, "Duplicates not removed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
