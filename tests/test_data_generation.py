"""
Unit tests for data generation module.
Tests: data shape, referential integrity, business rule validation.
"""
import pytest
import pandas as pd
from pathlib import Path
from scripts.data_generation.generate_data import (
    generate_customers,
    generate_products,
    generate_transactions,
    generate_transaction_items,
    validate_referential_integrity,
)


class TestDataGeneration:
    """Test suite for data generation functions."""

    def test_generate_customers(self):
        """Test customer generation."""
        num_customers = 100
        df = generate_customers(num_customers)

        assert len(df) == num_customers, "Generated customer count mismatch"
        assert "customer_id" in df.columns, "Missing customer_id column"
        assert df["customer_id"].nunique() == num_customers, "Duplicate customer IDs"
        assert df["email"].notna().all(), "NULL emails found"
        assert df["first_name"].notna().all(), "NULL first_name found"
        assert df["last_name"].notna().all(), "NULL last_name found"

    def test_generate_products(self):
        """Test product generation."""
        num_products = 50
        df = generate_products(num_products)

        assert len(df) == num_products, "Generated product count mismatch"
        assert "product_id" in df.columns, "Missing product_id column"
        assert df["product_id"].nunique() == num_products, "Duplicate product IDs"
        assert (df["price"] > 0).all(), "Products with price <= 0"
        assert (df["cost"] >= 0).all(), "Products with negative cost"
        assert (df["cost"] <= df["price"]).all(), "Cost greater than price"

    def test_generate_transactions(self):
        """Test transaction generation."""
        customers = generate_customers(50)
        num_txns = 100
        df = generate_transactions(num_txns, customers)

        assert len(df) == num_txns, "Generated transaction count mismatch"
        assert "transaction_id" in df.columns, "Missing transaction_id column"
        assert df["transaction_id"].nunique() == num_txns, "Duplicate transaction IDs"
        assert df["customer_id"].isin(customers["customer_id"]).all(), "Unknown customer IDs"
        assert df["payment_method"].notna().all(), "NULL payment methods"

    def test_generate_transaction_items(self):
        """Test transaction items generation."""
        customers = generate_customers(30)
        products = generate_products(40)
        transactions = generate_transactions(50, customers)
        items = generate_transaction_items(transactions, products)

        assert len(items) > 0, "No transaction items generated"
        assert "item_id" in items.columns, "Missing item_id column"
        assert items["item_id"].nunique() == len(items), "Duplicate item IDs"
        assert items["transaction_id"].isin(transactions["transaction_id"]).all(), "Unknown transaction IDs"
        assert items["product_id"].isin(products["product_id"]).all(), "Unknown product IDs"
        assert (items["quantity"] > 0).all(), "Items with quantity <= 0"
        assert (items["discount_percentage"] >= 0).all() and (items["discount_percentage"] <= 100).all(), "Invalid discount"
        assert (items["line_total"] > 0).all(), "Items with line_total <= 0"

    def test_validate_referential_integrity(self):
        """Test referential integrity validation."""
        customers = generate_customers(50)
        products = generate_products(30)
        transactions = generate_transactions(100, customers)
        items = generate_transaction_items(transactions, products)

        result = validate_referential_integrity(customers, products, transactions, items)

        assert "orphan_transactions_customers" in result, "Missing orphan_transactions_customers"
        assert "orphan_items_transactions" in result, "Missing orphan_items_transactions"
        assert "orphan_items_products" in result, "Missing orphan_items_products"
        assert result["orphan_transactions_customers"] == 0, "Orphan transactions found"
        assert result["orphan_items_transactions"] == 0, "Orphan items (transactions) found"
        assert result["orphan_items_products"] == 0, "Orphan items (products) found"
        assert result["quality_score"] == 100.0, "Quality score should be perfect"

    def test_transaction_total_calculation(self):
        """Test that transaction totals are correctly calculated."""
        customers = generate_customers(10)
        products = generate_products(5)
        transactions = generate_transactions(20, customers)
        items = generate_transaction_items(transactions, products)

        # Recalculate totals as the main script does
        totals = items.groupby("transaction_id")["line_total"].sum().round(2)
        transactions["total_amount"] = transactions["transaction_id"].map(totals).fillna(0).round(2)

        # Validate
        for txn_id in transactions["transaction_id"]:
            expected_total = items[items["transaction_id"] == txn_id]["line_total"].sum()
            actual_total = transactions[transactions["transaction_id"] == txn_id]["total_amount"].iloc[0]
            assert abs(expected_total - actual_total) < 0.01, f"Total mismatch for {txn_id}"


class TestDataQuality:
    """Test data quality rules."""

    def test_no_duplicate_customers(self):
        """Ensure no duplicate customer IDs."""
        df = generate_customers(100)
        assert df["customer_id"].duplicated().sum() == 0, "Duplicate customer IDs found"

    def test_price_ranges(self):
        """Ensure product prices are in valid range."""
        df = generate_products(100)
        assert (df["price"] > 0).all(), "Found non-positive prices"
        assert (df["price"] <= 10000).all(), "Price exceeds reasonable range"
        assert (df["cost"] < df["price"]).all(), "Cost not less than price"

    def test_date_validity(self):
        """Ensure transaction dates are valid."""
        customers = generate_customers(50)
        txns = generate_transactions(100, customers)
        assert txns["transaction_date"].notna().all(), "NULL transaction dates"
        assert (txns["transaction_date"] <= pd.Timestamp.today().date()).all(), "Future transaction dates"

    def test_age_group_values(self):
        """Ensure age groups are from allowed set."""
        df = generate_customers(100)
        allowed_age_groups = ["18-25", "26-35", "36-45", "46-60", "60+"]
        assert df["age_group"].isin(allowed_age_groups).all(), "Invalid age group values"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
