"""
Integration tests for the complete pipeline.
Tests end-to-end flow from generation to warehouse.
"""
import pytest
import json
from pathlib import Path
from scripts.data_generation.generate_data import main as generate_data
from scripts.ingestion.ingest_to_staging import main as ingest_to_staging
from scripts.quality_checks.validate_data import main as validate_data
from scripts.transformation.staging_to_production import main as staging_to_production
from scripts.transformation.load_warehouse import main as load_warehouse


class TestFullPipelineIntegration:
    """Integration tests for complete ETL pipeline."""

    def test_generation_creates_files(self):
        """Test that data generation creates required files."""
        generate_data()
        
        assert Path("data/raw/customers.csv").exists(), "customers.csv not created"
        assert Path("data/raw/products.csv").exists(), "products.csv not created"
        assert Path("data/raw/transactions.csv").exists(), "transactions.csv not created"
        assert Path("data/raw/transaction_items.csv").exists(), "transaction_items.csv not created"
        assert Path("data/raw/generation_metadata.json").exists(), "metadata not created"

    def test_generation_metadata_valid(self):
        """Test that generation metadata has required fields."""
        with open("data/raw/generation_metadata.json") as f:
            meta = json.load(f)
        
        assert "generated_at" in meta, "Missing generated_at"
        assert "record_counts" in meta, "Missing record_counts"
        assert meta["record_counts"]["customers"] > 0, "No customers generated"
        assert meta["record_counts"]["products"] > 0, "No products generated"
        assert meta["record_counts"]["transactions"] > 0, "No transactions generated"
        assert meta["record_counts"]["transaction_items"] >= 30000, "Not enough items"

    def test_ingestion_creates_report(self):
        """Test that ingestion creates summary report."""
        ingest_to_staging()
        
        assert Path("data/staging/ingestion_summary.json").exists(), "Ingestion summary not created"
        with open("data/staging/ingestion_summary.json") as f:
            summary = json.load(f)
        
        assert "ingestion_timestamp" in summary, "Missing timestamp"
        assert "tables_loaded" in summary, "Missing table stats"

    def test_quality_creates_report(self):
        """Test that quality checks create report."""
        validate_data()
        
        assert Path("data/staging/quality_report.json").exists(), "Quality report not created"
        with open("data/staging/quality_report.json") as f:
            report = json.load(f)
        
        assert "overall_quality_score" in report, "Missing quality score"
        assert "quality_grade" in report, "Missing grade"
        assert report["overall_quality_score"] >= 0, "Invalid score"
        assert report["overall_quality_score"] <= 100, "Invalid score"

    def test_transformation_creates_report(self):
        """Test that transformation creates report."""
        staging_to_production()
        
        assert Path("data/processed/transformation_summary.json").exists(), "Transform report not created"
        with open("data/processed/transformation_summary.json") as f:
            report = json.load(f)
        
        assert "tables_transformed" in report, "Missing transformation stats"

    def test_warehouse_creates_report(self):
        """Test that warehouse loading creates report."""
        load_warehouse()
        
        assert Path("data/processed/warehouse_build_report.json").exists(), "Warehouse report not created"
        with open("data/processed/warehouse_build_report.json") as f:
            report = json.load(f)
        
        assert "dimensions_built" in report, "Missing dimension stats"
        assert "fact_tables_built" in report, "Missing fact stats"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
