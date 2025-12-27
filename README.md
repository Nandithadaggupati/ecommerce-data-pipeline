# E-Commerce Data Pipeline

A production-ready end-to-end ETL/ELT pipeline for e-commerce analytics that demonstrates proficiency in data engineering, database design, data quality assurance, automation, and business intelligence.

Deadline: 27 Dec 2025, 04:59 PM IST

---

## Objective

Build a complete data pipeline that:
- Generates realistic e-commerce data (30,000+ records)
- Implements three-tier schema architecture (staging → production → warehouse)
- Ensures data quality with comprehensive validation
- Transforms and cleanses data following business rules
- Loads dimensional warehouse with SCD Type 2 support
- Provides 10+ analytical queries
- Orchestrates with error handling and monitoring
- Implements >80% unit test coverage
- Includes Docker, CI/CD, and professional documentation

---

## Project Structure

ecommerce-data-pipeline/
├── config/
│   └── config.yaml
├── data/
│   ├── raw/
│   ├── staging/
│   └── processed/
├── docker/
│   └── docker-compose.yml
├── logs/
│   └── pipeline.log
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── quality_checks/
│   ├── transformation/
│   └── pipeline_orchestrator.py
├── sql/
│   ├── ddl/
│   └── queries/
├── tests/
├── .env.example
├── setup.sh
├── requirements.txt
└── README.md

---

## Quick Start

### Prerequisites
- Python 3.9+
- PostgreSQL 14+
- Docker & Docker Compose (optional)
- Git

---

## Setup

git clone https://github.com/YOUR_USERNAME/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
chmod +x setup.sh
./setup.sh
source .venv/bin/activate
cp .env.example .env

Edit .env with your database credentials.

---

## Database Setup

### Option A: Local PostgreSQL

CREATE DATABASE ecommerce_db;
CREATE ROLE admin WITH LOGIN PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE ecommerce_db TO admin;

psql -h localhost -U admin -d ecommerce_db -f sql/ddl/create_staging_schema.sql
psql -h localhost -U admin -d ecommerce_db -f sql/ddl/create_production_schema.sql
psql -h localhost -U admin -d ecommerce_db -f sql/ddl/create_warehouse_schema.sql

---

### Option B: Docker

cd docker
docker-compose up -d

Schemas auto-initialize.

---

## Run the Pipeline

export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=ecommerce_db
export DB_USER=admin
export DB_PASSWORD=password

python scripts/pipeline_orchestrator.py

View logs:
cat data/processed/pipeline_execution_log.json

---

## Pipeline Components

1. Data Generation  
- 1,000 customers  
- 500 products  
- 10,000 transactions  
- 30,000+ transaction items  

2. Ingestion  
- CSV → staging  
- Row count validation  

3. Quality Checks  
- Completeness  
- Uniqueness  
- Validity  
- Consistency  
- Referential integrity  
- Quality score (0–100)

4. Transformation  
- Deduplication  
- Standardization  
- Business rules  

5. Warehouse  
- Star schema  
- SCD Type 2 dimensions  
- Aggregates (daily, product, CLV)

---

## Analytics Queries

Includes:
- Sales trends
- Top products
- Customer segmentation (RFM)
- Payment method analysis
- Geographic insights
- Customer lifetime value

---

## Testing

pytest tests/ -v
pytest tests/ --cov=scripts --cov-report=html

Coverage target: >80%

---

## BI Dashboard

Pages:
1. KPI Overview
2. Sales Trends
3. Product Performance
4. Customer Analytics
5. Advanced Insights

Connection:
Server: localhost  
Database: ecommerce_db  
Schema: warehouse  
User: admin  
Password: from .env  

---

## Configuration

config/config.yaml

data_generation:
  num_customers: 1000
  num_products: 500
  num_transactions: 10000

pipeline:
  batch_size: 1000
  log_level: INFO
  max_retries: 3

---

## Sample Queries

SELECT SUM(line_total) FROM warehouse.fact_sales;

SELECT customer_id, total_spent
FROM warehouse.agg_customer_lifetime
ORDER BY total_spent DESC
LIMIT 5;

SELECT category, COUNT(*)
FROM warehouse.dim_products
WHERE is_current = TRUE
GROUP BY category;

---

## License

MIT License

---

## Author

Nanditha Daggu  
GitHub: https://github.com/yourname  
Email: nanditha@example.com  

---

Last Updated: 26 Dec 2025  
Status: In Development