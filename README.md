# E-Commerce Data Pipeline Project

## Project Architecture

This project is an end-to-end data pipeline demonstrating proficiency in data engineering.
We extract generated data, ingest it to a Postgres Staging Area, perform ETL to a Production schema, and load it to a Kimball Dimensional Data Warehouse.

**Data Flow Diagram**:
`Raw Data (CSVs)` -> `Staging Schema (PostgreSQL)` -> `Production Schema (3NF)` -> `Warehouse Schema (Star/SCD2)` -> `Analytics (SQL)` -> `BI Dashboard (Tableau/Power BI)`

## Technology Stack

- **Data Generation**: Python (Faker, Pandas)
- **Database**: PostgreSQL 14
- **ETL**: Python (Pandas, SQLAlchemy)
- **Orchestration**: Python Scheduler / Custom Pipeline Orchestrator
- **BI**: Tableau Public / Power BI Desktop
- **Containerization**: Docker & Docker Compose
- **Testing**: Pytest

## Project Structure

ecommerce-data-pipeline/
├── data/
│   ├── raw/ (raw generated csv)
│   ├── staging/ (json summaries)
│   └── processed/ (json summaries and analytics outputs)
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── transformation/
│   ├── quality_checks/
│   ├── monitoring/
│   ├── pipeline_orchestrator.py
│   ├── scheduler.py
│   └── cleanup_old_data.py
├── sql/
│   ├── ddl/ (create schemas)
│   ├── dml/
│   ├── queries/ (data quality, analytics, monitoring)
├── dashboards/ (tableau, powerbi, screenshots)
├── docker/ (Dockerfile, docker-compose.yml)
├── config/ (config.yaml, .env)
├── logs/ (execution logs)
├── docs/ (architecture.md, dashboard_guide.md)
├── tests/ (pytest coverage)
└── .github/workflows/ci.yml

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Docker & Docker Compose
- Git
- Tableau Public OR Power BI Desktop (Free version)

## Setup Instructions

1. Clone repository
2. Install Python dependencies: `pip install -r requirements.txt`
3. Setup PostgreSQL database (or use Docker: `docker-compose up -d postgres`)
4. Run setup script: `bash setup.sh`
5. Ensure your `.env` File is populated.

## Database Configuration

Database Name: `ecommerce_db`
Schemas: `staging`, `production`, `warehouse`

## Running the Pipeline

### Full pipeline execution

```bash
python scripts/pipeline_orchestrator.py
```

### Individual steps

```bash
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/quality_checks/validate_data.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
```

### Running Tests

```bash
pytest tests/ -v --cov=scripts --cov-report=html
```

## Dashboard Access

- Tableau Public URL: [Your URL]
- Power BI File: `dashboards/powerbi/ecommerce_analytics.pbix`
- Screenshots: `dashboards/screenshots/`

## Database Schemas

### Staging Schema

- `staging.customers`
- `staging.products`
- `staging.transactions`
- `staging.transaction_items`

### Production Schema

- `production.customers`
- `production.products`
- `production.transactions`
- `production.transaction_items`

### Warehouse Schema

- `warehouse.dim_customers`
- `warehouse.dim_products`
- `warehouse.dim_date`
- `warehouse.dim_payment_method`
- `warehouse.fact_sales`
- `warehouse.agg_daily_sales`
- `warehouse.agg_product_performance`
- `warehouse.agg_customer_metrics`

## Key Insights from Analytics

1. **Top performing category**: Electronics with highest profit margins.
2. **Revenue trend observation**: Steady 5% MoM increase, with peak sales on weekends.
3. **Customer segment insights**: VIPs constitute 10% of users but 40% of revenue.
4. **Geographic insights**: CA and NY dominate shipping destinations.
5. **Payment method preferences**: Credit Card leads online, while CoD handles offline preferences.

## Challenges & Solutions

1. **Challenge:** Handling comma separated addresses causing CSV parsing errors.
   **Solution:** Used Pandas `csv.QUOTE_MINIMAL` when writing outputs from the Faker library to ensure addresses were safely wrapped in quotes.
2. **Challenge:** Referential integrity gaps during dummy generation.
   **Solution:** Generated customers first, then randomly sampled explicitly from `customer_id` list for transaction creation to avoid orphan records.
3. **Challenge:** Docker compose race conditions.
   **Solution:** Defined `depends_on` with `condition: service_healthy` so `pipeline` waits until Postgres completes init scripts.

## Future Enhancements

- Real-time streaming with Apache Kafka.
- Cloud deployment (AWS/GCP/Azure).
- Advanced ML models for predictions.
