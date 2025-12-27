# Data Pipeline Architecture

## Overview

This document describes the technical architecture of the e-commerce data pipeline, including system design, data flow, schema architecture, and deployment strategy.

---

## High-Level Architecture Diagram

┌─────────────────────────────────────────────────────────────────┐
│ DATA SOURCES (CSVs)                                              │
│ customers.csv, products.csv, transactions.csv, items.csv        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────┐
│ Data Generation        │ (generate_data.py)
│ - Faker library        │
│ - 1000+ customers     │
│ - 500+ products       │
│ - 10000+ transactions │
│ - 30000+ items        │
└────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────┐
│ STAGING SCHEMA (PostgreSQL)         │
│ ├─ staging.customers               │
│ ├─ staging.products                │
│ ├─ staging.transactions            │
│ └─ staging.transaction_items       │
│ 📊 Raw Data (No Constraints)        │
└────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────┐
│ Data Quality Validation             │
│ (validate_data.py)                 │
│ ✓ Completeness (nulls)             │
│ ✓ Uniqueness (duplicates)          │
│ ✓ Validity (ranges)                │
│ ✓ Consistency (business rules)     │
│ ✓ Referential Integrity (FKs)      │
│ 📈 Quality Score: 0–100             │
└────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────┐
│ PRODUCTION SCHEMA (PostgreSQL)      │
│ ├─ production.customers (3NF)      │
│ ├─ production.products (3NF)       │
│ ├─ production.transactions (3NF)   │
│ └─ production.transaction_items    │
│ 🔒 Cleansed + Validated Data        │
└────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────┐
│ Transformation Layer               │
│ (staging_to_production.py)         │
│ - Cleanse & deduplicate            │
│ - Apply business rules             │
│ - Enrich calculated fields         │
└────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────┐
│ WAREHOUSE SCHEMA (PostgreSQL)       │
│ Star Schema Design                 │
│                                    │
│ Dimensions:                        │
│ ├─ dim_customers (SCD Type 2)      │
│ ├─ dim_products (SCD Type 2)       │
│ ├─ dim_date                        │
│ └─ dim_payment_method              │
│                                    │
│ Facts:                             │
│ └─ fact_sales                      │
│                                    │
│ Aggregates:                        │
│ ├─ agg_daily_sales                 │
│ ├─ agg_product_sales               │
│ └─ agg_customer_lifetime           │
│ 📊 Denormalized for BI              │
└────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────┐
│ BI Dashboards (Tableau/PowerBI)     │
│ ├─ KPI Overview                    │
│ ├─ Sales Trends                    │
│ ├─ Product Performance             │
│ └─ Customer Analytics              │
│ 📈 Business Insights                │
└────────────────────────────────────┘

---

## Schema Architecture

### 1. Staging Schema (Raw Data Layer)

Purpose: Ingest raw data from source systems without transformation

Tables:
- staging.customers
- staging.products
- staging.transactions
- staging.transaction_items

Characteristics:
- Minimal constraints
- No foreign keys
- loaded_at timestamp
- Optimized for bulk inserts

Data Flow:
CSV → ingest_to_staging.py → PostgreSQL

---

### 2. Production Schema (Cleansed Data Layer)

Purpose: Store validated, cleansed business data

Tables:
- production.customers
- production.products
- production.transactions
- production.transaction_items

Characteristics:
- Fully normalized (3NF)
- Primary keys enforced
- Foreign keys enforced
- Surrogate keys
- Audit columns (is_active, created_at, updated_at)
- Indexed join columns

Cleansing Logic:

Customers:
- Remove duplicates
- Standardize names
- Fill missing fields with "Unknown"

Products:
- Fix invalid prices
- Ensure cost < price
- Fill missing supplier_id

Transactions:
- Validate dates
- Validate payment methods
- Handle NULL values

Transaction Items:
- Recalculate line totals
- Remove invalid quantities
- Validate discount range

---

### 3. Warehouse Schema (Dimensional Mart)

Purpose: Analytics-optimized star schema

#### Dimensions

dim_customers (SCD Type 2)
- customer_sk (PK)
- customer_id (business key)
- attributes
- is_current
- effective_date, end_date

dim_products (SCD Type 2)
- product_sk (PK)
- product_id (business key)
- attributes
- is_current
- effective_date, end_date

dim_date
- date_sk
- date attributes

dim_payment_method
- payment_method_sk
- attributes

#### Fact Table

fact_sales
- One row per transaction line item
- Foreign keys to all dimensions
- Measures: quantity, price, discount, totals

#### Aggregates

agg_daily_sales  
agg_product_sales  
agg_customer_lifetime  

---

## Data Flow Pipeline

Step 1: Data Generation  
generate_data.py → CSVs + metadata

Step 2: Ingestion  
ingest_to_staging.py → staging schema

Step 3: Quality Validation  
validate_data.py → quality_report.json

Step 4: Transformation  
staging_to_production.py → production schema

Step 5: Warehouse Loading  
load_warehouse.py → dimensions, facts, aggregates

Step 6: Analytics & BI  
analytics_queries.sql → Tableau / PowerBI

---

## Technology Stack

PostgreSQL 14+  
Python 3.9+  
Pandas  
SQLAlchemy  
Pytest  
Docker  
GitHub Actions  
Tableau / PowerBI  

---

## Performance Optimization

Staging:
- No indexes (fast load)

Production:
- Indexes on foreign keys

Warehouse:
- Indexes on surrogate keys
- Partitioned fact tables
### Production Schema
- Primary Keys (PK) on all tables
- Foreign Keys (FK) on transaction and transaction_item joins
- Indexes on:
  - customer_id
  - product_id
  - transaction_date

### Warehouse Schema
- Indexes on all Foreign Key (FK) columns
- Indexes on:
  - dim_date.date_id
  - dim_customers and dim_products business keys
- Clustered index on fact_sales.date_sk for time-series queries

---

## SCD Type 2 Implementation

**Slowly Changing Dimension Type 2:**  
Track all changes by creating new rows with versioning instead of overwriting data.

### Example: Customer Moves to a Different City

**Before Update**

| customer_sk | customer_id | first_name | city | is_current | effective_date | end_date |
|------------|-------------|------------|------|------------|----------------|----------|
| 1 | CUST001 | John | NYC | TRUE | 2023-01-01 | NULL |

**After Update (New City)**

| customer_sk | customer_id | first_name | city | is_current | effective_date | end_date |
|------------|-------------|------------|------|------------|----------------|----------|
| 1 | CUST001 | John | NYC | FALSE | 2023-01-01 | 2024-12-26 |
| 2 | CUST001 | John | LA  | TRUE  | 2024-12-27 | NULL |

### Benefits
- Historical tracking of customer and product changes
- Ability to analyze impact of attribute changes (e.g., price, location)
- Supports **as-of queries** (state of data at any past date)

---

## Error Handling & Monitoring

### Orchestrator Error Handling
1. **Step Failure:** Log error and continue if configured
2. **Database Connection:** Retry up to 3 times with 30-second delay
3. **Data Validation:** Flag quality issues without blocking pipeline
4. **Transaction Rollback:** TRUNCATE staging tables before reload (idempotent)

### Logging
- All pipeline steps log to `logs/pipeline.log`
- JSON reports generated per stage
- Central execution log with timestamps, status, and error details

### Monitoring Points
✓ Data generation: Record counts, date ranges  
✓ Ingestion: Rows loaded per table, NULL counts  
✓ Quality checks: Scores per dimension, violations  
✓ Transformation: Rows in vs rows out, rule violations  
✓ Warehouse: Dimension counts, fact counts, aggregate counts  

---

## Deployment Options

### Option 1: Local PostgreSQL + Python Virtual Environment

```bash
./setup.sh
export DB_HOST=localhost DB_PORT=5432 DB_NAME=ecommerce_db DB_USER=admin DB_PASSWORD=*****
python scripts/pipeline_orchestrator.py