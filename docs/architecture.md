# E-Commerce Data Pipeline Architecture

## Overview
End-to-end data pipeline extracting synthetic e-commerce data into PostgreSQL via ETL processes to serve analytical tools.

## System Components
1. **Data Generation Layer**: Generates synthetic e-commerce data using Faker.
2. **Data Ingestion Layer**: Loads raw CSV into staging schema using performant bulk functions.
3. **Data Storage Layer**: 
   - `staging`: direct copy of raw CSV structure.
   - `production`: 3NF schema with CHECK constraints and explicit Foreign Keys.
   - `warehouse`: Star schema supporting SCD Type 2 tracking (customers and products) and pre-calculated aggregates.
4. **Data Processing Layer**: Idempotently cleanses data, enforcing data constraints and executing updates mapping Staged tables into Production.
5. **Data Serving Layer**: Aggregation logic populates SQL views/tables containing daily metrics, product behavior, and customer segment metrics.
6. **Orchestration Layer**: Python pipeline orchestrator providing ordered, dependency-aware execution and retry logic.