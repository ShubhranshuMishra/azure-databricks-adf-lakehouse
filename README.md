# Azure Databricks ADF Lakehouse Pipeline

Portfolio project that demonstrates an enterprise-style Azure data engineering pattern using Azure Data Factory for orchestration, Databricks for transformation, and Delta Lake for medallion-style modeling.

## Stack
- Azure Data Factory
- Azure Databricks
- Delta Lake
- Azure Data Lake Storage Gen2
- PySpark
- SQL

## Scenario
This project ingests retail order data from a landing zone into bronze tables, applies cleansing and conformance rules in silver, and produces gold-level business metrics for downstream reporting.

## Architecture
1. Azure Data Factory copies raw CSV files into ADLS Gen2.
2. ADF triggers a Databricks notebook with runtime parameters.
3. Databricks loads raw files into Delta bronze tables.
4. Silver transformations standardize schema, deduplicate, and enforce data quality rules.
5. Gold tables produce curated KPIs such as daily revenue, top products, and country-level sales.

## Repository Layout
```text
adf/pipeline-definition.json
databricks/notebooks/01_bronze_to_silver.py
databricks/notebooks/02_silver_to_gold.py
sample-data/orders.csv
```

## What This Shows Recruiters
- Medallion data modeling
- Parameterized orchestration
- PySpark transformation logic
- Delta Lake merge and partitioning patterns
- Cloud-native batch pipeline design

## How To Demo
1. Upload the sample CSV into your landing container.
2. Create bronze, silver, and gold containers in ADLS.
3. Import the ADF pipeline JSON and wire linked services.
4. Run `01_bronze_to_silver.py`.
5. Run `02_silver_to_gold.py`.

## Improvement Ideas
- Add schema drift handling in ADF
- Add expectations with Great Expectations or Delta Live Tables
- Add CI validation for notebook deployment
