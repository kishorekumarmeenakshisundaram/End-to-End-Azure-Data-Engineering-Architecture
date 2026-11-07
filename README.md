# End-to-End Azure Data Engineering Architecture

An end-to-end Azure Data Engineering project built using Azure Data Factory, Azure Databricks, PySpark, Azure Data Lake Storage Gen2, and Delta Lake. This project processes Brazilian e-commerce data through a Medallion Architecture (Bronze, Silver, and Gold) and performs data quality validation.

## Project Overview

The project demonstrates how raw e-commerce data can be ingested, transformed, organized, and validated using Microsoft Azure data engineering services.

The Olist Brazilian E-Commerce dataset is used as the source. Eight CSV files are processed through Bronze, Silver, and Gold layers to produce analytical datasets.

## Architecture

![E-Commerce Data Engineering Architecture](architecture/E-Commerce%20Data%20Engineering%20Architecture.png)

Azure Data Factory orchestrates the Databricks notebooks, which perform the data transformations and validation.

## Technology Stack

- Microsoft Azure
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Databricks
- PySpark
- Delta Lake
- Python
- SQL

## Dataset

Source: Brazilian E-Commerce Public Dataset by Olist.

The project uses eight datasets:

- Customers
- Orders
- Order Items
- Order Payments
- Order Reviews
- Products
- Sellers
- Product Category Name Translation

## Data Processing

### Bronze Layer
- Stores the original CSV files in Azure Data Lake Storage Gen2.
- Maintains the raw source data for downstream processing.

### Silver Layer
- Reads raw data from Bronze.
- Standardizes column names and trims string values.
- Handles date and timestamp conversions.
- Removes duplicate records using business keys.
- Writes cleaned datasets in Delta format.

### Gold Layer

Creates five analytical Delta tables:

1. `gold_daily_sales` – Daily order counts and sales metrics.
2. `gold_category_performance` – Product category-level performance.
3. `gold_customer_state_sales` – Customer-state-level sales analysis.
4. `gold_payment_analysis` – Payment-type analysis.
5. `gold_review_analysis` – Review score distribution.

## Data Quality Checks

A separate Databricks notebook performs validation checks, including:

- Duplicate record checks.
- Null value checks.
- Schema validation.
- Review score validation.
- Date range validation.
- Business-level checks on Gold datasets.

## Pipeline Orchestration

Azure Data Factory orchestrates three Databricks notebooks in sequence:

1. `01_bronze_to_silver`
2. `02_silver_to_gold`
3. `03_data_quality_checks`

The activities are connected through success dependencies to execute the workflow in order.

## Repository Structure

```text
ecommerce-data-engineering/
│
├── architecture/
│   └── E-Commerce Data Engineering Architecture.png
│
├── notebooks/
│   ├── 01_bronze_to_silver.py
│   ├── 02_silver_to_gold.py
│   └── 03_data_quality_checks.py
│
├── screenshots/
│   ├── azure_resources/
│   ├── adf_pipeline/
│   ├── databricks/
│   └── data_quality/
│
└── README.md
```

## Key Outcomes

- Built an end-to-end cloud-based data engineering workflow.
- Implemented Bronze, Silver, and Gold data layers.
- Transformed raw CSV data into Delta Lake tables.
- Developed analytical datasets using PySpark.
- Orchestrated notebook execution using Azure Data Factory.
- Implemented data quality validation checks.

## Future Enhancements

- Integrate a business intelligence tool for reporting and visualization.
- Add automated monitoring and alerting.
- Extend the pipeline with incremental data processing.

## Disclaimer

This is a portfolio project developed for learning and demonstrating Azure Data Engineering concepts. Azure resources and configurations may differ when recreated in another environment.
