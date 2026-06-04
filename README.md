# NYC Taxi End-to-End Batch Data Pipeline

## Project Overview

This project is an end-to-end batch data engineering pipeline built using NYC Yellow Taxi trip data for 2023, 2024, and 2025.

The pipeline ingests raw taxi trip data from AWS S3, processes it using PySpark, applies data quality checks, and creates analytics-ready Gold tables.

## Tech Stack

- Python
- PySpark
- AWS S3
- Apache Airflow
- Parquet
- Boto3
- Data quality checks using PySpark / Great Expectations-style rules

## Architecture

```text
NYC Taxi Source Data
        ↓
AWS S3 Landing Zone
        ↓
Bronze Layer
Raw data + metadata
        ↓
Silver Layer
Cleaned, deduplicated, enriched trip-level data
        ↓
Data Quality Layer
Validation checks on Silver data
        ↓
Gold Layer
Daily, monthly, and zone-level analytics tables
        ↓
Airflow DAG
Pipeline orchestration
