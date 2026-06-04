# Architecture

## Pipeline Flow

```text
NYC Taxi Monthly Parquet Files
        ↓
AWS S3 Landing Zone
        ↓
Bronze Ingestion
        ↓
Silver Transformation
        ↓
Data Quality Checks
        ↓
Gold Analytics Tables
        ↓
Airflow DAG Orchestration
