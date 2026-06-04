from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.bronze_ingestion import run_bronze_ingestion
from src.silver_transformation import run_silver_transformation
from src.data_quality_checks import run_data_quality_checks
from src.gold_analytics import run_gold_analytics


default_args = {
    "owner": "manoj",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    description="End-to-end batch pipeline for 3 years of NYC Yellow Taxi data using PySpark and AWS S3",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["data-engineering", "pyspark", "aws-s3", "nyc-taxi"],
) as dag:

    bronze_ingestion_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze_ingestion,
    )

    silver_transformation_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver_transformation,
    )

    data_quality_checks_task = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    gold_analytics_task = PythonOperator(
        task_id="gold_analytics",
        python_callable=run_gold_analytics,
    )

    (
        bronze_ingestion_task
        >> silver_transformation_task
        >> data_quality_checks_task
        >> gold_analytics_task
    )
