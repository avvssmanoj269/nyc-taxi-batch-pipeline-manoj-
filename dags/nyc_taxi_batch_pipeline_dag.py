from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "manoj",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def check_landing_files():
    """
    Check whether all expected NYC Taxi source files exist in S3.
    """
    print("Checking 36 landing files in S3...")
    print("This will call the S3 file existence logic from src/check_landing_files.py")


def run_bronze_ingestion():
    """
    Run Bronze ingestion.
    """
    print("Running Bronze ingestion...")
    print("This will call src/bronze_ingestion.py")


def run_silver_transformation():
    """
    Run Silver transformation.
    """
    print("Running Silver transformation...")
    print("This will call src/silver_transformation.py")


def run_data_quality_checks():
    """
    Run data quality checks.
    """
    print("Running data quality checks...")
    print("This will call src/data_quality_checks.py")


def run_gold_analytics():
    """
    Run Gold analytics table generation.
    """
    print("Running Gold analytics...")
    print("This will call src/gold_analytics.py")


with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    description="End-to-end batch pipeline for 3 years of NYC Yellow Taxi data",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["data-engineering", "pyspark", "s3", "nyc-taxi"],
) as dag:

    check_landing_files_task = PythonOperator(
        task_id="check_landing_files",
        python_callable=check_landing_files,
    )

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
        check_landing_files_task
        >> bronze_ingestion_task
        >> silver_transformation_task
        >> data_quality_checks_task
        >> gold_analytics_task
    )
