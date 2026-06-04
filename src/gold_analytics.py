
import json
from datetime import datetime, timezone

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, current_date, year, month


BUCKET_NAME = "nyc-taxi-batch-pipeline-manoj"
AWS_REGION = "eu-west-1"

SILVER_PATH = f"s3a://{BUCKET_NAME}/silver/nyc_taxi/yellow_trips_clean/"
QUALITY_RESULTS_PREFIX = "quality/validation_results/"

YEARS_TO_PROCESS = [2023, 2024, 2025]
MONTHS_TO_PROCESS = list(range(1, 13))


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("nyc_taxi_data_quality_checks")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.endpoint.region", "eu-west-1")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )

    return spark


def silver_month_path(year_value, month_value):
    return (
        f"{SILVER_PATH}"
        f"pickup_year={year_value}/pickup_month={month_value}/"
    )


def run_quality_checks_for_month(spark, year_value, month_value):
    path = silver_month_path(year_value, month_value)

    print("=" * 80)
    print(f"Running quality checks for {year_value}-{month_value:02d}")
    print("Input:", path)

    df = spark.read.parquet(path)

    total_rows = df.count()

    checks = []

    def add_check(rule_name, failed_count, description):
        status = "PASS" if failed_count == 0 else "FAIL"

        checks.append({
            "rule_name": rule_name,
            "status": status,
            "failed_count": int(failed_count),
            "description": description
        })

    row_count_failed = 0 if total_rows > 0 else 1

    add_check(
        "row_count_positive",
        row_count_failed,
        "Monthly Silver partition should contain at least one row."
    )

    required_columns = [
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "pickup_location_id",
        "dropoff_location_id"
    ]

    null_counts_row = df.select([
        count(when(col(column_name).isNull(), column_name)).alias(column_name)
        for column_name in required_columns
    ]).collect()[0].asDict()

    for column_name, null_count in null_counts_row.items():
        add_check(
            f"{column_name}_not_null",
            null_count,
            f"{column_name} should not contain null values."
        )

    add_check(
        "trip_distance_positive",
        df.filter(col("trip_distance") <= 0).count(),
        "Trip distance should be greater than 0."
    )

    add_check(
        "fare_amount_non_negative",
        df.filter(col("fare_amount") < 0).count(),
        "Fare amount should be greater than or equal to 0."
    )

    add_check(
        "total_amount_non_negative",
        df.filter(col("total_amount") < 0).count(),
        "Total amount should be greater than or equal to 0."
    )

    add_check(
        "duration_positive",
        df.filter(col("trip_duration_minutes") <= 0).count(),
        "Trip duration should be greater than 0 minutes."
    )

    add_check(
        "duration_reasonable",
        df.filter(col("trip_duration_minutes") > 24 * 60).count(),
        "Trip duration should be less than or equal to 24 hours."
    )

    add_check(
        "no_future_pickup_dates",
        df.filter(col("pickup_date") > current_date()).count(),
        "Pickup date should not be in the future."
    )

    wrong_partition_rows = df.filter(
        (year(col("pickup_datetime")) != year_value) |
        (month(col("pickup_datetime")) != month_value)
    ).count()

    add_check(
        "expected_year_month",
        wrong_partition_rows,
        "Rows should belong to the expected pickup year and pickup month based on pickup_datetime."
    )

    failed_checks = [check for check in checks if check["status"] == "FAIL"]
    month_status = "PASS" if len(failed_checks) == 0 else "FAIL"

    result = {
        "pipeline": "nyc_taxi_batch_pipeline",
        "layer": "silver",
        "validation_type": "pyspark_quality_checks",
        "year": year_value,
        "month": month_value,
        "row_count": int(total_rows),
        "status": month_status,
        "total_checks": len(checks),
        "passed_checks": len(checks) - len(failed_checks),
        "failed_checks": len(failed_checks),
        "checks": checks,
        "validated_at_utc": datetime.now(timezone.utc).isoformat()
    }

    print("Rows:", total_rows)
    print("Status:", month_status)
    print("Failed checks:", len(failed_checks))

    return result


def write_validation_report_to_s3(quality_results, runtime_failures):
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    passed_months = [result for result in quality_results if result["status"] == "PASS"]
    failed_months = [result for result in quality_results if result["status"] == "FAIL"]

    overall_status = (
        "PASS"
        if len(failed_months) == 0 and len(runtime_failures) == 0
        else "FAIL"
    )

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    validation_report = {
        "pipeline": "nyc_taxi_batch_pipeline",
        "layer": "silver",
        "validation_tooling": "PySpark quality checks; Great Expectations-style rules",
        "source": SILVER_PATH,
        "overall_status": overall_status,
        "months_expected": 36,
        "months_validated": len(quality_results),
        "months_passed": len(passed_months),
        "months_failed": len(failed_months),
        "runtime_failures": runtime_failures,
        "total_rows_validated": int(sum(result["row_count"] for result in quality_results)),
        "results": quality_results,
        "created_at_utc": datetime.now(timezone.utc).isoformat()
    }

    report_key = (
        f"{QUALITY_RESULTS_PREFIX}"
        f"silver_validation_report_{run_timestamp}.json"
    )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=report_key,
        Body=json.dumps(validation_report, indent=2)
    )

    print("Validation report written to:")
    print(f"s3://{BUCKET_NAME}/{report_key}")
    print("Overall status:", overall_status)

    if overall_status != "PASS":
        raise Exception("Data quality validation failed.")

    return validation_report


def write_quality_summary_table(spark, quality_results):
    quality_summary_rows = []

    for result in quality_results:
        quality_summary_rows.append({
            "year": result["year"],
            "month": result["month"],
            "row_count": result["row_count"],
            "status": result["status"],
            "total_checks": result["total_checks"],
            "passed_checks": result["passed_checks"],
            "failed_checks": result["failed_checks"]
        })

    quality_summary_df = spark.createDataFrame(quality_summary_rows)

    quality_summary_path = (
        f"s3a://{BUCKET_NAME}/quality/validation_results/silver_summary_table/"
    )

    (
        quality_summary_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(quality_summary_path)
    )

    print("Quality summary table written to:", quality_summary_path)


def run_data_quality_checks():
    spark = create_spark_session()

    quality_results = []
    runtime_failures = []

    for year_value in YEARS_TO_PROCESS:
        for month_value in MONTHS_TO_PROCESS:
            try:
                result = run_quality_checks_for_month(
                    spark,
                    year_value,
                    month_value
                )
                quality_results.append(result)

            except Exception as error:
                print(f"Runtime failure for {year_value}-{month_value:02d}")
                print("Error:", error)

                runtime_failures.append({
                    "year": year_value,
                    "month": month_value,
                    "error": str(error)
                })

    print("Quality checks complete.")
    print("Months checked:", len(quality_results))
    print("Runtime failures:", len(runtime_failures))

    validation_report = write_validation_report_to_s3(
        quality_results,
        runtime_failures
    )

    write_quality_summary_table(spark, quality_results)

    return validation_report


if __name__ == "__main__":
    run_data_quality_checks()
