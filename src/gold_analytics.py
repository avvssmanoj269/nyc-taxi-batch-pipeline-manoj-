from datetime import datetime, timezone
import json

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    round,
    year,
    month,
    current_timestamp,
    lit
)


BUCKET_NAME = "nyc-taxi-batch-pipeline-manoj"
AWS_REGION = "eu-west-1"

SILVER_PATH = f"s3a://{BUCKET_NAME}/silver/nyc_taxi/yellow_trips_clean/"

GOLD_BASE_PATH = f"s3a://{BUCKET_NAME}/gold/nyc_taxi/"
DAILY_SUMMARY_PATH = f"{GOLD_BASE_PATH}daily_trip_summary/"
MONTHLY_SUMMARY_PATH = f"{GOLD_BASE_PATH}monthly_revenue_summary/"
ZONE_SUMMARY_PATH = f"{GOLD_BASE_PATH}zone_trip_summary/"

YEARS_TO_PROCESS = [2023, 2024, 2025]
MONTHS_TO_PROCESS = list(range(1, 13))


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("nyc_taxi_gold_analytics")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.endpoint.region", "eu-west-1")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )

    return spark


def silver_month_path(year_value, month_value):
    return (
        f"{SILVER_PATH}"
        f"pickup_year={year_value}/pickup_month={month_value}/"
    )


def create_daily_summary(spark):
    daily_results = []
    daily_failed = []

    for year_value in YEARS_TO_PROCESS:
        for month_value in MONTHS_TO_PROCESS:
            input_path = silver_month_path(year_value, month_value)

            output_path = (
                f"{DAILY_SUMMARY_PATH}"
                f"pickup_year={year_value}/pickup_month={month_value}/"
            )

            print("=" * 80)
            print(f"Creating daily summary for {year_value}-{month_value:02d}")
            print("Input:", input_path)
            print("Output:", output_path)

            try:
                df = spark.read.parquet(input_path)

                daily_df = (
                    df.groupBy("pickup_date")
                    .agg(
                        count("*").alias("total_trips"),
                        round(spark_sum("total_amount"), 2).alias("total_revenue"),
                        round(avg("total_amount"), 2).alias("avg_total_amount"),
                        round(avg("fare_amount"), 2).alias("avg_fare_amount"),
                        round(avg("trip_distance"), 2).alias("avg_trip_distance"),
                        round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
                        round(spark_sum("trip_distance"), 2).alias("total_trip_distance")
                    )
                    .withColumn("pickup_year", year(col("pickup_date")))
                    .withColumn("pickup_month", month(col("pickup_date")))
                    .withColumn("gold_processed_timestamp", current_timestamp())
                )

                row_count = daily_df.count()

                (
                    daily_df
                    .drop("pickup_year", "pickup_month")
                    .coalesce(1)
                    .write
                    .mode("overwrite")
                    .parquet(output_path)
                )

                daily_results.append({
                    "year": year_value,
                    "month": month_value,
                    "rows": row_count,
                    "output_path": output_path
                })

                print("Daily summary written.")
                print("Rows:", row_count)

            except Exception as error:
                print(f"Failed daily summary for {year_value}-{month_value:02d}")
                print("Error:", error)

                daily_failed.append({
                    "year": year_value,
                    "month": month_value,
                    "error": str(error)
                })

    return daily_results, daily_failed


def create_monthly_summary(spark):
    monthly_results = []
    monthly_failed = []

    for year_value in YEARS_TO_PROCESS:
        for month_value in MONTHS_TO_PROCESS:
            input_path = silver_month_path(year_value, month_value)

            output_path = (
                f"{MONTHLY_SUMMARY_PATH}"
                f"pickup_year={year_value}/pickup_month={month_value}/"
            )

            print("=" * 80)
            print(f"Creating monthly revenue summary for {year_value}-{month_value:02d}")
            print("Input:", input_path)
            print("Output:", output_path)

            try:
                df = spark.read.parquet(input_path)

                monthly_df = (
                    df.groupBy()
                    .agg(
                        count("*").alias("total_trips"),
                        round(spark_sum("total_amount"), 2).alias("total_revenue"),
                        round(avg("total_amount"), 2).alias("avg_total_amount"),
                        round(avg("fare_amount"), 2).alias("avg_fare_amount"),
                        round(avg("trip_distance"), 2).alias("avg_trip_distance"),
                        round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
                        round(spark_sum("trip_distance"), 2).alias("total_trip_distance")
                    )
                    .withColumn("pickup_year", lit(year_value))
                    .withColumn("pickup_month", lit(month_value))
                    .withColumn("gold_processed_timestamp", current_timestamp())
                )

                row_count = monthly_df.count()

                (
                    monthly_df
                    .drop("pickup_year", "pickup_month")
                    .coalesce(1)
                    .write
                    .mode("overwrite")
                    .parquet(output_path)
                )

                monthly_results.append({
                    "year": year_value,
                    "month": month_value,
                    "rows": row_count,
                    "output_path": output_path
                })

                print("Monthly summary written.")
                print("Rows:", row_count)

            except Exception as error:
                print(f"Failed monthly summary for {year_value}-{month_value:02d}")
                print("Error:", error)

                monthly_failed.append({
                    "year": year_value,
                    "month": month_value,
                    "error": str(error)
                })

    return monthly_results, monthly_failed


def create_zone_summary(spark):
    zone_results = []
    zone_failed = []

    for year_value in YEARS_TO_PROCESS:
        for month_value in MONTHS_TO_PROCESS:
            input_path = silver_month_path(year_value, month_value)

            output_path = (
                f"{ZONE_SUMMARY_PATH}"
                f"pickup_year={year_value}/pickup_month={month_value}/"
            )

            print("=" * 80)
            print(f"Creating zone summary for {year_value}-{month_value:02d}")
            print("Input:", input_path)
            print("Output:", output_path)

            try:
                df = spark.read.parquet(input_path)

                zone_df = (
                    df.groupBy(
                        "pickup_borough",
                        "pickup_zone",
                        "pickup_service_zone"
                    )
                    .agg(
                        count("*").alias("total_trips"),
                        round(spark_sum("total_amount"), 2).alias("total_revenue"),
                        round(avg("total_amount"), 2).alias("avg_total_amount"),
                        round(avg("fare_amount"), 2).alias("avg_fare_amount"),
                        round(avg("trip_distance"), 2).alias("avg_trip_distance"),
                        round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes")
                    )
                    .withColumn("pickup_year", lit(year_value))
                    .withColumn("pickup_month", lit(month_value))
                    .withColumn("gold_processed_timestamp", current_timestamp())
                )

                row_count = zone_df.count()

                (
                    zone_df
                    .drop("pickup_year", "pickup_month")
                    .coalesce(1)
                    .write
                    .mode("overwrite")
                    .parquet(output_path)
                )

                zone_results.append({
                    "year": year_value,
                    "month": month_value,
                    "rows": row_count,
                    "output_path": output_path
                })

                print("Zone summary written.")
                print("Rows:", row_count)

            except Exception as error:
                print(f"Failed zone summary for {year_value}-{month_value:02d}")
                print("Error:", error)

                zone_failed.append({
                    "year": year_value,
                    "month": month_value,
                    "error": str(error)
                })

    return zone_results, zone_failed


def write_gold_processing_log(daily_results, daily_failed, monthly_results, monthly_failed, zone_results, zone_failed):
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    gold_log = {
        "pipeline": "nyc_taxi_batch_pipeline",
        "layer": "gold",
        "source": SILVER_PATH,
        "targets": {
            "daily_trip_summary": DAILY_SUMMARY_PATH,
            "monthly_revenue_summary": MONTHLY_SUMMARY_PATH,
            "zone_trip_summary": ZONE_SUMMARY_PATH
        },
        "daily_summary": {
            "successful_months": len(daily_results),
            "failed_months": len(daily_failed),
            "failed": daily_failed
        },
        "monthly_summary": {
            "successful_months": len(monthly_results),
            "failed_months": len(monthly_failed),
            "failed": monthly_failed
        },
        "zone_summary": {
            "successful_months": len(zone_results),
            "failed_months": len(zone_failed),
            "failed": zone_failed
        },
        "created_at_utc": datetime.now(timezone.utc).isoformat()
    }

    log_key = f"logs/gold_analytics/gold_processing_summary_{run_timestamp}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=log_key,
        Body=json.dumps(gold_log, indent=2)
    )

    print(f"Gold processing log written to s3://{BUCKET_NAME}/{log_key}")

    return gold_log


def run_gold_analytics():
    spark = create_spark_session()

    daily_results, daily_failed = create_daily_summary(spark)
    monthly_results, monthly_failed = create_monthly_summary(spark)
    zone_results, zone_failed = create_zone_summary(spark)

    gold_log = write_gold_processing_log(
        daily_results,
        daily_failed,
        monthly_results,
        monthly_failed,
        zone_results,
        zone_failed
    )

    total_failed = len(daily_failed) + len(monthly_failed) + len(zone_failed)

    print("Gold analytics complete.")
    print("Daily successful months:", len(daily_results))
    print("Monthly successful months:", len(monthly_results))
    print("Zone successful months:", len(zone_results))
    print("Total failed tasks:", total_failed)

    if total_failed > 0:
        raise Exception("Gold analytics failed for one or more monthly outputs.")

    return gold_log


if __name__ == "__main__":
    run_gold_analytics()
