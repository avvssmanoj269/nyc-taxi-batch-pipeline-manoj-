from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    year,
    month,
    dayofmonth,
    round,
    unix_timestamp,
    current_timestamp,
    broadcast,
    lit
)
from pyspark.sql.types import IntegerType, DoubleType


BUCKET_NAME = "nyc-taxi-batch-pipeline-manoj"
AWS_REGION = "eu-west-1"

BRONZE_PATH = f"s3a://{BUCKET_NAME}/bronze/nyc_taxi/yellow_trips/"
SILVER_PATH = f"s3a://{BUCKET_NAME}/silver/nyc_taxi/yellow_trips_clean/"
LOOKUP_PATH = f"s3a://{BUCKET_NAME}/lookup/taxi_zone_lookup/taxi_zone_lookup.csv"

YEARS_TO_PROCESS = [2023, 2024, 2025]
MONTHS_TO_PROCESS = list(range(1, 13))


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("nyc_taxi_silver_transformation")
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


def prepare_lookup_tables(spark):
    lookup_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(LOOKUP_PATH)
    )

    pickup_lookup_df = (
        lookup_df
        .select(
            col("LocationID").alias("pickup_location_id"),
            col("Borough").alias("pickup_borough"),
            col("Zone").alias("pickup_zone"),
            col("service_zone").alias("pickup_service_zone")
        )
    )

    dropoff_lookup_df = (
        lookup_df
        .select(
            col("LocationID").alias("dropoff_location_id"),
            col("Borough").alias("dropoff_borough"),
            col("Zone").alias("dropoff_zone"),
            col("service_zone").alias("dropoff_service_zone")
        )
    )

    return pickup_lookup_df, dropoff_lookup_df


def transform_month(spark, pickup_lookup_df, dropoff_lookup_df, year_value, month_value):
    input_path = (
        f"{BRONZE_PATH}"
        f"pickup_year={year_value}/pickup_month={month_value}/"
    )

    output_path = (
        f"{SILVER_PATH}"
        f"pickup_year={year_value}/pickup_month={month_value}/"
    )

    print("=" * 80)
    print(f"Transforming Silver for {year_value}-{month_value:02d}")
    print("Input:", input_path)
    print("Output:", output_path)

    bronze_month_df = spark.read.parquet(input_path)

    if "airport_fee" not in bronze_month_df.columns:
        bronze_month_df = bronze_month_df.withColumn(
            "airport_fee",
            lit(None).cast(DoubleType())
        )

    if "congestion_surcharge" not in bronze_month_df.columns:
        bronze_month_df = bronze_month_df.withColumn(
            "congestion_surcharge",
            lit(None).cast(DoubleType())
        )

    raw_count = bronze_month_df.count()

    standardized_df = bronze_month_df.select(
        col("VendorID").cast(IntegerType()).alias("vendor_id"),
        to_timestamp(col("tpep_pickup_datetime")).alias("pickup_datetime"),
        to_timestamp(col("tpep_dropoff_datetime")).alias("dropoff_datetime"),
        col("passenger_count").cast(DoubleType()).alias("passenger_count"),
        col("trip_distance").cast(DoubleType()).alias("trip_distance"),
        col("RatecodeID").cast(IntegerType()).alias("rate_code_id"),
        col("store_and_fwd_flag").alias("store_and_forward_flag"),
        col("PULocationID").cast(IntegerType()).alias("pickup_location_id"),
        col("DOLocationID").cast(IntegerType()).alias("dropoff_location_id"),
        col("payment_type").cast(IntegerType()).alias("payment_type"),
        col("fare_amount").cast(DoubleType()).alias("fare_amount"),
        col("extra").cast(DoubleType()).alias("extra"),
        col("mta_tax").cast(DoubleType()).alias("mta_tax"),
        col("tip_amount").cast(DoubleType()).alias("tip_amount"),
        col("tolls_amount").cast(DoubleType()).alias("tolls_amount"),
        col("improvement_surcharge").cast(DoubleType()).alias("improvement_surcharge"),
        col("total_amount").cast(DoubleType()).alias("total_amount"),
        col("congestion_surcharge").cast(DoubleType()).alias("congestion_surcharge"),
        col("airport_fee").cast(DoubleType()).alias("airport_fee"),
        col("ingestion_timestamp"),
        col("source_file"),
        col("batch_id")
    )

    enriched_df = (
        standardized_df
        .withColumn("pickup_date", to_date(col("pickup_datetime")))
        .withColumn("dropoff_date", to_date(col("dropoff_datetime")))
        .withColumn("pickup_year", year(col("pickup_datetime")))
        .withColumn("pickup_month", month(col("pickup_datetime")))
        .withColumn("pickup_day", dayofmonth(col("pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            round(
                (
                    unix_timestamp(col("dropoff_datetime"))
                    - unix_timestamp(col("pickup_datetime"))
                ) / 60,
                2
            )
        )
        .withColumn("silver_processed_timestamp", current_timestamp())
    )

    cleaned_df = (
        enriched_df
        .filter(col("pickup_datetime").isNotNull())
        .filter(col("dropoff_datetime").isNotNull())
        .filter(col("pickup_year") == year_value)
        .filter(col("pickup_month") == month_value)
        .filter(col("trip_distance") > 0)
        .filter(col("fare_amount") >= 0)
        .filter(col("total_amount") >= 0)
        .filter(col("trip_duration_minutes") > 0)
        .filter(col("trip_duration_minutes") <= 24 * 60)
        .filter(col("pickup_location_id").isNotNull())
        .filter(col("dropoff_location_id").isNotNull())
    )

    deduped_df = cleaned_df.dropDuplicates([
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "fare_amount",
        "total_amount"
    ])

    enriched_with_zones_df = (
        deduped_df
        .join(broadcast(pickup_lookup_df), on="pickup_location_id", how="left")
        .join(broadcast(dropoff_lookup_df), on="dropoff_location_id", how="left")
    )

    silver_final_df = enriched_with_zones_df.select(
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "dropoff_date",
        "pickup_year",
        "pickup_month",
        "pickup_day",
        "passenger_count",
        "trip_distance",
        "trip_duration_minutes",
        "rate_code_id",
        "store_and_forward_flag",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        "dropoff_location_id",
        "dropoff_borough",
        "dropoff_zone",
        "dropoff_service_zone",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "total_amount",
        "ingestion_timestamp",
        "source_file",
        "batch_id",
        "silver_processed_timestamp"
    )

    silver_count = silver_final_df.count()
    rejected_count = raw_count - silver_count

    (
        silver_final_df
        .drop("pickup_year", "pickup_month")
        .coalesce(4)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Silver transformation completed for {year_value}-{month_value:02d}")
    print("Bronze rows:", raw_count)
    print("Silver rows:", silver_count)
    print("Rejected rows:", rejected_count)

    return {
        "year": year_value,
        "month": month_value,
        "bronze_rows": raw_count,
        "silver_rows": silver_count,
        "rejected_rows": rejected_count,
        "output_path": output_path
    }


def run_silver_transformation():
    spark = create_spark_session()

    pickup_lookup_df, dropoff_lookup_df = prepare_lookup_tables(spark)

    successful_months = []
    failed_months = []

    for year_value in YEARS_TO_PROCESS:
        for month_value in MONTHS_TO_PROCESS:
            try:
                result = transform_month(
                    spark,
                    pickup_lookup_df,
                    dropoff_lookup_df,
                    year_value,
                    month_value
                )
                successful_months.append(result)

            except Exception as error:
                print(f"Silver transformation failed for {year_value}-{month_value:02d}")
                print("Error:", error)

                failed_months.append({
                    "year": year_value,
                    "month": month_value,
                    "error": str(error)
                })

    print("Silver transformation complete.")
    print("Successful months:", len(successful_months))
    print("Failed months:", len(failed_months))

    if failed_months:
        raise Exception(f"Silver transformation failed for some months: {failed_months}")

    return {
        "successful_months": successful_months,
        "failed_months": failed_months
    }


if __name__ == "__main__":
    run_silver_transformation()
