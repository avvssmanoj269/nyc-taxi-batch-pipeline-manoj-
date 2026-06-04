import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, year, month, col


BUCKET_NAME = "nyc-taxi-batch-pipeline-manoj"
AWS_REGION = "eu-west-1"

YEARS_TO_PROCESS = [2023, 2024, 2025]
MONTHS_TO_PROCESS = list(range(1, 13))


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("nyc_taxi_bronze_ingestion")
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


def run_bronze_ingestion():
    spark = create_spark_session()

    batch_id = str(uuid.uuid4())

    successful_months = []
    failed_months = []

    for year_value in YEARS_TO_PROCESS:
        for month_value in MONTHS_TO_PROCESS:
            input_path = (
                f"s3a://{BUCKET_NAME}/landing/nyc_taxi/yellow/"
                f"year={year_value}/month={month_value:02d}/"
                f"yellow_tripdata_{year_value}-{month_value:02d}.parquet"
            )

            output_path = (
                f"s3a://{BUCKET_NAME}/bronze/nyc_taxi/yellow_trips/"
                f"pickup_year={year_value}/pickup_month={month_value}/"
            )

            print("=" * 80)
            print(f"Processing Bronze ingestion for {year_value}-{month_value:02d}")
            print("Input:", input_path)
            print("Output:", output_path)

            try:
                monthly_df = spark.read.parquet(input_path)

                bronze_month_df = (
                    monthly_df
                    .withColumn("ingestion_timestamp", current_timestamp())
                    .withColumn("source_file", input_file_name())
                    .withColumn("batch_id", lit(batch_id))
                    .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
                    .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
                    .filter(
                        (col("pickup_year") == year_value) &
                        (col("pickup_month") == month_value)
                    )
                )

                row_count = bronze_month_df.count()

                if row_count == 0:
                    failed_months.append((year_value, month_value, "0 rows"))
                    continue

                (
                    bronze_month_df
                    .drop("pickup_year", "pickup_month")
                    .coalesce(4)
                    .write
                    .mode("overwrite")
                    .parquet(output_path)
                )

                successful_months.append((year_value, month_value, row_count))

                print(f"Bronze ingestion completed for {year_value}-{month_value:02d}")
                print("Rows:", row_count)

            except Exception as error:
                failed_months.append((year_value, month_value, str(error)))
                print(f"Bronze ingestion failed for {year_value}-{month_value:02d}")
                print("Error:", error)

    print("Bronze ingestion complete.")
    print("Successful months:", len(successful_months))
    print("Failed months:", len(failed_months))

    if failed_months:
        raise Exception(f"Bronze ingestion failed for some months: {failed_months}")

    return {
        "batch_id": batch_id,
        "successful_months": successful_months,
        "failed_months": failed_months
    }


if __name__ == "__main__":
    run_bronze_ingestion()
