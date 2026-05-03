"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Raw files are written as-is (unmodified) with an ingestion_timestamp column added.
Partitioned by source (one Delta table per source).

Optimization: Explicit schema for JSONL read to avoid Spark's schema inference
on 3M rows (Stage 2). Schema inference samples the file multiple times, adding
minutes to the pipeline.
"""
import os
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, BooleanType
)
from pyspark.sql.functions import current_timestamp

from pipeline.spark_utils import get_spark_session, get_config


TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("transaction_time", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("merchant_subcategory", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("location", StructType([
        StructField("province", StringType(), True),
        StructField("city", StringType(), True),
        StructField("coordinates", StringType(), True),
    ]), True),
    StructField("metadata", StructType([
        StructField("device_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("retry_flag", BooleanType(), True),
    ]), True),
])


def run_ingestion():
    config = get_config()
    spark = get_spark_session(
        app_name=config.get("spark", {}).get("app_name", "nedbank-de-pipeline"),
        master=config.get("spark", {}).get("master", "local[2]"),
    )

    input_paths = config["input"]
    output_paths = config["output"]
    bronze_path = output_paths["bronze_path"]

    ingestion_ts = current_timestamp()

    accounts_df = spark.read.option("header", "true").csv(
        input_paths["accounts_path"]
    )
    accounts_df = accounts_df.withColumn("ingestion_timestamp", ingestion_ts)
    accounts_df.coalesce(2).write.format("delta").mode("overwrite").save(
        os.path.join(bronze_path, "accounts")
    )
    print("Bronze: accounts done")

    cust_df = spark.read.option("header", "true").csv(
        input_paths["customers_path"]
    )
    cust_df = cust_df.withColumn("ingestion_timestamp", ingestion_ts)
    cust_df.coalesce(2).write.format("delta").mode("overwrite").save(
        os.path.join(bronze_path, "customers")
    )
    print("Bronze: customers done")

    txn_df = spark.read.schema(TRANSACTIONS_SCHEMA).json(
        input_paths["transactions_path"]
    )
    txn_df = txn_df.withColumn("ingestion_timestamp", ingestion_ts)
    txn_df.coalesce(2).write.format("delta").mode("overwrite").save(
        os.path.join(bronze_path, "transactions")
    )
    print("Bronze: transactions done")

    print("Bronze layer ingestion completed.")


if __name__ == "__main__":
    run_ingestion()
