"""
Stage 3 — Streaming ingestion for micro-batch JSONL files.

Processes all pre-staged stream files from /data/stream/ in filename order,
writes upserted output to /data/output/stream_gold/.

Optimization: All stream files are read into one DataFrame and merged in a
single Delta merge operation (instead of 12 separate merges).
This reduces Delta version churn and wall-clock time.

Two output tables:
  - current_balances/   — one row per account_id (upsert via Delta merge)
  - recent_transactions/ — last 50 transactions per account_id
"""
import os
import time
from pyspark.sql.functions import (
    col, to_date, coalesce, from_unixtime, to_timestamp,
    concat_ws, lit, when, row_number,
    current_timestamp, sum as spark_sum, max as spark_max
)
from pyspark.sql.window import Window

from pipeline.spark_utils import get_spark_session, get_config


def parse_mixed_date(date_col):
    return coalesce(
        to_date(date_col, "yyyy-MM-dd"),
        to_date(date_col, "dd/MM/yyyy"),
        to_date(from_unixtime(date_col.cast("bigint")))
    )


def process_all_files(spark, config):
    stream_path = config.get("streaming", {}).get(
        "stream_input_path", "/data/stream"
    )
    if not os.path.isdir(stream_path):
        print("  Stream directory not found — skipping")
        return 0, 0

    stream_files = sorted([
        f for f in os.listdir(stream_path) if f.endswith(".jsonl")
    ])
    if not stream_files:
        return 0, 0

    silver_path = config["output"]["silver_path"]
    stream_gold_path = config.get("streaming", {}).get(
        "stream_gold_path", "/data/output/stream_gold"
    )
    current_balances_path = config.get("streaming", {}).get(
        "current_balances_path", os.path.join(stream_gold_path, "current_balances")
    )
    recent_txn_path = config.get("streaming", {}).get(
        "recent_transactions_path", os.path.join(stream_gold_path, "recent_transactions")
    )

    print(f"  Found {len(stream_files)} stream files to process")
    batch_ts = current_timestamp()

    accounts_df = spark.read.format("delta").load(
        os.path.join(silver_path, "accounts")
    ).select("account_id", "current_balance")

    all_dfs = []
    for filename in stream_files:
        filepath = os.path.join(stream_path, filename)
        df = spark.read.json(filepath)
        df = df.withColumn("_source_file", lit(filename))
        all_dfs.append(df)

    df = all_dfs[0]
    for extra_df in all_dfs[1:]:
        df = df.unionByName(extra_df, allowMissingColumns=True)

    total_events = df.count()

    df = df.withColumn("transaction_date", parse_mixed_date(col("transaction_date")))
    df = df.withColumn(
        "transaction_timestamp",
        to_timestamp(
            concat_ws(" ", col("transaction_date"), col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    for c in ["merchant_subcategory", "merchant_category", "channel"]:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast("string"))

    df = df.withColumn("amount", col("amount").cast("decimal(18,2)"))
    df = df.withColumn(
        "currency",
        when(
            col("currency").isin(["R", "rands", "710", "zar", "ZAR", "Rand"]),
            lit("ZAR")
        ).otherwise(lit("ZAR"))
    )
    if "location" in df.columns:
        df = df.withColumn("province", col("location.province"))
    df = df.withColumn("updated_at", batch_ts)

    balance_delta = when(col("transaction_type") == "CREDIT", col("amount")) \
                   .when(col("transaction_type") == "REVERSAL", col("amount")) \
                   .when(col("transaction_type") == "DEBIT", -col("amount")) \
                   .when(col("transaction_type") == "FEE", -col("amount")) \
                   .otherwise(lit(0))

    txn_with_delta = df.select(
        "account_id", "transaction_timestamp", "amount",
        "transaction_type", balance_delta.alias("balance_delta"), "updated_at"
    )

    batch_agg = txn_with_delta.groupBy("account_id").agg(
        spark_sum("balance_delta").alias("batch_delta"),
        spark_max("transaction_timestamp").alias("last_txn_ts"),
        spark_max("updated_at").alias("batch_updated_at")
    )

    from delta.tables import DeltaTable

    if os.path.isdir(current_balances_path) and os.path.exists(
        os.path.join(current_balances_path, "_delta_log")
    ):
        delta_tbl = DeltaTable.forPath(spark, current_balances_path)
        delta_tbl.alias("target").merge(
            batch_agg.alias("source"),
            "target.account_id = source.account_id"
        ).whenMatchedUpdate(set={
            "current_balance": col("target.current_balance") + col("source.batch_delta"),
            "last_transaction_timestamp": col("source.last_txn_ts"),
            "updated_at": col("source.batch_updated_at")
        }).whenNotMatchedInsert(values={
            "account_id": col("source.account_id"),
            "current_balance": col("source.batch_delta"),
            "last_transaction_timestamp": col("source.last_txn_ts"),
            "updated_at": col("source.batch_updated_at")
        }).execute()
    else:
        init_balances = accounts_df.join(batch_agg, "account_id", "left")
        init_balances = init_balances.withColumn(
            "current_balance",
            coalesce(col("current_balance"), lit(0)) + coalesce(col("batch_delta"), lit(0))
        ).withColumn(
            "last_transaction_timestamp",
            coalesce(col("last_txn_ts"), col("batch_updated_at"))
        ).withColumn(
            "updated_at", coalesce(col("batch_updated_at"), current_timestamp())
        ).select("account_id", "current_balance", "last_transaction_timestamp", "updated_at")
        init_balances.write.format("delta").mode("overwrite").save(current_balances_path)

    recent_cols = [
        "account_id", "transaction_id", "transaction_timestamp",
        "amount", "transaction_type", "channel", "updated_at"
    ]
    batch_recent = df.select(recent_cols)

    if os.path.isdir(recent_txn_path) and os.path.exists(
        os.path.join(recent_txn_path, "_delta_log")
    ):
        delta_tbl = DeltaTable.forPath(spark, recent_txn_path)
        delta_tbl.alias("target").merge(
            batch_recent.alias("source"),
            "target.account_id = source.account_id AND target.transaction_id = source.transaction_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        recent_df = spark.read.format("delta").load(recent_txn_path)
        w = Window.partitionBy("account_id").orderBy(col("transaction_timestamp").desc())
        ranked = recent_df.withColumn("_rn", row_number().over(w))
        to_keep = ranked.filter(col("_rn") <= 50).drop("_rn")
        to_keep.write.format("delta").mode("overwrite").save(recent_txn_path)
    else:
        batch_recent.write.format("delta").mode("overwrite").save(recent_txn_path)

    print(f"    -> {total_events} events processed across {len(stream_files)} files")
    return len(stream_files), total_events


def run_stream_ingestion():
    config = get_config()
    spark = get_spark_session(
        app_name="nedbank-de-stream",
        master=config.get("spark", {}).get("master", "local[2]"),
    )

    print("Stage 3 streaming ingestion starting...")
    file_count, event_count = process_all_files(spark, config)

    if file_count > 0:
        print(f"Stream ingestion complete: {file_count} files, {event_count} events")
    else:
        print("Stream ingestion: no files found")


if __name__ == "__main__":
    run_stream_ingestion()
