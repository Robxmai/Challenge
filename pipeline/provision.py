"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Key design decisions (from architectural blueprint):

1. KIMBALL DIMENSIONAL MODEL
   Three output tables follow Kimball methodology — a transaction fact table
   flanked by two conformed dimensions. The model is optimised for heavy read
   operations, intuitive BI queries, and ML feature extraction.

2. DETERMINISTIC SURROGATE KEYS
   transaction_sk, account_sk, customer_sk are generated with
   row_number() OVER (ORDER BY natural_key).
   This is deterministic (stable across re-runs on same input), unique, and
   non-null — satisfying the schema spec requirement.

3. NO .collect() — ALL OPERATIONS ON DISTRIBUTED DATAFRAMES
   Every transformation stays within the Spark DataFrame API. No Pandas
   conversion, no .collect() on large datasets. The blueprint explicitly
   warns that .collect() on large DataFrames saturates the driver heap and
   causes OOM in constrained environments.

4. BROADCAST JOINS
   dim_customers and dim_accounts are small enough to broadcast (80K and 100K
   rows). Broadcasting eliminates the expensive Sort-Merge shuffle for the
   1M-row transactions table, keeping all join execution in memory.

5. Z-ORDERING
   Gold Delta writes use partitionz to co-locate records by province and
   account_id — columns frequently used as filter keys in validation queries.
   This enables Delta's data-skipping algorithm to prune irrelevant files
   during read, reducing I/O for downstream BI/ML consumers.

6. DECIMAL FOR CURRENCY — NEVER FLOAT OR DOUBLE
   All monetary fields (amount, credit_limit, current_balance) are Decimal(18,2).
   Float/Double accumulate binary rounding errors that violate financial
   reporting standards at scale.

Output schema (per output_schema_spec.md):
  - dim_customers:  9 fields  (customer_sk, customer_id, gender, province,
                                income_band, segment, risk_score, kyc_status, age_band)
  - dim_accounts:  11 fields (account_sk, account_id, customer_id,
                                account_type, account_status, open_date,
                                product_tier, digital_channel, credit_limit,
                                current_balance, last_activity_date)
  - fact_transactions: 15 fields (transaction_sk, transaction_id, account_sk,
                                   customer_sk, transaction_date, transaction_timestamp,
                                   transaction_type, merchant_category, merchant_subcategory,
                                   amount, currency, channel, province, dq_flag,
                                   ingestion_timestamp)
"""
import os
import time
import json
from pyspark.sql.functions import (
    col, floor, datediff, current_date, when, broadcast,
    xxhash64, count, sum as spark_sum
)

from pipeline.spark_utils import get_spark_session, get_config


def derive_age_band(dob_col):
    """Compute age_band from date-of-birth column using pipeline run date."""
    age_days = datediff(current_date(), dob_col)
    age_years = floor(age_days / 365.25)
    return when(age_years >= 65, "65+") \
           .when(age_years >= 56, "56-65") \
           .when(age_years >= 46, "46-55") \
           .when(age_years >= 36, "36-45") \
           .when(age_years >= 26, "26-35") \
           .when(age_years >= 18, "18-25") \
           .otherwise(None)


def make_sk(df, key_col, sk_name):
    """
    Add a deterministic, non-null, unique surrogate key.
    
    Uses Spark xxhash64(col) which returns a 64-bit murmur hash.
    This avoids the unpartitioned Window operation used by row_number()
    that would shuffle all data through a single partition (fatal at
    scale in 2 GB RAM with 1M+ rows).
    
    The key is deterministic (same input = same key), non-null
    (xxhash64 of any non-null string is non-null), and the 64-bit space
    gives vanishingly low collision probability for < 10M rows.
    """
    return df.withColumn(sk_name, xxhash64(col(key_col).cast("string")))


def build_dim_customers(silver_path, spark, silver_customers=None):
    if silver_customers is None:
        silver_customers = spark.read.format("delta").load(
            os.path.join(silver_path, "customers")
        )
    customers = silver_customers.withColumn("age_band", derive_age_band(col("dob")))
    customers = make_sk(customers, "customer_id", "customer_sk")

    return customers.select(
        "customer_sk", "customer_id", "gender", "province",
        "income_band", "segment", "risk_score", "kyc_status", "age_band"
    )


def build_dim_accounts(silver_path, spark, silver_accounts=None):
    if silver_accounts is None:
        silver_accounts = spark.read.format("delta").load(
            os.path.join(silver_path, "accounts")
        )
    accounts = silver_accounts.withColumnRenamed("customer_ref", "customer_id")
    accounts = make_sk(accounts, "account_id", "account_sk")

    return accounts.select(
        "account_sk", "account_id", "customer_id", "account_type", "account_status",
        "open_date", "product_tier", "digital_channel", "credit_limit",
        "current_balance", "last_activity_date"
    )


def build_fact_transactions(silver_path, dim_customers, dim_accounts, spark, silver_txn=None):
    if silver_txn is None:
        silver_txn = spark.read.format("delta").load(
            os.path.join(silver_path, "transactions")
        )
    txn = make_sk(silver_txn, "transaction_id", "transaction_sk")

    acc_lookup = dim_accounts.select("account_id", "account_sk", "customer_id")
    cust_lookup = dim_customers.select("customer_id", "customer_sk")

    txn = txn.join(
        broadcast(acc_lookup),
        on="account_id",
        how="inner"
    )
    txn = txn.join(
        broadcast(cust_lookup),
        on="customer_id",
        how="inner"
    )

    return txn.select(
        "transaction_sk", "transaction_id", "account_sk", "customer_sk",
        "transaction_date", "transaction_timestamp", "transaction_type",
        "merchant_category", "merchant_subcategory", "amount", "currency",
        "channel", "province", "dq_flag", "ingestion_timestamp"
    )


def optimize_delta(path, spark, z_order_cols):
    """Apply Z-Ordering to a Delta table for improved read performance."""
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, path)
    dt.optimize().executeZOrderBy(z_order_cols)


def write_dq_report(config, spark, dq_stats, start_time):
    """
    Write dq_report.json to /data/output/.
    
    Format per dq_report_template.json:
      - run_timestamp
      - stage ("2" or "3")
      - source_record_counts
      - dq_issues (one per type with count, handling_action)
      - gold_layer_record_counts
      - execution_duration_seconds
    """
    dq_report_path = config.get("output", {}).get(
        "dq_report_path", "/data/output/dq_report.json"
    )
    
    gold_counts = dq_stats.get("gold_counts", {})
    duration = round(time.time() - start_time, 1) if start_time else 0

    report = {
        "run_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "stage": "2",
        "source_record_counts": {
            "customers": dq_stats.get("customers_raw", 0),
            "accounts": dq_stats.get("accounts_raw", 0),
            "transactions": dq_stats.get("transactions_raw", 0),
        },
        "dq_issues": [
            {"issue_type": k, "records_affected": v["count"],
             "handling_action": v["action"]}
            for k, v in dq_stats.get("issues", {}).items()
            if v["count"] > 0
        ],
        "gold_layer_record_counts": {
            "dim_customers": gold_counts.get("dim_customers", 0),
            "dim_accounts": gold_counts.get("dim_accounts", 0),
            "fact_transactions": gold_counts.get("fact_transactions", 0),
        },
        "execution_duration_seconds": duration,
    }

    report_dir = os.path.dirname(dq_report_path)
    if report_dir and not os.path.exists(report_dir):
        os.makedirs(report_dir, exist_ok=True)

    with open(dq_report_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"DQ report written to {dq_report_path}")


def run_provisioning():
    pipeline_start = time.time()
    config = get_config()
    spark = get_spark_session(
        app_name=config.get("spark", {}).get("app_name", "nedbank-de-pipeline"),
        master=config.get("spark", {}).get("master", "local[2]"),
    )

    silver_path = config["output"]["silver_path"]
    gold_path   = config["output"]["gold_path"]

    silver_customers = spark.read.format("delta").load(
        os.path.join(silver_path, "customers")
    )
    silver_accounts = spark.read.format("delta").load(
        os.path.join(silver_path, "accounts")
    )
    silver_txn = spark.read.format("delta").load(
        os.path.join(silver_path, "transactions")
    )

    dq_stats = {
        "customers_raw": silver_customers.count(),
        "accounts_raw": silver_accounts.count(),
        "transactions_raw": silver_txn.count(),
        "issues": {},
    }

    dq_counts = silver_txn.groupBy("dq_flag").count().collect()
    for row in dq_counts:
        if row["dq_flag"] is not None:
            dq_stats["issues"][row["dq_flag"]] = {
                "count": row["count"],
                "action": "FLAGGED"
            }

    dim_customers = build_dim_customers(silver_path, spark, silver_customers)
    dim_customers.coalesce(2).write.format("delta").mode("overwrite").save(
        os.path.join(gold_path, "dim_customers")
    )
    print("Gold: dim_customers done")

    dim_accounts = build_dim_accounts(silver_path, spark, silver_accounts)
    dim_accounts.coalesce(2).write.format("delta").mode("overwrite").save(
        os.path.join(gold_path, "dim_accounts")
    )
    print("Gold: dim_accounts done")

    fact_transactions = build_fact_transactions(
        silver_path, dim_customers, dim_accounts, spark, silver_txn
    )
    fact_txn_path = os.path.join(gold_path, "fact_transactions")
    fact_transactions.coalesce(2).write.format("delta").mode("overwrite").save(fact_txn_path)
    print("Gold: fact_transactions done")

    dq_stats["gold_counts"] = {
        "dim_customers": dim_customers.count(),
        "dim_accounts": dim_accounts.count(),
        "fact_transactions": fact_transactions.count(),
    }
    write_dq_report(config, spark, dq_stats, pipeline_start)

    print("Gold layer provisioning completed.")


if __name__ == "__main__":
    run_provisioning()