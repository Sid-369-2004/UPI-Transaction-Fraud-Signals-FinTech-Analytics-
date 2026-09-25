"""
===============================================================================
CAPSTONE PROJECT: TOPIC 06 - UPI TRANSACTION FRAUD SIGNALS
DATABRICKS MEDALLION PIPELINE (PYSPARK IMPLEMENTATION)
===============================================================================
Student Name: Siddharth Sonkar
Roll Number:  23051628
Catalog / Schema / Volume Setup:
  MY_ID = "23051628_SiddharthSonkar"
  VOL = f"/Volumes/workspace/capstone_{MY_ID}/raw"

Architecture:
  Bronze Layer: Raw ingested data with metadata (_source_file, _ingested_at, _row_hash)
  Silver Layer: Cleaned, deduplicated, validated txns & account master + silver_rejects
  Gold Layer: 
    1. GOLD_ACCOUNT_DAY: Account daily metrics, trailing 30d baseline, spikes & device bursts
    2. GOLD_MERCHANT_HOUR: Hourly merchant metrics, total vs failed transactions, failure rate
===============================================================================
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def run_medallion_pipeline(spark: SparkSession, volume_path: str):
    print("==========================================================")
    print("STARTING DATABRICKS MEDALLION PIPELINE FOR UPI FRAUD")
    print("==========================================================")

    # -------------------------------------------------------------------------
    # 1. BRONZE LAYER - LAND IT, CHANGE NOTHING
    # -------------------------------------------------------------------------
    print("\n--- [1/3] BRONZE LAYER INGESTION ---")
    
    # Ingest Accounts
    bronze_accounts = (
        spark.read.option("header", True)
        .csv(f"{volume_path}/accounts.csv")
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_row_hash", F.sha2(F.concat_ws("||", "account_id", "bank", "home_city", "primary_device", "secondary_device"), 256))
    )
    bronze_accounts.write.mode("overwrite").saveAsTable("bronze_accounts")
    bronze_accounts_count = bronze_accounts.count()
    print(f"Bronze Accounts Count: {bronze_accounts_count:,} rows")

    # Ingest Transactions
    bronze_txns = (
        spark.read.option("header", True)
        .csv(f"{volume_path}/txns.csv")
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_row_hash", F.sha2(F.concat_ws("||", "txn_id", "account_id", "merchant_id", "merchant_category", "txn_ts", "device_id", "status", "amount"), 256))
    )
    bronze_txns.write.mode("overwrite").saveAsTable("bronze_txns")
    bronze_txns_count = bronze_txns.count()
    print(f"Bronze Transactions Count: {bronze_txns_count:,} rows")

    # -------------------------------------------------------------------------
    # 2. SILVER LAYER - CLEANING, DEDUPLICATION & REJECTS ROUTING
    # -------------------------------------------------------------------------
    print("\n--- [2/3] SILVER LAYER TRANSFORMATIONS & REJECTS ---")

    # Step A: Deduplication on txn_id
    deduped_txns = bronze_txns.dropDuplicates(["txn_id"])
    deduped_count = deduped_txns.count()
    duplicate_count = bronze_txns_count - deduped_count
    print(f"Deduplicated Txns: {deduped_count:,} rows (Removed {duplicate_count:,} duplicates)")

    # Step B: Data Type Casting (try_cast)
    parsed_txns = deduped_txns.withColumn("amount_cast", F.expr("try_cast(amount as decimal(18,2))")) \
                              .withColumn("txn_ts_cast", F.expr("try_cast(txn_ts as timestamp)"))

    # Step C: Rejects Routing
    # Reject 1: Unparseable Amount (NA)
    rejects_unparseable = parsed_txns.filter(F.col("amount_cast").isNull()) \
        .select("txn_id", "account_id", "merchant_id", "txn_ts", F.lit("unparseable_amount").alias("reject_reason"), F.current_timestamp().alias("_rejected_at"))

    # Valid amount filter
    valid_amount_txns = parsed_txns.filter(F.col("amount_cast").isNotNull())

    # Reject 2: Unknown Account ('ACC99999999')
    rejects_unknown_account = valid_amount_txns.filter(F.col("account_id") == "ACC99999999") \
        .select("txn_id", "account_id", "merchant_id", "txn_ts", F.lit("unknown_account").alias("reject_reason"), F.current_timestamp().alias("_rejected_at"))

    valid_account_txns = valid_amount_txns.filter(F.col("account_id") != "ACC99999999")

    # Reject 3: Timestamp Out of Range (outside 2025-05-01 to 2025-06-14, e.g. 1970 timestamps)
    rejects_out_of_range = valid_account_txns.filter(
        (F.col("txn_ts_cast") < F.to_timestamp(F.lit("2025-05-01 00:00:00"))) | 
        (F.col("txn_ts_cast") > F.to_timestamp(F.lit("2025-06-14 23:59:59")))
    ).select("txn_id", "account_id", "merchant_id", "txn_ts", F.lit("timestamp_out_of_range").alias("reject_reason"), F.current_timestamp().alias("_rejected_at"))

    # Consolidate Rejects
    silver_rejects = rejects_unparseable.unionByName(rejects_unknown_account).unionByName(rejects_out_of_range)
    silver_rejects.write.mode("overwrite").saveAsTable("silver_rejects")
    silver_rejects_count = silver_rejects.count()

    # Clean Silver Transactions Dataset
    silver_txns = valid_account_txns.filter(
        (F.col("txn_ts_cast") >= F.to_timestamp(F.lit("2025-05-01 00:00:00"))) & 
        (F.col("txn_ts_cast") <= F.to_timestamp(F.lit("2025-06-14 23:59:59")))
    ).withColumn("amount", F.col("amount_cast")) \
     .withColumn("txn_ts", F.col("txn_ts_cast")) \
     .withColumn("account_day", F.to_date(F.col("txn_ts"))) \
     .drop("amount_cast", "txn_ts_cast")

    silver_txns.write.mode("overwrite").saveAsTable("silver_txns")
    silver_txns_count = silver_txns.count()

    print(f"Silver Rejects Count: {silver_rejects_count:,} rows")
    print(f"Silver Transactions Clean Count: {silver_txns_count:,} rows")
    print(f"Reconciliation Check: {bronze_txns_count:,} - {duplicate_count:,} (dupes) - {silver_rejects_count:,} (rejects) = {silver_txns_count:,} (clean)")

    # -------------------------------------------------------------------------
    # 3. GOLD LAYER - ANALYTICAL SHAPES
    # -------------------------------------------------------------------------
    print("\n--- [3/3] GOLD LAYER METRICS & FEATURE ENGINEERING ---")

    # Table 1: GOLD_ACCOUNT_DAY
    # Aggregate daily metrics per account
    daily_account_agg = silver_txns.groupBy("account_id", "account_day").agg(
        F.count("txn_id").alias("txns"),
        F.max("amount").alias("max_amount"),
        F.countDistinct("device_id").alias("distinct_devices")
    )

    # Window spec for 30-day trailing baseline: ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    # Crucial engineering rule: EXCLUDE current row to avoid baseline inflation trap
    w_30d = Window.partitionBy("account_id").orderBy("account_day").rowsBetween(-30, -1)

    gold_account_day = daily_account_agg \
        .withColumn("avg_amount_30d_trailing", F.avg("max_amount").over(w_30d)) \
        .withColumn("prior_txn_count", F.count("max_amount").over(w_30d)) \
        .withColumn("is_amount_spike", F.when(
            (F.col("prior_txn_count") >= 5) & (F.col("max_amount") >= 5 * F.col("avg_amount_30d_trailing")), True
        ).otherwise(False)) \
        .withColumn("is_device_burst", F.when(F.col("distinct_devices") >= 3, True).otherwise(False))

    gold_account_day.write.mode("overwrite").saveAsTable("gold_account_day")
    gold_account_day_count = gold_account_day.count()
    device_burst_count = gold_account_day.filter("is_device_burst = true").count()
    amount_spike_count = gold_account_day.filter("is_amount_spike = true").count()

    print(f"GOLD_ACCOUNT_DAY Count: {gold_account_day_count:,} account-days")
    print(f"Device Bursts Detected (>= 3 devices/day): {device_burst_count} account-days")
    print(f"Amount Spikes Flagged (>= 5x 30d baseline & >=5 prior txns): {amount_spike_count} account-days")

    # Table 2: GOLD_MERCHANT_HOUR
    silver_txns_with_hour = silver_txns.withColumn("hour_of_day", F.hour("txn_ts"))
    
    gold_merchant_hour = silver_txns_with_hour.groupBy("merchant_id", "hour_of_day").agg(
        F.count("txn_id").alias("txns"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_txns")
    ).withColumn("failure_rate", F.round(F.col("failed_txns") / F.col("txns"), 4))

    gold_merchant_hour.write.mode("overwrite").saveAsTable("gold_merchant_hour")
    gold_merchant_hour_count = gold_merchant_hour.count()
    print(f"GOLD_MERCHANT_HOUR Count: {gold_merchant_hour_count:,} merchant-hours (Expected 1,200 merchants x 24 hours = 28,800)")

    print("\n==========================================================")
    print("DATABRICKS MEDALLION PIPELINE EXECUTED SUCCESSFULLY")
    print("==========================================================")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("UPI_Fraud_Signals_Pipeline").getOrCreate()
    run_medallion_pipeline(spark, "/Volumes/workspace/capstone_student/raw")
