from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round
)

spark = (
    SparkSession.builder
    .appName("HealthcareClaimsAnalytics")
    .getOrCreate()
)

# Read curated Silver claims
silver_path = "data/silver/claims"

claims_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

# Aggregate claims by status
claims_summary = (
    claims_df
    .groupBy("claim_status")
    .agg(
        count("claim_id").alias("total_claims"),
        round(sum("claim_amount"), 2).alias("total_claim_amount"),
        round(avg("claim_amount"), 2).alias("average_claim_amount")
    )
    .orderBy("claim_status")
)

claims_summary.show(truncate=False)

# Write analytics-ready Gold layer
gold_path = "data/gold/claims_summary"

(
    claims_summary.write
    .format("delta")
    .mode("overwrite")
    .save(gold_path)
)

print("Gold analytics layer created successfully.")
