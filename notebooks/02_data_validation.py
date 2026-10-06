from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("HealthcareClaimsValidation")
    .getOrCreate()
)

# Read Bronze layer
bronze_path = "data/bronze/claims"

claims_df = (
    spark.read
    .format("delta")
    .load(bronze_path)
)

# Required field validation
invalid_required = claims_df.filter(
    col("claim_id").isNull()
    | col("patient_id").isNull()
    | col("provider_id").isNull()
)

# Claim amount validation
invalid_amount = claims_df.filter(
    col("claim_amount").isNull()
    | (col("claim_amount") <= 0)
)

# Claim status validation
valid_statuses = ["APPROVED", "PENDING", "DENIED"]

invalid_status = claims_df.filter(
    ~col("claim_status").isin(valid_statuses)
)

# Duplicate validation
duplicate_claims = (
    claims_df
    .groupBy("claim_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

# Data quality results
print("Total records:", claims_df.count())
print("Missing required fields:", invalid_required.count())
print("Invalid claim amounts:", invalid_amount.count())
print("Invalid claim statuses:", invalid_status.count())
print("Duplicate claim IDs:", duplicate_claims.count())

if invalid_required.count() > 0:
    raise ValueError(
        "Data quality failure: required fields contain null values."
    )

if duplicate_claims.count() > 0:
    raise ValueError(
        "Data quality failure: duplicate claim IDs detected."
    )

print("Data quality validation completed successfully.")
