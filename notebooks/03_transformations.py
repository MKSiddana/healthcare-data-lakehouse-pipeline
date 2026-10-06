from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    to_date,
    current_timestamp,
    round
)

spark = (
    SparkSession.builder
    .appName("HealthcareClaimsTransformation")
    .getOrCreate()
)

# Read Bronze layer
bronze_path = "data/bronze/claims"

claims_df = (
    spark.read
    .format("delta")
    .load(bronze_path)
)

# Clean and transform claims
transformed_df = (
    claims_df
    .dropDuplicates(["claim_id"])
    .withColumn(
        "claim_status",
        upper(trim(col("claim_status")))
    )
    .withColumn(
        "service_date",
        to_date(col("service_date"), "yyyy-MM-dd")
    )
    .withColumn(
        "claim_amount",
        round(col("claim_amount"), 2)
    )
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )
)

# Keep valid records
valid_statuses = ["APPROVED", "PENDING", "DENIED"]

curated_df = transformed_df.filter(
    col("claim_id").isNotNull()
    & col("patient_id").isNotNull()
    & col("provider_id").isNotNull()
    & (col("claim_amount") > 0)
    & col("claim_status").isin(valid_statuses)
)

# Write Silver layer
silver_path = "data/silver/claims"

(
    curated_df.write
    .format("delta")
    .mode("overwrite")
    .save(silver_path)
)

print("Claims transformation completed successfully.")
print("Curated record count:", curated_df.count())

curated_df.show(truncate=False)
