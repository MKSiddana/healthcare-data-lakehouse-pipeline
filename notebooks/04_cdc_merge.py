from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("HealthcareClaimsCDC")
    .getOrCreate()
)

# Define schema for incremental claims
claims_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("provider_id", StringType(), False),
    StructField("service_date", StringType(), True),
    StructField("procedure_code", StringType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("claim_status", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

# Read incremental update file
updates_path = "data/sample_claims_updates.csv"

updates_df = (
    spark.read
    .option("header", "true")
    .schema(claims_schema)
    .csv(updates_path)
)

print("Incremental records received:", updates_df.count())

# Target Delta table created by transformation pipeline
silver_path = "data/silver/claims"

silver_table = DeltaTable.forPath(
    spark,
    silver_path
)

# CDC / UPSERT
(
    silver_table.alias("target")
    .merge(
        updates_df.alias("source"),
        "target.claim_id = source.claim_id"
    )
    .whenMatchedUpdateAll(
        condition="source.last_updated > target.last_updated"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("CDC merge completed successfully.")

# Validate results
result_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

print("Total records after CDC:", result_df.count())

result_df.orderBy("claim_id").show(
    truncate=False
)
