from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

# Create Spark session
spark = (
    SparkSession.builder
    .appName("HealthcareClaimsIngestion")
    .getOrCreate()
)

# Define schema
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

# Read source data
input_path = "data/sample_claims.csv"

claims_df = (
    spark.read
    .option("header", "true")
    .schema(claims_schema)
    .csv(input_path)
)

# Basic validation
print(f"Source record count: {claims_df.count()}")
claims_df.printSchema()
claims_df.show(truncate=False)

# Write Bronze layer
bronze_path = "data/bronze/claims"

(
    claims_df.write
    .format("delta")
    .mode("overwrite")
    .save(bronze_path)
)

print("Healthcare claims successfully loaded to Bronze layer.")
