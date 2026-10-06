# Healthcare Data Lakehouse Pipeline

An end-to-end healthcare data engineering project demonstrating scalable ingestion, data quality validation, PySpark transformations, incremental CDC processing, Delta Lake architecture, and analytics-ready data preparation.

## Architecture

```text
Healthcare Claims CSV
        |
        v
   Data Ingestion
        |
        v
   Bronze Layer
        |
        v
 Data Quality Checks
        |
        v
 PySpark Transformations
        |
        v
   Silver Layer
        |
        +------ Incremental Claims
        |             |
        |             v
        |        CDC / Delta MERGE
        |             |
        +-------------+
        |
        v
   Gold Layer
        |
        v
 Analytics / Reporting
```

## Technologies

- Python
- PySpark
- Spark SQL
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Delta Lake
- Snowflake
- SQL

## Project Features

- Batch healthcare claims ingestion
- Explicit schema enforcement
- Bronze, Silver, and Gold lakehouse architecture
- Data cleansing and standardization
- Data quality and schema validation
- Duplicate detection
- Incremental data processing
- CDC using Delta Lake MERGE
- Insert and update handling
- Production-style validation and error handling
- Analytics-ready claim aggregations

## Repository Structure

```text
healthcare-data-lakehouse-pipeline/
│
├── data/
│   ├── sample_claims.csv
│   └── sample_claims_updates.csv
│
├── notebooks/
│   ├── 01_ingestion.py
│   ├── 02_data_validation.py
│   ├── 03_transformations.py
│   ├── 04_cdc_merge.py
│   └── 05_analytics.py
│
├── .gitignore
└── README.md
```

## Pipeline Flow

### 1. Data Ingestion
Reads healthcare claims data using PySpark with an explicitly defined schema and loads the source data into the Bronze layer.

### 2. Data Quality Validation
Performs validation for required fields, invalid claim amounts, claim statuses, and duplicate claim IDs.

### 3. Data Transformation
Cleans and standardizes claims data, removes duplicates, validates business fields, and writes curated records to the Silver layer.

### 4. Incremental CDC Processing
Processes changed and newly arrived claims using Delta Lake MERGE.

- Existing claim + newer timestamp → UPDATE
- New claim → INSERT

This avoids unnecessary full-dataset reprocessing.

### 5. Gold Analytics Layer
Aggregates curated claims by status and calculates:

- Total claims
- Total claim amount
- Average claim amount

The resulting Gold dataset is prepared for downstream analytics and reporting.

## Example CDC Scenario

```text
CLM10002  Existing claim  -> UPDATE
CLM10005  Existing claim  -> UPDATE
CLM10011  New claim       -> INSERT
CLM10012  New claim       -> INSERT
```

## Data Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

- ETL/ELT pipeline development
- Medallion architecture
- PySpark transformations
- Data quality controls
- Delta Lake
- CDC and incremental processing
- SQL and analytical data preparation
- Data lakehouse design
- Production pipeline reliability
