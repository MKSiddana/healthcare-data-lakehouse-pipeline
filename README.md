# Healthcare Data Lakehouse Pipeline

An end-to-end healthcare data engineering project demonstrating scalable
data ingestion, transformation, validation, incremental processing, and
analytics using Azure and Databricks.

## Architecture

Sample Healthcare Data
        |
        v
Azure Data Factory
        |
        v
ADLS Gen2 - Raw Layer
        |
        v
Databricks / PySpark
        |
        +-- Data Cleansing
        +-- Schema Validation
        +-- Duplicate Handling
        +-- Data Quality Checks
        +-- CDC / Incremental Processing
        |
        v
Delta Lake - Curated Layer
        |
        v
Snowflake
        |
        v
Analytics / Reporting

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

## Key Features

- Batch data ingestion
- PySpark-based data transformations
- Incremental and CDC processing
- Delta Lake MERGE operations
- Schema validation
- Duplicate and null-value checks
- Data reconciliation
- Error handling
- Pipeline monitoring and logging
- Analytics-ready curated datasets

## Project Structure

healthcare-data-lakehouse-pipeline/
- data/ - Sample healthcare datasets
- notebooks/ - PySpark processing notebooks
- sql/ - SQL scripts and analytical queries
- config/ - Pipeline configuration
- tests/ - Data quality and pipeline tests
- docs/ - Architecture documentation

## Data Privacy

This project uses synthetic sample healthcare data created solely for
demonstration purposes. It does not contain real patient information,
proprietary company data, or production source code.

## Purpose

This project demonstrates practical data engineering concepts including
cloud data ingestion, distributed data processing, ETL/ELT development,
incremental data processing, data quality, and data warehouse integration.
