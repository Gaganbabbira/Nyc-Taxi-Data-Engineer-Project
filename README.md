# NYC Taxi Data Engineering Project (Databricks)

## Overview
This project is an end-to-end data engineering pipeline built using Databricks and the Medallion Architecture (Bronze, Silver, Gold layers). It processes NYC taxi trip data from raw ingestion to business-level analytics.

The pipeline demonstrates real-world data engineering concepts like ETL, data cleaning, transformation, and orchestration using Databricks Jobs.

---

## Tech Stack
- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Medallion Architecture

---

## Architecture

### Bronze Layer
- Raw NYC taxi data ingestion
- No transformations applied
- Data stored in Delta format

### Silver Layer
- Data cleaning and validation
- Removed nulls and duplicates
- Standardized column formats (datetime, IDs)

### Gold Layer
- Aggregated business insights
- Trip analysis (revenue, distance, demand patterns)
- Final analytics-ready tables

---

## Pipeline Workflow
Bronze → Silver → Gold

All layers are connected using a structured ETL pipeline in Databricks.

---

## Job Orchestration
A Databricks Job Pipeline is created to automate the workflow:
1. Bronze ingestion notebook
2. Silver transformation notebook
3. Gold analytics notebook

This ensures scheduled and automated execution of the full pipeline.

---

## Key Features
- End-to-end ETL pipeline using PySpark
- Medallion Architecture implementation
- Delta Lake for optimized storage
- Data cleaning and transformation logic
- Automated Databricks Job scheduling
- Scalable data processing workflow

---

## How to Run
1. Upload dataset into Databricks
2. Run Bronze ingestion notebook
3. Run Silver transformation notebook
4. Run Gold analytics notebook
5. Or execute full pipeline using Databricks Job

---

## Author
Gagan

---

## Note
This project is built for learning and demonstrating real-world data engineering skills using Databricks.
