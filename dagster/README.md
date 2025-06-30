# FIX Trading Pipeline Project

This project implements a FIX (Financial Information Exchange) trading pipeline using **Dagster** for orchestration, **dbt** for data transformations, **DuckDB** for local and containerized data storage, and **MinIO** for S3-compatible object storage. The pipeline processes FIX log files, converts them to Parquet format, and performs data transformations to generate insights and reports.

---

## Project Overview

### Key Components
1. **Dagster**:
   - Orchestrates the pipeline, including sensors, jobs, and resources.
   - Monitors new FIX log files in MinIO and triggers processing and dbt transformations.

2. **dbt**:
   - Performs SQL-based data transformations on the processed FIX data.
   - Generates staging, fact, and dimension tables for reporting.

3. **DuckDB**:
   - Lightweight database for local and containerized data storage.
   - Stores raw and transformed FIX data.

4. **MinIO**:
   - S3-compatible object storage for managing FIX log files and Parquet outputs.

5. **PostgreSQL**:
   - Tracks processed files and logs metadata for the pipeline.

---

## Pipeline Workflow

1. **FIX Log Ingestion**:
   - A Dagster sensor monitors the `textfixlogs` bucket in MinIO for new FIX log files.
   - New files are parsed, converted to Parquet, and uploaded to the `parquetfixlogs` bucket.

2. **Data Transformation**:
   - dbt transforms the Parquet data into staging, fact, and dimension tables.
   - Aggregated KPIs (e.g., by symbol, system, and day) are generated for reporting.

3. **Reporting**:
   - The transformed data is available for analysis and visualization in tools like Superset.

---

## Project Structure

```plaintext
FIX_trading/
├── dagster/
│   ├── dagster.yaml                # Dagster instance configuration
│   ├── workspace.yaml              # Dagster workspace configuration
│   ├── docker-compose.yml          # Docker Compose file for the entire stack
│   ├── [README.md](http://_vscodecontentref_/1)                   # Project documentation
│   ├── deployments/
│   │   ├── fix_pipeline/
│   │   │   ├── fix_sensor.py       # Dagster sensor for FIX log ingestion
│   │   │   ├── fix_sensor_new.py   # Alternate sensor implementation
│   │   │   ├── dbt_assets.py       # dbt asset loader for Dagster
│   │   │   ├── Dockerfile_user_code # Dockerfile for Dagster user code
│   │   │   ├── pictet_fix_project/ # dbt project directory
│   │   │   │   ├── dbt_project.yml # dbt project configuration
│   │   │   │   ├── models/         # dbt models for transformations
│   │   │   │   ├── .dbt/           # dbt profiles for local and containerized runs
├── duck_db/                        # Directory for DuckDB files
│   ├── duckdb_pictet               # DuckDB database file
├── quickfix/
│   ├── text_to_parquet.py          # Standalone script for FIX log processing

dbt run --profiles-dir ~/.dbt --target dev -s <model_name>

dagster dev -f dagster/deployments/fix_pipeline/fix_sensor.py

docker-compose up --build

Access Services:

- Dagster UI: http://localhost:3000
- MinIO Console: http://localhost:9001
- Superset: http://localhost:8088

