from dagster import Definitions, job, op, sensor, RunRequest, SkipReason
from dagster_dbt import DbtCliResource, dbt_assets
from fix_sensor import process_new_files
from datetime import datetime
import os
from pathlib import Path
import subprocess
import boto3
import psycopg2

# Get the current directory and dbt project paths
current_dir = Path(__file__).parent
dbt_project_dir = current_dir / "pictet_fix_project"
dbt_profiles_dir = dbt_project_dir / ".dbt"
manifest_path = dbt_project_dir / "target" / "manifest.json"

# Define the dbt CLI resource
dbt_resource = DbtCliResource(
    project_dir=os.fspath(dbt_project_dir),
    profiles_dir=os.fspath(dbt_profiles_dir),
    target="container",
)

# Define the `dbt compile` operation
@op
def dbt_compile_op(context):
    if manifest_path.exists():
        context.log.info(f"Manifest file found at {manifest_path}. Skipping `dbt compile`.")
    else:
        context.log.info(f"Manifest file not found at {manifest_path}. Running `dbt compile`...")
        try:
            subprocess.run(
                [
                    "dbt",
                    "compile",
                    "--profiles-dir",
                    os.fspath(dbt_profiles_dir),
                    "--target",
                    "container",
                ],
                cwd=os.fspath(dbt_project_dir),
                check=True,
            )
            context.log.info("`dbt compile` completed successfully.")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to run `dbt compile`: {e}")

# Define dbt assets
@dbt_assets(manifest=os.fspath(manifest_path))
def pictet_fix_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# Define the custom processing operation
@op
def process_fix_files_op(context):
    files_to_process = context.run_config.get("ops", {}).get("process_fix_files_op", {}).get("config", {}).get("files", [])
    if not isinstance(files_to_process, list):
        raise ValueError("Invalid configuration: 'files' must be a list.")
    process_new_files(context, files=files_to_process)
    context.log.info("File processing completed.")

# Define the Dagster job
@job(resource_defs={"dbt": dbt_resource})
def fix_ingest_and_dbt_job():
    dbt_compile_op()
    process_fix_files_op()
    pictet_fix_dbt_assets()

# Define the sensor
@sensor(
    job=fix_ingest_and_dbt_job,
    minimum_interval_seconds=30,
)
def new_fix_file_sensor(context):
    bucket = "textfixlogs"
    try:
        # Hardcoded MinIO and PostgreSQL parameters
        minio_client = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        conn = psycopg2.connect(
            host="postgres",
            dbname="fix_db",
            user="admin",
            password="admin",
        )
        cur = conn.cursor()

        # Create table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_name TEXT PRIMARY KEY,
                file_source TEXT,
                process_date DATE,
                execution_time TIMESTAMP,
                nb_lines INT
            );
        """)
        conn.commit()

        # Get already processed files
        cur.execute("SELECT file_name FROM processed_files;")
        already_processed = {row[0] for row in cur.fetchall()}

        # List all files in the bucket
        paginator = minio_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket)
        all_files = []
        for page in pages:
            for obj in page.get("Contents", []):
                all_files.append(obj["Key"])

        new_files = [f for f in all_files if f.endswith(".txt") and f not in already_processed]

        cur.close()
        conn.close()

        if not new_files:
            return SkipReason("No new files to process.")

        return RunRequest(
            run_key=f"run-{datetime.utcnow().isoformat()}",
            run_config={
                "ops": {
                    "process_fix_files_op": {
                        "config": {"files": new_files}
                    }
                }
            },
        )
    except Exception as e:
        context.log.error(f"Error in sensor: {e}")
        return SkipReason(f"Error in sensor: {e}")

# Define Dagster Definitions
defs = Definitions(
    assets=[pictet_fix_dbt_assets],
    jobs=[fix_ingest_and_dbt_job],
    sensors=[new_fix_file_sensor],
    resources={
        "dbt": dbt_resource,  # Add the dbt resource here
    },
)