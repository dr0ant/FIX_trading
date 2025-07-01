from dagster import Definitions, job, op, sensor, RunRequest, SkipReason, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from fix_sensor import process_new_files, MinioResource, PostgresResource
from datetime import datetime
import os
from pathlib import Path

# Get the current directory and dbt project paths
current_dir = Path(__file__).parent
dbt_project_dir = current_dir / "pictet_fix_project"
dbt_profiles_dir = dbt_project_dir / ".dbt"

# Define the dbt CLI resource
dbt_resource = DbtCliResource(
    project_dir=os.fspath(dbt_project_dir),
    profiles_dir=os.fspath(dbt_profiles_dir),
    target="container",  # Use container target for Docker deployment
)

# Define dbt assets using the new approach
@dbt_assets(manifest=dbt_project_dir / "target" / "manifest.json")
def pictet_fix_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt assets for the FIX trading project.
    """
    yield from dbt.cli(["build"], context=context).stream()

# Define the custom processing operation
@op(required_resource_keys={"minio", "postgres"})
def process_fix_files_op(context):
    """
    Process new FIX files from MinIO and convert to Parquet.
    """
    # Get files from run config if available, otherwise process all new files
    files_to_process = context.run_config.get("ops", {}).get("process_new_files", {}).get("config", {}).get("files", [])
    
    if files_to_process:
        # Process specific files passed from sensor
        return process_new_files(context, files_to_process)
    else:
        # Process all new files (fallback)
        return process_new_files(context)

# Define the Dagster job
@job(resource_defs={"minio": MinioResource, "postgres": PostgresResource, "dbt": dbt_resource})
def fix_ingest_and_dbt_job():
    """
    Dagster job that processes new FIX files and runs dbt transformations.
    """
    process_fix_files_op()

# Define the sensor
@sensor(
    job=fix_ingest_and_dbt_job,
    minimum_interval_seconds=30,
)
def new_fix_file_sensor(context):
    """
    Sensor that monitors the MinIO bucket for new FIX files and triggers the job.
    """
    # Initialize resources manually since sensor doesn't have direct access
    minio_resource = MinioResource(
        endpoint_url="http://minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    postgres_resource = PostgresResource(
        host="postgres",
        dbname="fix_db",
        user="admin",
        password="admin",
    )
    
    bucket = "textfixlogs"
    context.log.info("🔍 Scanning MinIO bucket for new FIX files")
    
    minio_client = minio_resource.get_client()
    conn = postgres_resource.get_conn()
    cur = conn.cursor()
    
    # Create processed_files table if it doesn't exist
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
    
    # Get list of already processed files
    cur.execute("SELECT file_name FROM processed_files;")
    already_processed = {row[0] for row in cur.fetchall()}

    # List all files in the bucket
    try:
        paginator = minio_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket)
        all_files = []
        for page in pages:
            for obj in page.get("Contents", []):
                all_files.append(obj["Key"])
    except Exception as e:
        context.log.error(f"Error accessing MinIO bucket: {e}")
        cur.close()
        conn.close()
        return SkipReason(f"Error accessing MinIO bucket: {e}")

    # Find new files
    new_files = [f for f in all_files if f.endswith(".txt") and f not in already_processed]
    context.log.info(f"🆕 Found {len(new_files)} new file(s): {new_files}")

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

# Define Dagster Definitions
defs = Definitions(
    assets=[pictet_fix_dbt_assets],
    jobs=[fix_ingest_and_dbt_job],
    sensors=[new_fix_file_sensor],
    resources={
        "minio": MinioResource(
            endpoint_url="http://minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
        ),
        "postgres": PostgresResource(
            host="postgres",
            dbname="fix_db",
            user="admin",
            password="admin",
        ),
        "dbt": dbt_resource,
    },
)