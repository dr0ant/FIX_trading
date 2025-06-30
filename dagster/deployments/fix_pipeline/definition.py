from dagster import Definitions, job, op, sensor, RunRequest, SkipReason
from fix_sensor import process_new_files, MinioResource, PostgresResource
import subprocess
import sys
import os
from datetime import datetime

@op
def run_dbt_script(_):
    base_dir = os.path.dirname(__file__)
    script_path = os.path.join(base_dir, "dbt_run.py")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"dbt_run.py failed:\n{result.stdout}\n{result.stderr}")

@job
def fix_ingest_and_dbt_job():
    process_new_files()
    run_dbt_script()

@sensor(
    job=fix_ingest_and_dbt_job,
    minimum_interval_seconds=30,
    required_resource_keys={"minio", "postgres"},
)
def new_fix_file_sensor(context):
    bucket = "textfixlogs"
    context.log.info("🔍 Scanning MinIO bucket for new FIX files")
    minio_client = context.resources.minio.get_client()
    conn = context.resources.postgres.get_conn()
    cur = conn.cursor()
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
    cur.execute("SELECT file_name FROM processed_files;")
    already_processed = {row[0] for row in cur.fetchall()}

    paginator = minio_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket)
    all_files = []
    for page in pages:
        for obj in page.get("Contents", []):
            all_files.append(obj["Key"])

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
                "process_new_files": {
                    "config": {"files": new_files}
                }
            }
        },
    )

defs = Definitions(
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
    },
)