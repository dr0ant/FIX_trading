import io
import os
import subprocess
import pandas as pd
from datetime import datetime, date
import boto3
import psycopg2

from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    job,
    op,
    ConfigurableResource,
    Definitions,
)

# --- FIX Converter Class ---
class FixToParquetConverter:
    def parse_fix_lines(self, fix_text):
        records = []
        for line in fix_text.splitlines():
            if '8=FIX' not in line:
                continue
            fix_message = line.split('8=FIX', 1)[-1]
            fix_message = '8=FIX' + fix_message
            fix_message = fix_message.strip().split('|')
            parsed = {kv.split('=')[0]: kv.split('=')[1] for kv in fix_message if '=' in kv}
            records.append(parsed)
        return pd.DataFrame(records)


# --- Resources ---
class MinioResource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str

    def get_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )


class PostgresResource(ConfigurableResource):
    host: str
    dbname: str
    user: str
    password: str

    def get_conn(self):
        return psycopg2.connect(
            host=self.host, dbname=self.dbname, user=self.user, password=self.password
        )


# --- OP ---
@op(required_resource_keys={"minio", "postgres"}, config_schema={"files": list})
def process_new_files(context):
    minio = context.resources.minio.get_client()
    conn = context.resources.postgres.get_conn()
    cur = conn.cursor()
    parser = FixToParquetConverter()

    bucket_source = "textfixlogs"
    bucket_target = "parquetfixlogs"
    process_date = date.today()
    execution_time = datetime.now()

    for filename in context.op_config["files"]:
        context.log.info(f"Starting to process: {filename}")

        # Download FIX log from MinIO
        obj = minio.get_object(Bucket=bucket_source, Key=filename)
        fix_text = obj["Body"].read().decode("utf-8")
        df = parser.parse_fix_lines(fix_text)
        nb_lines = len(df)
        context.log.info(f"Parsed {nb_lines} lines from {filename}")

        # Convert to Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        # Upload Parquet
        parquet_key = filename.replace(".txt", ".parquet")
        context.log.info(f"Uploading {parquet_key} to {bucket_target}")
        minio.put_object(
            Bucket=bucket_target,
            Key=parquet_key,
            Body=parquet_buffer.getvalue(),
        )

        # Log to Postgres
        cur.execute("""
            INSERT INTO processed_files (file_name, file_source, process_date, execution_time, nb_lines)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (file_name) DO NOTHING
        """, (filename, bucket_source, process_date, execution_time, nb_lines))
        conn.commit()

        context.log.info(f"✅ Processed and recorded: {filename}")

    # Optional: run dbt
    try:
        subprocess.run(["dbt", "run", "--select", "your_model"], check=True)
        context.log.info("✅ DBT model executed")
    except subprocess.CalledProcessError as e:
        context.log.error(f"❌ DBT run failed: {e}")
        raise

    cur.close()
    conn.close()


# --- JOB ---
@job
def fix_processing_job():
    process_new_files()


# --- SENSOR ---
@sensor(
    job=fix_processing_job,
    minimum_interval_seconds=60,
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


# --- DEFINITIONS ---
defs = Definitions(
    jobs=[fix_processing_job],
    sensors=[new_fix_file_sensor],
    resources={
        "minio": MinioResource(
            endpoint_url="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
        ),
        "postgres": PostgresResource(
            host="localhost",
            dbname="fix_db",
            user="admin",
            password="admin",
        ),
    },
)
