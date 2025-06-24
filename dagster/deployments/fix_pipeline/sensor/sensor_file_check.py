import boto3
import psycopg2
import pandas as pd
import io
import subprocess
from datetime import datetime, date
from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    job,
    op,
    ConfigurableResource,
    Definitions,
)


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

    bucket_source = "textfixlogs"
    bucket_target = "parquetfixlogs"
    process_date = date.today()
    execution_time = datetime.now()

    for filename in context.op_config["files"]:
        context.log.info(f"Starting to process: {filename}")

        # Get file from MinIO
        context.log.info(f"Downloading {filename} from bucket {bucket_source}")
        obj = minio.get_object(Bucket=bucket_source, Key=filename)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        nb_lines = len(df)
        context.log.info(f"File {filename} has {nb_lines} lines")

        # Convert to Parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        # Upload Parquet to target bucket
        parquet_key = filename.replace(".csv", ".parquet")
        context.log.info(f"Uploading {parquet_key} to bucket {bucket_target}")
        minio.put_object(
            Bucket=bucket_target,
            Key=parquet_key,
            Body=parquet_buffer.getvalue(),
        )

        # Insert metadata into Postgres
        context.log.info(f"Inserting metadata for {filename} into Postgres")
        cur.execute("""
            INSERT INTO processed_files (file_name, file_source, process_date, execution_time, nb_lines)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (file_name) DO NOTHING
        """, (filename, bucket_source, process_date, execution_time, nb_lines))
        conn.commit()

        context.log.info(f"Finished processing: {filename}")

    # Run dbt model
    context.log.info("Running dbt model...")
    try:
        subprocess.run(["dbt", "run", "--select", "your_model"], check=True)
        context.log.info("DBT model executed successfully.")
    except subprocess.CalledProcessError as e:
        context.log.error(f"DBT execution failed: {e}")
        raise

    cur.close()
    conn.close()
    context.log.info("Closed Postgres connection.")


# --- JOB ---

@job
def my_processing_job():
    process_new_files()


# --- SENSOR ---

@sensor(
    job=my_processing_job,
    minimum_interval_seconds=60,
    required_resource_keys={"minio", "postgres"},
)
def new_minio_file_sensor(context):
    bucket = "textfixlogs"
    context.log.info("Starting sensor: checking for new files in bucket 'textfixlogs'")

    minio_client = context.resources.minio.get_client()
    conn = context.resources.postgres.get_conn()
    cursor = conn.cursor()

    context.log.info("Ensuring processed_files table exists")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_name TEXT PRIMARY KEY,
            file_source TEXT,
            process_date DATE,
            execution_time TIMESTAMP,
            nb_lines INT
        )
    """)
    conn.commit()

    context.log.info("Fetching list of already processed files")
    cursor.execute("SELECT file_name FROM processed_files")
    already_processed = {row[0] for row in cursor.fetchall()}

    context.log.info("Listing all files in MinIO bucket...")
    paginator = minio_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket)
    all_files = []
    for page in pages:
        for obj in page.get("Contents", []):
            all_files.append(obj["Key"])

    context.log.info(f"All files in bucket: {all_files}")

    new_files = [f for f in all_files if f not in already_processed]
    context.log.info(f"New unprocessed files: {new_files}")

    if not new_files:
        context.log.info("No new files found. Skipping run.")
        return SkipReason("No new files found.")

    return RunRequest(
        run_key=f"new-files-{context.cursor or 'start'}",
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
    jobs=[my_processing_job],
    sensors=[new_minio_file_sensor],
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
