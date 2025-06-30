import io
import os
import subprocess
import pandas as pd
from datetime import datetime, date
import boto3
import psycopg2
from dagster import op, ConfigurableResource

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

    cur.close()
    conn.close()