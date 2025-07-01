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

# --- Updated OP to handle both sensor-triggered and manual runs ---
def process_new_files(context, files=None):
    """
    Process new files - can be called directly or from an op.
    """
    # Get resources
    if hasattr(context, 'resources'):
        # Called from an op
        minio_client = context.resources.minio.get_client()
        conn = context.resources.postgres.get_conn()
        
        # Get files from op config if available
        if files is None and hasattr(context, 'op_config'):
            files = context.op_config.get("files", [])
    else:
        # Called directly - create resources
        minio_resource = MinioResource(
            endpoint_url="http://minio:9000",
            access_key="minioadmin", 
            secret_key="minioadmin"
        )
        postgres_resource = PostgresResource(
            host="postgres",
            dbname="fix_db",
            user="admin",
            password="admin"
        )
        minio_client = minio_resource.get_client()
        conn = postgres_resource.get_conn()
    
    cur = conn.cursor()
    parser = FixToParquetConverter()

    bucket_source = "textfixlogs"
    bucket_target = "parquetfixlogs"
    process_date = date.today()
    execution_time = datetime.now()

    # If no files specified, get all unprocessed files
    if not files:
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
        
        # List all files in bucket
        try:
            paginator = minio_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket_source)
            all_files = []
            for page in pages:
                for obj in page.get("Contents", []):
                    all_files.append(obj["Key"])
            
            files = [f for f in all_files if f.endswith(".txt") and f not in already_processed]
        except Exception as e:
            if hasattr(context, 'log'):
                context.log.error(f"Error listing files: {e}")
            cur.close()
            conn.close()
            return

    # Process each file
    for filename in files:
        if hasattr(context, 'log'):
            context.log.info(f"Starting to process: {filename}")

        try:
            # Download FIX log from MinIO
            obj = minio_client.get_object(Bucket=bucket_source, Key=filename)
            fix_text = obj["Body"].read().decode("utf-8")
            df = parser.parse_fix_lines(fix_text)
            nb_lines = len(df)
            
            if hasattr(context, 'log'):
                context.log.info(f"Parsed {nb_lines} lines from {filename}")

            # Convert to Parquet
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)

            # Upload Parquet
            parquet_key = filename.replace(".txt", ".parquet")
            if hasattr(context, 'log'):
                context.log.info(f"Uploading {parquet_key} to {bucket_target}")
            
            minio_client.put_object(
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

            if hasattr(context, 'log'):
                context.log.info(f"✅ Processed and recorded: {filename}")
                
        except Exception as e:
            if hasattr(context, 'log'):
                context.log.error(f"Error processing {filename}: {e}")
            else:
                print(f"Error processing {filename}: {e}")

    cur.close()
    conn.close()