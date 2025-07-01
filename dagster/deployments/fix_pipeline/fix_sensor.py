import io
import boto3
import psycopg2
import pandas as pd
from datetime import datetime, date


# --- FIX Converter Class ---
class FixToParquetConverter:
    """
    A utility class to parse FIX log lines and convert them to a DataFrame.
    """
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


# --- File Processing Function ---
def process_new_files(context, files=None):
    """
    Process new FIX files from MinIO and convert them to Parquet.
    """
    # Hardcoded MinIO and PostgreSQL parameters
    minio_client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    postgres_conn = psycopg2.connect(
        host="postgres",
        dbname="fix_db",
        user="admin",
        password="admin",
    )
    cur = postgres_conn.cursor()
    parser = FixToParquetConverter()

    bucket_source = "textfixlogs"
    bucket_target = "parquetfixlogs"
    process_date = date.today()
    execution_time = datetime.now()

    # If no files specified, fetch unprocessed files
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
        postgres_conn.commit()

        # Get already processed files
        cur.execute("SELECT file_name FROM processed_files;")
        already_processed = {row[0] for row in cur.fetchall()}

        # List all files in the bucket
        paginator = minio_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_source)
        all_files = []
        for page in pages:
            for obj in page.get("Contents", []):
                all_files.append(obj["Key"])

        files = [f for f in all_files if f.endswith(".txt") and f not in already_processed]

    # Process each file
    for filename in files:
        context.log.info(f"Processing file: {filename}")
        try:
            # Download FIX log from MinIO
            obj = minio_client.get_object(Bucket=bucket_source, Key=filename)
            fix_text = obj["Body"].read().decode("utf-8")
            df = parser.parse_fix_lines(fix_text)
            nb_lines = len(df)

            # Ensure DataFrame is not empty
            if df.empty:
                context.log.warning(f"File {filename} contains no valid FIX messages.")
                continue

            # Convert to Parquet
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)

            # Upload Parquet to MinIO
            parquet_key = filename.replace(".txt", ".parquet")
            minio_client.put_object(
                Bucket=bucket_target,
                Key=parquet_key,
                Body=parquet_buffer.getvalue(),
            )

            # Log to PostgreSQL
            cur.execute("""
                INSERT INTO processed_files (file_name, file_source, process_date, execution_time, nb_lines)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (file_name) DO NOTHING
            """, (filename, bucket_source, process_date, execution_time, nb_lines))
            postgres_conn.commit()

            context.log.info(f"✅ Successfully processed: {filename}")
        except Exception as e:
            context.log.error(f"Error processing {filename}: {e}")

    cur.close()
    postgres_conn.close()