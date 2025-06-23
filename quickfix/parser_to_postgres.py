import os
import re
import psycopg2
from datetime import datetime
import json
import boto3
import pandas as pd  # Import pandas for Parquet conversion

# PostgreSQL connection params
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB = os.getenv("PG_DB", "fix_db")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "admin")

# MinIO connection params
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "parquetfixlogs")

FIX_LOG_FILE = "fix_logs.txt"

# Match FIX lines and extract timestamp, source, and raw FIX payload
LINE_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[(?P<system>[^\]]+)\] .* : (?P<fix>8=.*)"
)

def parse_fix_message(fix_string: str):
    return {tag: value for tag, value in (field.split("=", 1) for field in fix_string.strip().split("|") if "=" in field)}

def ensure_table_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fix_messages (
            id SERIAL PRIMARY KEY,
            log_time TIMESTAMP,
            system TEXT,
            msg_type TEXT,
            raw_fix TEXT,
            fix_tags JSONB
        );
    """)

def insert_fix_message(cursor, log_time, system, msg_type, raw_fix, fix_tags):
    cursor.execute("""
        INSERT INTO fix_messages (log_time, system, msg_type, raw_fix, fix_tags)
        VALUES (%s, %s, %s, %s, %s);
    """, (log_time, system, msg_type, raw_fix, json.dumps(fix_tags)))  # Convert fix_tags to JSON string

def upload_to_minio(minio_client, bucket_name, object_name, file_path):
    try:
        # Ensure the bucket exists
        try:
            minio_client.head_bucket(Bucket=bucket_name)
        except minio_client.exceptions.ClientError:
            # If the bucket does not exist, create it
            minio_client.create_bucket(Bucket=bucket_name)

        # Upload the file
        with open(file_path, "rb") as f:
            minio_client.put_object(
                Bucket=bucket_name,
                Key=object_name,
                Body=f,
                ContentType="application/octet-stream"
            )
        print(f"Uploaded {object_name} to bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")



def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cursor = conn.cursor()
    ensure_table_exists(cursor)

    # Connect to MinIO
    minio_client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # Process the FIX log file
    parsed_data = []
    with open(FIX_LOG_FILE, "r") as f:
        for line in f:
            match = LINE_PATTERN.match(line)
            if not match:
                continue

            ts = datetime.strptime(match.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f")
            system = match.group("system")
            fix_raw = match.group("fix")
            fix_tags = parse_fix_message(fix_raw)
            msg_type = fix_tags.get("35", "UNKNOWN")

            # Insert into PostgreSQL
            insert_fix_message(cursor, ts, system, msg_type, fix_raw, fix_tags)

            # Add to parsed data for Parquet conversion
            parsed_data.append({
                "timestamp": ts.isoformat(),
                "system": system,
                "msg_type": msg_type,
                "raw_fix": fix_raw,
                "fix_tags": json.dumps(fix_tags)  # Store as JSON string
            })

    # Convert parsed data to a DataFrame
    df = pd.DataFrame(parsed_data)

    # Save DataFrame to a Parquet file
    parquet_file = "fix_logs.parquet"
    df.to_parquet(parquet_file, engine="pyarrow", index=False)

    # Upload the Parquet file to MinIO
    upload_to_minio(minio_client, MINIO_BUCKET, "fix_logs.parquet", parquet_file)

    # Commit and close PostgreSQL connection
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()