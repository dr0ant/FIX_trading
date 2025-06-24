import psycopg2
from dagster import op, In, Out

@op(out={"new_files": Out(list)})
def get_new_files(context):
    import boto3
    import os

    minio = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',  # adjust
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=localhost")
    cur = conn.cursor()
    
    # Create tracking table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            id SERIAL PRIMARY KEY,
            file_name TEXT UNIQUE,
            file_source TEXT,
            process_date DATE,
            execution_time TIMESTAMP,
            nb_lines INT
        )
    """)
    conn.commit()

    # Get already processed files
    cur.execute("SELECT file_name FROM processed_files")
    processed = {row[0] for row in cur.fetchall()}

    # List files in MinIO
    response = minio.list_objects_v2(Bucket='textfixlogs')
    new_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'] not in processed]
    
    cur.close()
    conn.close()
    
    return new_files
