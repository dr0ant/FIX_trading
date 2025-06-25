import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from minio import Minio
from io import BytesIO
from datetime import datetime


class FixToParquetConverter:
    def __init__(self, minio_endpoint, access_key, secret_key, secure=True):
        self.minio_client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

    def download_fix_file(self, bucket_name, object_name):
        response = self.minio_client.get_object(bucket_name, object_name)
        return response.read().decode("utf-8")

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

    def convert_to_parquet(self, df, parquet_file_path):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file_path)

    def upload_parquet_to_minio(self, bucket_name, object_name, parquet_file_path):
        with open(parquet_file_path, 'rb') as f:
            self.minio_client.put_object(
                bucket_name,
                object_name,
                f,
                length=os.path.getsize(parquet_file_path),
                content_type='application/octet-stream'
            )

    def process(self, fix_bucket, fix_object, parquet_bucket, parquet_object):
        print("Downloading FIX log...")
        fix_text = self.download_fix_file(fix_bucket, fix_object)

        print("Parsing FIX log...")
        df = self.parse_fix_lines(fix_text)

        parquet_file = "/tmp/fix_output.parquet"
        print("Converting to Parquet...")
        self.convert_to_parquet(df, parquet_file)

        print("Uploading Parquet to MinIO...")
        self.upload_parquet_to_minio(parquet_bucket, parquet_object, parquet_file)

        print("Done.")


if __name__ == "__main__":
    # Set your MinIO credentials and endpoint
    MINIO_ENDPOINT = "localhost:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"

    converter = FixToParquetConverter(
        minio_endpoint=MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False  # Set to True if using https
    )

    # Input and output info
    FIX_BUCKET = "textfixlogs"
    FIX_OBJECT = "fix_logs.txt"
    PARQUET_BUCKET = "parquetfixlogs"
    PARQUET_OBJECT = f"fix_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

    converter.process(FIX_BUCKET, FIX_OBJECT, PARQUET_BUCKET, PARQUET_OBJECT)
