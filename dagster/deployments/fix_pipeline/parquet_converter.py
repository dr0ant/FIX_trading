import pandas as pd
from dagster import op, In, Out

@op(ins={"file_list": In(list)}, out=Out(list))
def convert_to_parquet(context, file_list):
    import boto3
    import io
    import time
    processed_info = []

    minio = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    for file_key in file_list:
        # Read the file
        response = minio.get_object(Bucket='textfixlogs', Key=file_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()), sep="\t", header=None)

        # Write Parquet to memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        # Upload to parquetfixlogs
        minio.put_object(
            Bucket='parquetfixlogs',
            Key=file_key.replace(".txt", ".parquet"),
            Body=parquet_buffer.getvalue()
        )

        processed_info.append({
            "file_name": file_key,
            "file_source": "textfixlogs",
            "process_date": pd.Timestamp.now().date(),
            "execution_time": pd.Timestamp.now(),
            "nb_lines": len(df)
        })
    
    return processed_info
