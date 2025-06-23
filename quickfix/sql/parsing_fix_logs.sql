-- Step 1: Set MinIO connection parameters
SET s3_region='us-east-1'; -- Placeholder region for MinIO
SET s3_url_style='path'; -- Use path-style access for MinIO
SET s3_endpoint='localhost:9000'; -- Ensure the endpoint includes 'http://'
SET s3_access_key_id='minioadmin'; -- MinIO root user
SET s3_secret_access_key='minioadmin'; -- MinIO root password
SET s3_use_ssl='false'; -- Set to 'true' if using HTTPS

-- Step 2: Load necessary extensions from DuckDB's online repository
LOAD httpfs; -- Load the HTTPFS extension
LOAD parquet; -- Load the Parquet extension

-- Step 3: Query the Parquet file in the MinIO bucket
SELECT *
FROM read_parquet('s3://parquetfixlogs/fix_logs.parquet')
LIMIT 100;



SELECT
  filename,
  timestamp,
  system,
  fix_tags ->> '35' AS msg_type,
  fix_tags ->> '11' AS cl_ord_id,
  fix_tags ->> '37' AS order_id,
  fix_tags ->> '17' AS exec_id,
  fix_tags ->> '150' AS exec_type,
  fix_tags ->> '39' AS ord_status,
  fix_tags ->> '1'   AS account,
  fix_tags ->> '55'  AS symbol,
  fix_tags ->> '54'  AS side,
  fix_tags ->> '15'  AS currency,
  fix_tags ->> '44'  AS price,
  fix_tags ->> '38'  AS quantity,
  fix_tags ->> '60'  AS transact_time,
  fix_tags ->> '49'  AS sender_comp_id,
  fix_tags ->> '56'  AS target_comp_id,
  fix_tags ->> '59'  AS time_in_force,
  fix_tags ->> '10'  AS checksum,
  fix_tags ->> '9'   AS body_length,
  fix_tags ->> '8'   AS begin_string,
  fix_tags ->> '34'  AS msg_seq_num,
  fix_tags ->> '52'  AS sending_time,
  fix_tags ->> '526' AS secondary_cl_ord_id
FROM read_parquet('s3://parquetfixlogs/*.parquet')
WHERE fix_tags IS NOT NULL
ORDER BY timestamp
LIMIT 100;

