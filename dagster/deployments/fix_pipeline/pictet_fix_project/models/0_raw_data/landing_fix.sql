{{ 
  config(
    materialized='table',
    schema='landing_fix',
    alias='fix_trading_raw'
  ) 
}}

SELECT
  filename,  -- Optional: only if added manually during parsing
  "52" AS timestamp,
  "49" AS system,
  "35" AS msg_type,
  "11" AS cl_ord_id,
  "37" AS order_id,
  "17" AS exec_id,
  "150" AS exec_type,
  "39" AS ord_status,
  "1"   AS account,
  "55"  AS symbol,
  "54"  AS side,
  "15"  AS currency,
  "44"  AS price,
  "38"  AS quantity,
  "60"  AS transact_time,
  "49"  AS sender_comp_id,
  "56"  AS target_comp_id,
  "59"  AS time_in_force,
  "10"  AS checksum,
  "9"   AS body_length,
  "8"   AS begin_string,
  "34"  AS msg_seq_num,
  "52"  AS sending_time,
  "526" AS secondary_cl_ord_id
FROM read_parquet('s3://parquetfixlogs/*.parquet')
ORDER BY "52"