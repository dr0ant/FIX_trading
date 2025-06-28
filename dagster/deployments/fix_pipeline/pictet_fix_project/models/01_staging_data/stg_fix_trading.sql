-- DBT LAYER: staging/landing_fix__stg_fix_trading.sql
-- This prepares the raw FIX logs for transformation

{{
  config(
    materialized='view',
    schema='staging_fix'
  )
}}


WITH base AS (
  SELECT
  	STRPTIME(timestamp, '%Y%m%d-%H:%M:%S.%f')::timestamp AS msg_timestamp,
  	STRPTIME('2023-03-09-09:30:05.063', '%Y-%m-%d-%H:%M:%S.%f')::timestamp as transact_time,
  	STRPTIME(sending_time, '%Y%m%d-%H:%M:%S.%f')::timestamp AS sending_time,
    msg_type,
    cl_ord_id,
    secondary_cl_ord_id,
    order_id,
    exec_id,
    exec_type,
    ord_status,
    account,
    symbol,
    side,
    currency,
    try_cast(price AS double) AS price,
    try_cast(quantity AS double) AS qty,
    sender_comp_id,
    target_comp_id,
    time_in_force,
    try_cast(checksum AS int) AS checksum,
    try_cast(body_length AS int) AS body_length,
    try_cast(msg_seq_num AS int) AS msg_seq_num,
    begin_string,
    filename,
    system
  FROM {{ ref('landing_fix') }}
)

SELECT * FROM base