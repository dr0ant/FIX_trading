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
    try_cast(timestamp AS timestamp) AS msg_timestamp,
    try_cast(transact_time AS timestamp) AS transact_time,
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
    try_cast(sending_time AS timestamp) AS sending_time,
    filename,
    system
  FROM {{ ref('landing_fix') }}
)

SELECT * FROM base