-- intermediate/fix__int_order_metrics.sql
{{ config(materialized='view',
 schema='02_intermediate_fix') }}

WITH enriched AS (
  SELECT
    *,
    msg_timestamp - transact_time AS latency_interval,
    ROW_NUMBER() OVER (PARTITION BY cl_ord_id ORDER BY msg_timestamp) = 1 AS is_first_msg
  FROM {{ ref('stg_fix_trading') }}
)

SELECT * FROM enriched
