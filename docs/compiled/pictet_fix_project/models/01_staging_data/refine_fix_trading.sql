-- intermediate/fix__int_order_metrics.sql


WITH enriched AS (
  SELECT
    *,
    msg_timestamp - transact_time AS latency_interval,
    ROW_NUMBER() OVER (PARTITION BY cl_ord_id ORDER BY msg_timestamp) = 1 AS is_first_msg
  FROM "duckdb_pictet"."main_01_staging_fix"."stg_fix_trading"
)

SELECT * FROM enriched