

SELECT
  DATE_TRUNC('minute', msg_timestamp) AS trading_minute,
  symbol,
  COUNT(DISTINCT cl_ord_id) AS total_orders,
  COUNT(CASE WHEN exec_type = 'F' THEN 1 END) AS execution_count,
  COUNT(CASE WHEN ord_status = '8' THEN 1 END) * 1.0 / COUNT(*) AS reject_rate,
  AVG(latency_interval) AS avg_latency,
  COUNT(CASE WHEN checksum IS NULL OR checksum <= 0 THEN 1 END) AS invalid_checksums,
  AVG(body_length) AS avg_body_length
FROM "duckdb_pictet"."main_02_intermediate_fix"."refine_fix_trading"
GROUP BY 1, 2
ORDER BY 1, 2