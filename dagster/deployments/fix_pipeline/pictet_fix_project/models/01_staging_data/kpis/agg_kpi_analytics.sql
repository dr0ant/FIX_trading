{{
  config(
    materialized='table',
    schema='03_analytics',
    alias='agg_system_message_flow'
  )
}}

SELECT
  sender_comp_id,
  target_comp_id,
  fix_trading_raw.msg_type,
  fix_trading_raw.symbol,
  dmt.msg_type_description,
  COUNT(*) AS message_count
FROM
  {{ ref('refine_fix_trading') }} fix_trading_raw
LEFT JOIN
  {{ ref('dim_fix_message_types') }} dmt
  ON fix_trading_raw.msg_type = dmt.msg_type
GROUP BY
  sender_comp_id,
  target_comp_id,
  fix_trading_raw.msg_type,
  dmt.msg_type_description,
  fix_trading_raw.symbol
ORDER BY
  message_count DESC
