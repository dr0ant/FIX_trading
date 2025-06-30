SELECT
  msg_timestamp,
  transact_time,
  msg_type,
  exec_type,
  ord_status,
  price,
  qty,
  checksum,
  body_length,
  msg_seq_num,
  begin_string,
  cl_ord_id,
  symbol,
  currency,
  sender_comp_id,
  target_comp_id,

  -- Latency calculation (in milliseconds)
  ROUND(EXTRACT('EPOCH' FROM msg_timestamp - 
        LAG(msg_timestamp) OVER (PARTITION BY cl_ord_id ORDER BY msg_timestamp)) * 1000, 3) AS latency_ms

FROM {{ ref('stg_fix_trading') }}
