{{ config(materialized='table', schema='mart_fix') }}

select
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
  target_comp_id
from {{ ref('stg_fix_trading') }}
