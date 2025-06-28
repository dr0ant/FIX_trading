{{ config(materialized='view', schema='mart_fix') }}

select distinct
  sender_comp_id,
  target_comp_id
from {{ ref('stg_fix_trading') }}
