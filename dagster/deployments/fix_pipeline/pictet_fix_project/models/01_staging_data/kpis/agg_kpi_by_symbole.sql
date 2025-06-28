{{ config(materialized='view', schema='report_fix') }}

select
  symbol,
  currency,
  msg_type,
  count(*) as nb_messages,
  sum(qty) as volume,
  avg(price) as avg_price
from {{ ref('fact_fix_messages') }}
group by 1, 2, 3
