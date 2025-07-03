

select
  symbol,
  currency,
  msg_type,
  count(*) as nb_messages,
  sum(qty) as volume,
  avg(price) as avg_price
from "duckdb_pictet"."main"."fact_fix_messages"
group by 1, 2, 3