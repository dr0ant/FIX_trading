

select distinct
  symbol,
  currency
from "duckdb_pictet"."main_staging_fix"."stg_fix_trading"
where symbol is not null