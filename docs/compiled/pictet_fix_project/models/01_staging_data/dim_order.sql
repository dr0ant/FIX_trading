

select distinct
  cl_ord_id,
  secondary_cl_ord_id,
  order_id,
  account,
  time_in_force
from "duckdb_pictet"."main_staging_fix"."stg_fix_trading"
where cl_ord_id is not null