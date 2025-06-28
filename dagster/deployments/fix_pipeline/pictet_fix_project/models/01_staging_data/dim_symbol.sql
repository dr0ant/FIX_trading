{{ config(materialized='view', schema='mart_fix') }}

select distinct
  symbol,
  currency
from {{ ref('stg_fix_trading') }}
where symbol is not null
