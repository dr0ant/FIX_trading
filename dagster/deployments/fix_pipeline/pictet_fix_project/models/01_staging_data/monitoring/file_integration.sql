-- intermediate/fix__int_order_metrics.sql
{{ 
    config(materialized='table',
    schema='000_monitoring')
 }}

 select 
 pg_txt_files.*,
 landing_fix.*
 from 
   {{ ref('postgres_processed_files') }} as pg_txt_files
LEFT join  {{ ref('landing_fix') }} as landing_fix
    on REPLACE(pg_txt_files.file_name,'.txt','') = REPLACE (REPLACE(landing_fix.filename,'.parquet',''),'s3://parquetfixlogs/','')