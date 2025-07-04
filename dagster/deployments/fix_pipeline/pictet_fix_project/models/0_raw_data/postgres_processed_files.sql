{{
  config(
    materialized='view', 
    schema='00_landing_external'
  )
}}

SELECT *
FROM postgres_scan_pushdown('host=localhost port=5432 user=admin password=admin dbname=fix_db', 'public', 'processed_files')