{{ config(materialized='table', schema='03_KPI_fix') }}


select
    sender_comp_id,
    target_comp_id,
    count(*) as nb_messages,
    count(case when msg_type = 'D' then 1 end) as nb_orders,
    count(case when msg_type = '8' then 1 end) as nb_exec_reports,
    sum(try_cast(qty as double)) as volume_total,
    avg(latency_interval) as avg_latency_ms
from {{ ref('refine_fix_trading') }}
group by sender_comp_id, target_comp_id
