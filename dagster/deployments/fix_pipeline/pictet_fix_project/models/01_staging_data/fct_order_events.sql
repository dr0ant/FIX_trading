{{
  config(
    materialized='table',
    schema='03_analytics', 
    alias='fct_order_events'
  )
}}

SELECT
  raw.msg_timestamp, 
  raw.account,
  raw.symbol,
  raw.side,
  raw.cl_ord_id,
  raw.order_id,
  raw.exec_id,
  raw.qty,          
  raw.price,
  raw.transact_time,
  raw.sender_comp_id,
  raw.target_comp_id,
  raw.msg_type,
  dmt.msg_type_description,
  raw.ord_status,
  dos_ord.status_value AS ord_status_description,
  raw.exec_type,
  dos_exec.status_value AS exec_type_description,
  CASE
    WHEN raw.exec_type = '2' THEN 'Executed'
    WHEN raw.exec_type = '1' THEN 'Partially Filled'
    WHEN raw.exec_type = '4' THEN 'Canceled'
    WHEN raw.exec_type = '8' THEN 'Rejected'
    -- You can add more specific categorization if needed
    ELSE 'Other'
  END AS event_category
FROM
  {{ ref('refine_fix_trading') }} raw -- Updated reference to the new staging model
LEFT JOIN
  {{ ref('dim_fix_message_types') }} dmt
  ON raw.msg_type = dmt.msg_type
LEFT JOIN
  {{ ref('dim_fix_order_statuses') }} dos_ord
  ON raw.ord_status = dos_ord.status_code AND dos_ord.tag_id = 39
LEFT JOIN
  {{ ref('dim_fix_order_statuses') }} dos_exec
  ON raw.exec_type = dos_exec.status_code AND dos_exec.tag_id = 150
WHERE
  raw.msg_type = '8' -- Focus on Execution Reports for status updates
  OR raw.msg_type = 'D' -- Also include New Order Singles for initial submission
ORDER BY
  raw.msg_timestamp, raw.cl_ord_id
