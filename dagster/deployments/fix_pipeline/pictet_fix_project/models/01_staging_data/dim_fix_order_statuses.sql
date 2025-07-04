{{
  config(
    materialized='table',
    schema='03_analytics',
    alias='dim_fix_order_statuses'
  )
}}

SELECT 39 AS tag_id, 'OrdStatus' AS field_name, '0' AS status_code, 'New' AS status_value, 'Order is new and has been accepted by the exchange/system.' AS description
UNION ALL SELECT 39, 'OrdStatus', '1', 'Partially Filled', 'Part of the order has been executed.'
UNION ALL SELECT 39, 'OrdStatus', '2', 'Filled', 'The order has been completely executed.'
UNION ALL SELECT 39, 'OrdStatus', '3', 'Done for Day', 'Order is no longer active for the day.'
UNION ALL SELECT 39, 'OrdStatus', '4', 'Canceled', 'Order has been canceled.'
UNION ALL SELECT 39, 'OrdStatus', '5', 'Replaced', 'Order has been replaced/amended.'
UNION ALL SELECT 39, 'OrdStatus', '6', 'Pending Cancel', 'Currently being canceled.'
UNION ALL SELECT 39, 'OrdStatus', '7', 'Stopped', 'Order has been stopped (usually by a specialist in a manual execution market).'
UNION ALL SELECT 39, 'OrdStatus', '8', 'Rejected', 'Order has been rejected by the exchange/system.'
UNION ALL SELECT 39, 'OrdStatus', '9', 'Suspended', 'Order has been suspended.'
UNION ALL SELECT 39, 'OrdStatus', 'A', 'Pending New', 'Currently being submitted.'
UNION ALL SELECT 39, 'OrdStatus', 'B', 'Calculated', 'Quantity has been calculated.'
UNION ALL SELECT 39, 'OrdStatus', 'C', 'Expired', 'Order has expired.'
UNION ALL SELECT 39, 'OrdStatus', 'D', 'Accepted for Bidding', 'Order has been accepted for bidding (for use in auctions).'
UNION ALL SELECT 39, 'OrdStatus', 'E', 'Pending Replace', 'Currently being replaced/amended.'
UNION ALL SELECT 39, 'OrdStatus', 'F', 'Accepted for Execution', 'Order has been accepted for execution.'
UNION ALL SELECT 39, 'OrdStatus', 'G', 'Pending Cancel/Replace', 'Currently pending cancel or replace.'
-- Add more OrdStatus values as needed based on FIX specifications

UNION ALL SELECT 150 AS tag_id, 'ExecType' AS field_name, '0' AS status_code, 'New' AS status_value, 'New order accepted.' AS description
UNION ALL SELECT 150, 'ExecType', '1', 'Partial Fill', 'Partial execution of an order.'
UNION ALL SELECT 150, 'ExecType', '2', 'Fill', 'Complete execution of an order.'
UNION ALL SELECT 150, 'ExecType', '3', 'Done for Day', 'Order no longer active for the day.'
UNION ALL SELECT 150, 'ExecType', '4', 'Canceled', 'Order has been canceled.'
UNION ALL SELECT 150, 'ExecType', '5', 'Replace', 'Order has been replaced/amended.'
UNION ALL SELECT 150, 'ExecType', '6', 'Pending Cancel', 'Currently being canceled.'
UNION ALL SELECT 150, 'ExecType', '7', 'Stopped', 'Order has been stopped.'
UNION ALL SELECT 150, 'ExecType', '8', 'Rejected', 'Order has been rejected.'
UNION ALL SELECT 150, 'ExecType', '9', 'Suspended', 'Order has been suspended.'
UNION ALL SELECT 150, 'ExecType', 'A', 'Pending New', 'Currently being submitted.'
UNION ALL SELECT 150, 'ExecType', 'B', 'Calculated', 'Quantity has been calculated.'
UNION ALL SELECT 150, 'ExecType', 'C', 'Expired', 'Order has expired.'
UNION ALL SELECT 150, 'ExecType', 'D', 'Restated', 'Order has been restated.'
UNION ALL SELECT 150, 'ExecType', 'E', 'Pending Replace', 'Currently being replaced/amended.'
UNION ALL SELECT 150, 'ExecType', 'F', 'Trade', 'Order has been traded.'
UNION ALL SELECT 150, 'ExecType', 'G', 'Trade Correct', 'Correction to a previous trade.'
UNION ALL SELECT 150, 'ExecType', 'H', 'Trade Cancel', 'Cancellation of a previously reported trade.'
UNION ALL SELECT 150, 'ExecType', 'I', 'Order Status', 'Status request response for an order.'
UNION ALL SELECT 150, 'ExecType', 'J', 'Trade Confirm', 'Confirmation of a trade.'
-- Add more ExecType values as needed based on FIX specifications
