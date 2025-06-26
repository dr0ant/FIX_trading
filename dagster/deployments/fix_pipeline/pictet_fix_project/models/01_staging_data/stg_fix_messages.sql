select
    message_id,
    msg_type,
    cl_ord_id,
    order_id,
    exec_id,
    account,
    symbol,
    currency,
    sender_comp_id,
    target_comp_id,
    fix_version,
    msg_seq_num,
    msg_timestamp,
    recv_timestamp,
    latency_ms
from {{ ref('stg_fix_trading') }}
where msg_type in ('D', '8')  -- include only Order/Execution reports
