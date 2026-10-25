select
    message_id,
    anonymous_id,
    user_id,
    event_ts,
    received_at,
    device_type,
    utm_source,
    event_name                                           as page_name,
    json_extract_string(payload, '$.properties.path')    as page_path
from {{ ref('stg_segment__events') }}
where message_type = 'page'
