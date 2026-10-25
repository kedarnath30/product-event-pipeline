select
    message_id,
    anonymous_id,
    user_id,
    event_ts,
    received_at,
    device_type,
    json_extract_string(payload, '$.traits.state')             as state,
    json_extract_string(payload, '$.traits.home_value_band')   as home_value_band,
    json_extract_string(payload, '$.traits.signup_method')     as signup_method,
    try_cast(json_extract_string(payload, '$.traits.created_at') as timestamptz) as user_created_at
from {{ ref('stg_segment__events') }}
where message_type = 'identify'
