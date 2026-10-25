-- track calls with typed, tracking-plan properties
select
    message_id,
    event_name,
    anonymous_id,
    user_id,
    event_ts,
    received_at,
    arrival_lag_seconds,
    device_type,
    app_version,
    utm_source,
    json_extract_string(payload, '$.properties.property_state')                as property_state,
    json_extract_string(payload, '$.properties.entry_point')                   as entry_point,
    try_cast(json_extract(payload, '$.properties.estimated_home_value') as double) as estimated_home_value,
    try_cast(json_extract(payload, '$.properties.mortgage_balance') as double)     as mortgage_balance,
    try_cast(json_extract(payload, '$.properties.equity_requested') as double)     as equity_requested,
    json_extract_string(payload, '$.properties.property_type')                 as property_type,
    json_extract_string(payload, '$.properties.signup_method')                 as signup_method,
    json_extract_string(payload, '$.properties.application_id')                as application_id,
    json_extract_string(payload, '$.properties.product')                       as product,
    try_cast(json_extract(payload, '$.properties.document_count') as integer)  as document_count,
    try_cast(json_extract(payload, '$.properties.offer_amount') as double)     as offer_amount,
    try_cast(json_extract(payload, '$.properties.investment_percentage') as double) as investment_percentage
from {{ ref('stg_segment__events') }}
where message_type = 'track'
