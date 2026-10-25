{{ config(materialized='incremental', unique_key='message_id', incremental_strategy='append') }}

-- One row per Segment message. De-duplicates SDK retries on message_id (first
-- delivery wins). Incremental on loaded_at (load time, not event time), so
-- late-arriving events are picked up by whichever run loads them.
with src as (
    select * from {{ source('segment', 'events') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
      and message_id not in (select message_id from {{ this }})
    {% endif %}
),
deduped as (
    select *, row_number() over (partition by message_id order by received_at, loaded_at) as delivery_rank
    from src
)
select
    message_id,
    type                                                    as message_type,
    event_name,
    anonymous_id,
    user_id,
    event_ts,
    sent_at,
    received_at,
    loaded_at,
    date_diff('second', event_ts, received_at)              as arrival_lag_seconds,
    library_name,
    app_version,
    device_type,
    json_extract_string(payload, '$.context.campaign.source') as utm_source,
    json_extract_string(payload, '$.context.campaign.medium') as utm_medium,
    payload
from deduped
where delivery_rank = 1
