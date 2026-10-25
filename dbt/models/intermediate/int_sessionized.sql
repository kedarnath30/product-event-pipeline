-- 30-minute inactivity sessionization per person (Segment/GA convention)
with ordered as (
    select *,
        lag(event_ts) over (partition by person_id order by event_ts, message_id) as prev_ts
    from {{ ref('int_events_resolved') }}
),
flagged as (
    select *,
        case when prev_ts is null
               or date_diff('second', prev_ts, event_ts) > {{ var('session_timeout_minutes') }} * 60
             then 1 else 0 end as is_new_session
    from ordered
),
numbered as (
    select *,
        sum(is_new_session) over (partition by person_id order by event_ts, message_id
                                  rows between unbounded preceding and current row) as session_number
    from flagged
)
select
    md5(person_id || '-' || cast(session_number as varchar)) as session_id,
    session_number,
    message_id, message_type, event_name, anonymous_id, user_id, person_id, is_identified,
    event_ts, received_at, arrival_lag_seconds, device_type, utm_source, is_new_session
from numbered
