with s as (select * from {{ ref('int_sessionized') }})
select
    session_id,
    person_id,
    session_number,
    min(event_ts)                                              as started_at,
    max(event_ts)                                              as ended_at,
    cast(min(event_ts) as date)                                as session_date,
    date_diff('second', min(event_ts), max(event_ts))          as duration_seconds,
    count(*)                                                   as n_events,
    count(*) filter (where message_type = 'page')              as n_pageviews,
    count(*) filter (where message_type = 'track')             as n_tracks,
    arg_min(event_name, event_ts) filter (where message_type = 'page') as landing_page,
    coalesce(arg_min(utm_source, event_ts) filter (where utm_source is not null), 'direct') as session_source,
    arg_min(device_type, event_ts)                             as device_type,
    bool_or(is_identified)                                     as is_identified,
    bool_or(event_name = 'Estimate Started')                   as has_estimate_start,
    bool_or(event_name = 'Application Submitted')              as has_application,
    bool_or(event_name = 'Offer Accepted')                     as has_offer_accepted,
    max(arrival_lag_seconds)                                   as max_arrival_lag_seconds
from s
group by session_id, person_id, session_number
