-- one row per person (identified user, or anonymous visitor never stitched)
with ev as (select * from {{ ref('int_events_resolved') }}),
first_event as (
    select person_id, event_ts as first_seen_at, device_type as first_device_type,
           coalesce(utm_source, 'direct') as first_touch_source
    from ev
    qualify row_number() over (partition by person_id order by event_ts, message_id) = 1
),
agg as (
    select person_id, max(user_id) as user_id, bool_or(is_identified) as is_identified,
           max(event_ts) as last_seen_at, count(*) as lifetime_events
    from ev group by 1
),
traits as (
    select user_id, arg_min(state, event_ts) as state,
           arg_min(home_value_band, event_ts) filter (where home_value_band is not null) as home_value_band,
           arg_min(signup_method, event_ts) filter (where signup_method is not null) as signup_method
    from {{ ref('stg_segment__identifies') }} group by 1
),
signup as (
    select r.person_id, min(t.event_ts) as signed_up_at
    from {{ ref('stg_segment__tracks') }} t join ev r using (message_id)
    where t.event_name = 'Account Created' group by 1
),
est_state as (
    select r.person_id, arg_min(t.property_state, t.event_ts) as property_state
    from {{ ref('stg_segment__tracks') }} t join ev r using (message_id)
    where t.event_name = 'Estimate Started' group by 1
)
select
    a.person_id, a.user_id, a.is_identified,
    f.first_seen_at, a.last_seen_at, cast(f.first_seen_at as date) as first_seen_date,
    f.first_touch_source, f.first_device_type,
    s.signed_up_at,
    coalesce(tr.state, es.property_state) as state,
    tr.home_value_band, tr.signup_method,
    a.lifetime_events
from agg a
join first_event f using (person_id)
left join traits tr on tr.user_id = a.user_id
left join signup s using (person_id)
left join est_state es using (person_id)
