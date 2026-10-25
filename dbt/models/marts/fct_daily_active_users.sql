-- DAU / WAU / MAU (rolling 7 / 28 days) and new users, on a complete date spine
with activity as (
    select distinct person_id, cast(event_ts as date) as activity_date, is_identified
    from {{ ref('int_events_resolved') }}
    where event_ts < timestamp '{{ var("pipeline_as_of") }}'
),
spine as (
    select cast(range as date) as activity_date
    from range((select min(activity_date) from activity),
               (select max(activity_date) from activity) + interval 1 day, interval 1 day)
),
dau as (
    select activity_date, count(distinct person_id) as dau,
           count(distinct person_id) filter (where is_identified) as identified_dau
    from activity group by 1
),
rolling as (
    select s.activity_date,
           count(distinct a.person_id) filter (where a.activity_date > s.activity_date - 7)  as wau,
           count(distinct a.person_id)                                                      as mau
    from spine s
    join activity a on a.activity_date between s.activity_date - 27 and s.activity_date
    group by 1
),
new_users as (
    select first_seen_date as activity_date, count(*) as new_users
    from {{ ref('dim_users') }} group by 1
)
select
    s.activity_date,
    coalesce(d.dau, 0)              as dau,
    coalesce(d.identified_dau, 0)   as identified_dau,
    coalesce(n.new_users, 0)        as new_users,
    r.wau,
    r.mau,
    round(coalesce(d.dau, 0) / nullif(r.mau, 0), 4) as dau_mau_ratio,
    s.activity_date - (select min(activity_date) from spine) < 27 as mau_window_incomplete
from spine s
left join dau d using (activity_date)
left join rolling r using (activity_date)
left join new_users n using (activity_date)
