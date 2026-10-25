-- funnel conversion by first-touch acquisition source, plus an 'all' row set
with steps as (
    select f.*, u.first_touch_source
    from {{ ref('fct_funnel_steps') }} f
    join {{ ref('dim_users') }} u using (person_id)
),
by_source as (
    select first_touch_source as segment, step_order, step_name, count(*) as users,
           median(hours_from_previous_step) as median_hours_from_previous_step
    from steps group by all
    union all
    select 'all', step_order, step_name, count(*), median(hours_from_previous_step)
    from steps group by all
)
select
    segment || ':' || step_order as funnel_row_id,
    segment, step_order, step_name, users,
    round(users / lag(users) over (partition by segment order by step_order), 4)         as conversion_from_previous,
    round(users / first_value(users) over (partition by segment order by step_order), 4) as conversion_from_start,
    round(median_hours_from_previous_step, 2)                                           as median_hours_from_previous_step
from by_source
