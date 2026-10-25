-- one row per person who entered the funnel. Strict ordered funnel: a step only
-- counts if the previous (ordered) step happened at or before it.
{% set steps = funnel_steps() %}
with firsts as (
    select r.person_id, t.event_name, min(t.event_ts) as first_at
    from {{ ref('stg_segment__tracks') }} t
    join {{ ref('int_events_resolved') }} r using (message_id)
    where t.event_name in ({% for s in steps %}'{{ s }}'{{ "," if not loop.last }}{% endfor %})
    group by 1, 2
),
pivoted as (
    select person_id
    {%- for s in steps %}
        , max(case when event_name = '{{ s }}' then first_at end) as raw_{{ loop.index }}_at
    {%- endfor %}
    from firsts
    group by 1
),
ordered_1 as (
    select *, raw_1_at as step_1_at from pivoted where raw_1_at is not null
)
{%- for k in range(2, steps | length + 1) %},
ordered_{{ k }} as (
    select *,
        case when step_{{ k - 1 }}_at is not null and raw_{{ k }}_at >= step_{{ k - 1 }}_at
             then raw_{{ k }}_at end as step_{{ k }}_at
    from ordered_{{ k - 1 }}
)
{%- endfor %}
select person_id
{%- for k in range(1, steps | length + 1) %}, step_{{ k }}_at{% endfor %}
from ordered_{{ steps | length }}
