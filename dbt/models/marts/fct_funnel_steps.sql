-- long format: one row per person per funnel step reached (strict order)
{% set steps = funnel_steps() %}
with w as (select * from {{ ref('int_funnel_wide') }})
{% for s in steps %}
select
    md5(person_id || '-{{ loop.index }}')        as funnel_step_id,
    person_id,
    {{ loop.index }}                             as step_order,
    '{{ s }}'                                    as step_name,
    step_{{ loop.index }}_at                     as reached_at,
    {% if loop.index > 1 %}date_diff('second', step_{{ loop.index - 1 }}_at, step_{{ loop.index }}_at) / 3600.0
    {% else %}cast(null as double){% endif %}    as hours_from_previous_step,
    date_diff('second', step_1_at, step_{{ loop.index }}_at) / 3600.0 as hours_from_funnel_start
from w where step_{{ loop.index }}_at is not null
{{ "union all" if not loop.last }}
{% endfor %}
