{#
  Volume test: every complete day between the first day and the reference day
  must have at least `min_rows_per_day` rows, and must not move more than
  `max_pct_change` vs the trailing 7-day median. Missing days count as 0 rows.
  This is the hard guardrail; softer anomaly alerts live in src/dq_monitor.py.
#}
{% test daily_volume_within_bounds(model, column_name, min_rows_per_day, max_pct_change=0.5, reference=none) %}
{% set ref_ts = reference if reference is not none else var('pipeline_as_of') %}
with bounds as (
    select min(cast({{ column_name }} as date)) as d0,
           cast(timestamp '{{ ref_ts }}' - interval 1 day as date) as d1
    from {{ model }}
),
spine as (
    select cast(range as date) as d
    from bounds, range(bounds.d0, bounds.d1 + interval 1 day, interval 1 day)
),
daily as (
    select cast({{ column_name }} as date) as d, count(*) as n from {{ model }} group by 1
),
filled as (
    select spine.d, coalesce(daily.n, 0) as n from spine left join daily using (d)
),
scored as (
    select d, n,
           median(n) over (order by d rows between 7 preceding and 1 preceding) as trailing_median
    from filled
)
select * from scored
where n < {{ min_rows_per_day }}
   or (trailing_median is not null and abs(n - trailing_median) / trailing_median > {{ max_pct_change }})
{% endtest %}
