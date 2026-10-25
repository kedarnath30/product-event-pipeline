{#
  Freshness test that uses the pipeline's clock (var pipeline_as_of) instead of
  wall-clock now(), so it is meaningful for a replayed/synthetic stream.
  Fails when the newest value of `column_name` is more than `max_lag_hours`
  behind the reference time.
#}
{% test recency_within(model, column_name, max_lag_hours, reference=none) %}
{% set ref_ts = reference if reference is not none else var('pipeline_as_of') %}
with s as (
    select max({{ column_name }}) as latest, timestamp '{{ ref_ts }}' as reference_ts from {{ model }}
)
select latest, reference_ts,
       date_diff('minute', latest, reference_ts) / 60.0 as lag_hours
from s
where latest is null
   or latest < reference_ts - interval ({{ max_lag_hours }}) hour
{% endtest %}
