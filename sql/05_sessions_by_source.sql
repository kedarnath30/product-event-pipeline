-- Session quality by acquisition source
select session_source,
       count(*) as sessions,
       round(avg(n_events), 2) as avg_events,
       round(quantile_cont(duration_seconds, 0.5), 0) as p50_duration_s,
       round(avg(has_estimate_start::int), 4) as estimate_start_rate,
       round(avg(has_application::int), 4) as application_rate
from marts.fct_sessions
group by 1
order by sessions desc;
