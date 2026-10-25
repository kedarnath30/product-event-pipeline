-- Event-time vs arrival-time lateness by SDK (why marts must tolerate late data)
select library_name,
       count(*) as events,
       round(quantile_cont(arrival_lag_seconds, 0.50), 1) as p50_seconds,
       round(quantile_cont(arrival_lag_seconds, 0.95), 1) as p95_seconds,
       round(quantile_cont(arrival_lag_seconds, 0.99) / 3600, 2) as p99_hours,
       round(max(arrival_lag_seconds) / 3600, 1) as max_hours,
       round(count(*) filter (where arrival_lag_seconds > 3600) / count(*), 4) as share_over_1h
from staging.stg_segment__events
group by 1
order by share_over_1h desc;
