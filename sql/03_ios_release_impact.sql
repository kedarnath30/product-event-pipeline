-- Business impact of the iOS 3.4.0 schema bug: Estimate Completed events that
-- reached the warehouse vs were quarantined, iOS only, by day.
with ok as (
    select cast(event_ts as date) as d, count(*) as loaded
    from staging.stg_segment__tracks
    where event_name = 'Estimate Completed' and device_type = 'ios'
    group by 1
),
bad as (
    select cast(received_at as date) as d, count(*) as quarantined
    from raw.events_quarantine
    where event_name = 'Estimate Completed'
      and json_extract_string(payload, '$.context.device.type') = 'ios'
    group by 1
)
select coalesce(ok.d, bad.d) as day, coalesce(loaded, 0) as loaded, coalesce(quarantined, 0) as quarantined,
       round(coalesce(quarantined, 0) / (coalesce(loaded, 0) + coalesce(quarantined, 0)), 4) as share_quarantined
from ok full join bad using (d)
order by 1;
