-- DQ gate: share of messages the tracking-plan validator rejected must stay
-- under var('max_quarantine_rate') (5%). Breaching it fails `dbt build`.
with c as (
    select (select count(*) from {{ source('segment', 'events_quarantine') }}) as quarantined,
           (select count(*) from {{ source('segment', 'events') }})            as accepted
)
select quarantined, accepted, quarantined / (quarantined + accepted) as quarantine_rate
from c
where quarantined / nullif(quarantined + accepted, 0) > {{ var('max_quarantine_rate') }}
