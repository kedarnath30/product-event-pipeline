-- instrumentation contract: every Account Created must be followed by an
-- identify for the same user (within 1 day). Tolerates <= 1% misses, since an
-- identify can be lost to the validator or still be in flight on a phone.
with acc as (
    select t.user_id, t.event_ts from {{ ref('stg_segment__tracks') }} t
    where t.event_name = 'Account Created'
),
matched as (
    select acc.user_id,
           exists (select 1 from {{ ref('stg_segment__identifies') }} i
                   where i.user_id = acc.user_id
                     and i.event_ts between acc.event_ts - interval 1 minute and acc.event_ts + interval 1 day) as has_identify
    from acc
)
select count(*) filter (where not has_identify) as missing, count(*) as accounts
from matched
having count(*) filter (where not has_identify) > 0.01 * count(*)
