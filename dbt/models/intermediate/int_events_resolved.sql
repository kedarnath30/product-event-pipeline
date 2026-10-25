-- every message with a stable person_id: the user_id if known (directly or via
-- stitching, which also back-fills pre-signup anonymous activity), else the anonymous_id
select
    e.message_id,
    e.message_type,
    e.event_name,
    e.anonymous_id,
    coalesce(e.user_id, m.user_id)                         as user_id,
    coalesce(e.user_id, m.user_id, e.anonymous_id)         as person_id,
    coalesce(e.user_id, m.user_id) is not null             as is_identified,
    e.event_ts,
    e.received_at,
    e.arrival_lag_seconds,
    e.device_type,
    e.utm_source
from {{ ref('stg_segment__events') }} e
left join {{ ref('int_identity_map') }} m using (anonymous_id)
