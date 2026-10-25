-- anonymous_id -> user_id stitching. Any message carrying both ids links them
-- (identify calls, and every call made after login). If a device was shared by
-- several users, the first user seen on it wins.
with pairs as (
    select anonymous_id, user_id, min(event_ts) as first_linked_at
    from {{ ref('stg_segment__events') }}
    where anonymous_id is not null and user_id is not null
    group by 1, 2
)
select anonymous_id, user_id, first_linked_at
from pairs
qualify row_number() over (partition by anonymous_id order by first_linked_at, user_id) = 1
