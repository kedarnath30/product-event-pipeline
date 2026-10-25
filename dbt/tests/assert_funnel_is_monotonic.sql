-- an ordered funnel can never gain users from one step to the next
select segment, step_order, users, prev_users
from (
    select segment, step_order, users,
           lag(users) over (partition by segment order by step_order) as prev_users
    from {{ ref('mart_funnel_conversion') }}
)
where prev_users is not null and users > prev_users
