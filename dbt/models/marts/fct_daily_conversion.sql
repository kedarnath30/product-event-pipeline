-- conversion by funnel-entry cohort (day of first Estimate Started).
-- Recent cohorts are right-censored (offers take days), so `is_mature` flags
-- cohorts old enough to read.
with w as (select * from {{ ref('int_funnel_wide') }})
select
    cast(step_1_at as date)                                        as cohort_date,
    count(*)                                                       as estimate_starters,
    count(step_2_at)                                               as estimates_completed,
    count(step_3_at)                                               as accounts_created,
    count(step_4_at)                                               as applications_submitted,
    count(step_7_at)                                               as offers_accepted,
    round(count(step_3_at) / count(*), 4)                          as start_to_account_rate,
    round(count(step_4_at) / count(*), 4)                          as start_to_application_rate,
    round(count(step_7_at) / count(*), 4)                          as start_to_accept_rate,
    cast(step_1_at as date) <= cast(timestamp '{{ var("pipeline_as_of") }}' as date) - {{ var('funnel_maturity_days') }} as is_mature
from w
group by 1
