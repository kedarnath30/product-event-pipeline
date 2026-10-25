-- How long does each step take? (hours from previous step, among people who reached it)
select step_order, step_name, count(*) as users,
       round(quantile_cont(hours_from_previous_step, 0.5), 2)  as p50_hours,
       round(quantile_cont(hours_from_previous_step, 0.9), 2)  as p90_hours,
       round(quantile_cont(hours_from_funnel_start, 0.5), 2)   as p50_hours_from_start
from marts.fct_funnel_steps
group by 1, 2
order by 1;
