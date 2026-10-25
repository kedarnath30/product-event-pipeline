-- Ordered funnel by first device: where does mobile web leak?
select u.first_device_type as device,
       count(*) filter (where step_order = 1) as estimate_started,
       count(*) filter (where step_order = 4) as applications,
       count(*) filter (where step_order = 5) as documents_uploaded,
       count(*) filter (where step_order = 7) as offers_accepted,
       round(count(*) filter (where step_order = 5) / nullif(count(*) filter (where step_order = 4), 0), 4) as application_to_docs_rate,
       round(count(*) filter (where step_order = 7) / count(*) filter (where step_order = 1), 4) as start_to_accept_rate
from marts.fct_funnel_steps f
join marts.dim_users u using (person_id)
group by 1
order by start_to_accept_rate desc;
