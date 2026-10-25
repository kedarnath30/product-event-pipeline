-- sessions end after they start, contain >= 1 event and never exceed 24h
select * from {{ ref('fct_sessions') }}
where ended_at < started_at or n_events < 1 or duration_seconds > 86400
