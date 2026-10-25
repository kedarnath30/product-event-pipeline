"""Data-quality monitoring for the event pipeline.

Runs after dbt and writes:
  results/dq_daily_metrics.csv   one row per received day
  results/dq_alerts.json         every alert raised (check, severity, when, value, threshold)
  results/dq_report.md           human-readable summary

Checks
  volume        daily volume vs trailing-7-day median; hourly per-SDK drops (outages)
  validity      daily block (quarantine) rate; per event x app-version rejection spikes
  schema_drift  unplanned properties / events, and type changes, by first-seen time + app version
  completeness  campaign attribution on landing pages, identify after signup, required columns
  timeliness    share of events arriving > 1h late (offline buffering)
  uniqueness    SDK-retry duplicate rate in the raw layer
  privacy       any PII-like keys (always critical)
dbt tests are the hard gate (build fails); these are softer, trend-aware alerts.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from .config import DUCKDB_PATH, RESULTS_DIR

T = dict(volume_pct=0.20, hourly_drop=0.50, block_warn=0.02, block_crit=0.05, segment_reject=0.10,
         segment_min_n=20, drift_min_n=10, late_share=0.10, dup_rate=0.01, utm_min=0.80, identify_min=0.99)


def _fmt_when(w):
    if w is None:
        return "whole window"
    if isinstance(w, (pd.Timestamp,)) or hasattr(w, "strftime"):
        return w.strftime("%Y-%m-%d %H:%M") if getattr(w, "hour", 0) or getattr(w, "minute", 0) else w.strftime("%Y-%m-%d")
    return str(w).replace(" 00:00:00", "")


def _alert(alerts, check, severity, when, metric, value, threshold, message):
    alerts.append(dict(check=check, severity=severity, when=_fmt_when(when), metric=metric,
                       value=None if value is None else round(float(value), 4),
                       threshold=threshold, message=message))


def run(db: Path = DUCKDB_PATH, out: Path = RESULTS_DIR) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db), read_only=True)
    q = lambda sql: con.execute(sql).df()
    alerts: list[dict] = []

    # ---------------- daily metrics (by received day) ----------------
    daily = q("""
      with acc as (select cast(received_at as date) d, count(*) accepted,
                          count(*) filter (where date_diff('second', event_ts, received_at) > 3600) late_1h,
                          count(*) - count(distinct message_id) dup_copies
                   from raw.events group by 1),
           qua as (select cast(received_at as date) d, count(*) quarantined
                   from raw.events_quarantine where received_at is not null group by 1),
           warn as (select cast(received_at as date) d, count(distinct message_id) warned
                    from raw.schema_violations where severity = 'warn' group by 1)
      select acc.d as received_date, accepted + coalesce(quarantined, 0) as messages, accepted,
             coalesce(quarantined, 0) as quarantined, coalesce(warned, 0) as warned, late_1h, dup_copies
      from acc left join qua using (d) left join warn using (d) order by 1""")
    daily["block_rate"] = daily.quarantined / daily.messages
    daily["warn_rate"] = daily.warned / daily.messages
    daily["late_share"] = daily.late_1h / daily.accepted
    # baseline = median of the previous 7 days of the same day type (weekday vs weekend)
    daily["is_weekend"] = pd.to_datetime(daily.received_date).dt.day_of_week >= 5
    daily["trailing7_median"] = daily.groupby("is_weekend").messages.transform(
        lambda s: s.shift(1).rolling(7, min_periods=2).median())
    daily["volume_dev"] = (daily.messages - daily.trailing7_median) / daily.trailing7_median

    for r in daily.itertuples():
        if pd.notna(r.volume_dev) and abs(r.volume_dev) > T["volume_pct"]:
            _alert(alerts, "volume", "warning", r.received_date, "daily_messages_vs_trailing7_median",
                   r.volume_dev, T["volume_pct"],
                   f"{r.messages:,} messages vs trailing median {r.trailing7_median:,.0f} ({r.volume_dev:+.0%})")
        if r.block_rate > T["block_warn"]:
            sev = "critical" if r.block_rate > T["block_crit"] else "warning"
            _alert(alerts, "validity", sev, r.received_date, "daily_quarantine_rate", r.block_rate,
                   T["block_warn"], f"{r.quarantined} of {r.messages:,} messages quarantined ({r.block_rate:.2%})")
        if r.late_share > T["late_share"]:
            _alert(alerts, "timeliness", "warning", r.received_date, "share_arriving_over_1h_late",
                   r.late_share, T["late_share"], f"{r.late_share:.1%} of events arrived > 1h after they happened")

    # ---------------- hourly outage detection per SDK ----------------
    hourly = q("""
      select library_name, date_trunc('hour', received_at) h, count(*) n
      from raw.events group by all order by 1, 2""")
    for lib, g in hourly.groupby("library_name"):
        g = g.set_index("h")["n"].asfreq("h", fill_value=0).to_frame("n")
        g["hour"] = g.index.hour
        g["base"] = g.groupby("hour")["n"].transform(lambda s: s.shift(1).rolling(7, min_periods=3).median())
        g["drop"] = 1 - g.n / g.base
        bad = g[(g["drop"] > T["hourly_drop"]) & (g.base >= 30)]
        if len(bad):
            # group consecutive hours into incidents, keep those lasting >= 2h
            runs = (bad.index.to_series().diff() != pd.Timedelta("1h")).cumsum()
            for _, inc in bad.groupby(runs.values):
                if len(inc) >= 3:
                    lost = (inc.base - inc.n).sum()
                    _alert(alerts, "volume", "critical", f"{inc.index[0]:%Y-%m-%d %H:%M} to {inc.index[-1] + pd.Timedelta('1h'):%H:%M} UTC",
                           f"hourly_{lib}_drop_vs_same_hour_median", inc["drop"].mean(), T["hourly_drop"],
                           f"{lib}: volume down {inc['drop'].mean():.0%} for {len(inc)}h (~{lost:,.0f} events missing) - likely SDK/collector outage")

    # ---------------- rejection spikes by event x app version ----------------
    seg = q("""
      with r as (select cast(received_at as date) d, event_name,
                        coalesce(json_extract_string(payload, '$.context.app.version'),
                                 json_extract_string(payload, '$.context.library.name')) ver,
                        count(*) rej
                 from raw.events_quarantine where received_at is not null and json_valid(payload)
                 group by all),
           a as (select cast(received_at as date) d, event_name, coalesce(app_version, library_name) ver, count(*) n
                 from raw.events group by all)
      select coalesce(a.d, r.d) d, coalesce(a.event_name, r.event_name) event_name, coalesce(a.ver, r.ver) ver,
             coalesce(n, 0) + coalesce(rej, 0) total, coalesce(rej, 0) rejected
      from a full join r
        on a.d = r.d and a.event_name = r.event_name and a.ver = r.ver""")
    seg["rate"] = seg.rejected / seg.total
    spikes = seg[(seg.rate > T["segment_reject"]) & (seg.total >= 5)]
    for (ev, ver), g in spikes.groupby(["event_name", "ver"]):
        if g.total.sum() < T["segment_min_n"] or g.rejected.sum() < 10:
            continue
        _alert(alerts, "validity", "critical", f"{g.d.min()} to {g.d.max()}", f"rejection_rate[{ev} @ {ver}]",
               g.rejected.sum() / g.total.sum(), T["segment_reject"],
               f"'{ev}' from {ver}: {g.rejected.sum()} of {g.total.sum()} rejected across {len(g)} day(s)")

    # ---------------- schema drift ----------------
    drift = q("""
      select code, path, event_name, coalesce(app_version, library_name) ver,
             min(received_at) first_seen, max(received_at) last_seen, count(*) n, any_value(detail) example
      from raw.schema_violations
      where code in ('unexpected_property', 'wrong_type', 'unplanned_event', 'naming_convention')
      group by all order by first_seen""")
    for r in drift.itertuples():
        if r.code == "unexpected_property" and r.n >= T["drift_min_n"]:
            _alert(alerts, "schema_drift", "warning", r.first_seen, f"unplanned_property[{r.path}]", r.n, T["drift_min_n"],
                   f"'{r.event_name}' from {r.ver} sends unplanned '{r.path}' ({r.n}x since {r.first_seen:%Y-%m-%d %H:%M}); "
                   "add it to the tracking plan or fix the client")
        if r.code == "wrong_type" and r.n >= T["drift_min_n"]:
            _alert(alerts, "schema_drift", "critical", r.first_seen, f"type_change[{r.path}]", r.n, T["drift_min_n"],
                   f"'{r.event_name}' from {r.ver}: {r.path} has the wrong type ({r.n}x, e.g. {r.example})")
    names = drift[drift.code.isin(["unplanned_event", "naming_convention"])].groupby("event_name").n.sum()
    names = names[names >= 3].sort_values(ascending=False)
    if len(names):
        _alert(alerts, "schema_drift", "warning", None, "unplanned_event_names", int(names.sum()), 3,
               f"{len(names)} event names not in the tracking plan (quarantined): "
               + ", ".join(f"'{k}' ({v}x)" for k, v in names.items()))

    # ---------------- completeness ----------------
    comp = q("""
      select
        (select count(*) filter (where utm_source is not null) / count(*) from staging.stg_segment__pages
          where page_name = 'Home') as home_utm_coverage,
        (select count(*) filter (where i.user_id is not null) / count(*)
           from staging.stg_segment__tracks t
           left join (select distinct user_id from staging.stg_segment__identifies) i using (user_id)
          where t.event_name = 'Account Created') as identify_coverage,
        (select count(*) filter (where device_type is null or event_ts is null or person_id is null) / count(*)
           from intermediate.int_sessionized) as key_null_rate,
        (select count(*) filter (where equity_requested is null) / nullif(count(*), 0) from staging.stg_segment__tracks
          where event_name in ('Estimate Completed', 'Application Submitted')) as equity_null_rate
    """).iloc[0].to_dict()
    if comp["home_utm_coverage"] < T["utm_min"]:
        _alert(alerts, "completeness", "warning", None, "home_utm_coverage", comp["home_utm_coverage"], T["utm_min"],
               "landing page views missing campaign attribution")
    if comp["identify_coverage"] < T["identify_min"]:
        _alert(alerts, "completeness", "warning", None, "identify_after_signup", comp["identify_coverage"],
               T["identify_min"], f"{1 - comp['identify_coverage']:.1%} of Account Created have no identify")
    if comp["key_null_rate"] > 0 or (comp["equity_null_rate"] or 0) > 0:
        _alert(alerts, "completeness", "critical", None, "required_field_nulls",
               max(comp["key_null_rate"], comp["equity_null_rate"] or 0), 0, "required fields are null after validation")

    # ---------------- uniqueness + privacy ----------------
    dup = q("select count(*) total, count(*) - count(distinct message_id) dups from raw.events").iloc[0]
    dup_rate = dup.dups / dup.total
    if dup_rate > T["dup_rate"]:
        _alert(alerts, "uniqueness", "warning", None, "raw_duplicate_rate", dup_rate, T["dup_rate"],
               f"{dup.dups} duplicate deliveries in raw.events")
    pii = q("select path, count(*) n, min(received_at) first_seen from raw.schema_violations where code='pii_detected' group by 1")
    for r in pii.itertuples():
        _alert(alerts, "privacy", "critical", r.first_seen, f"pii[{r.path}]", r.n, 0,
               f"PII-like key '{r.path}' sent {r.n}x; events quarantined, not loaded")

    # ---------------- summary ----------------
    viol_by_code = q("select code, severity, count(*) n from raw.schema_violations group by all order by n desc")
    tot = q("""select (select count(*) from raw.events) accepted,
                      (select count(*) from raw.events_quarantine) quarantined,
                      (select count(distinct message_id) from raw.schema_violations where severity='warn') warned""").iloc[0]
    msgs = int(tot.accepted + tot.quarantined)
    summary = dict(
        messages_received=msgs, accepted=int(tot.accepted), quarantined=int(tot.quarantined),
        quarantine_rate=round(tot.quarantined / msgs, 4), warned_accepted=int(tot.warned),
        raw_duplicate_rate=round(float(dup_rate), 4),
        late_over_1h_share=round(float(daily.late_1h.sum() / daily.accepted.sum()), 4),
        completeness={k: round(float(v), 4) if v is not None else None for k, v in comp.items()},
        violations_by_code=viol_by_code.to_dict("records"),
        alerts_by_check=pd.Series([a["check"] for a in alerts]).value_counts().to_dict(),
        alerts_by_severity=pd.Series([a["severity"] for a in alerts]).value_counts().to_dict(),
        thresholds=T,
    )
    con.close()

    daily.round({c: 4 for c in ["block_rate", "warn_rate", "late_share", "volume_dev"]}).to_csv(out / "dq_daily_metrics.csv", index=False)
    (out / "dq_alerts.json").write_text(json.dumps(dict(summary=summary, alerts=alerts), indent=2, default=str))
    _report(out / "dq_report.md", summary, alerts, daily)
    print(f"DQ: {len(alerts)} alerts {summary['alerts_by_severity']}; quarantine rate {summary['quarantine_rate']:.2%}")
    return dict(summary=summary, alerts=alerts)


def _report(path: Path, s: dict, alerts: list[dict], daily: pd.DataFrame):
    L = ["# Data-quality report (synthetic stream)", "",
         f"- Messages received: **{s['messages_received']:,}**; accepted {s['accepted']:,}; "
         f"quarantined {s['quarantined']:,} (**{s['quarantine_rate']:.2%}**); accepted-with-warning {s['warned_accepted']:,}",
         f"- Raw duplicate deliveries: {s['raw_duplicate_rate']:.2%} (removed in dbt staging)",
         f"- Events arriving > 1h late: {s['late_over_1h_share']:.2%}",
         f"- Completeness: " + ", ".join(f"{k} = {v}" for k, v in s["completeness"].items()), "",
         "## Violations by code", "", "| code | severity | count |", "|---|---|---|"]
    L += [f"| {v['code']} | {v['severity']} | {v['n']:,} |" for v in s["violations_by_code"]]
    L += ["", f"## Alerts ({len(alerts)})", "", "| severity | check | when | message |", "|---|---|---|---|"]
    order = {"critical": 0, "warning": 1}
    for a in sorted(alerts, key=lambda a: (order[a["severity"]], a["check"], a["when"])):
        L.append(f"| {a['severity']} | {a['check']} | {a['when']} | {a['message']} |")
    path.write_text("\n".join(L) + "\n")


if __name__ == "__main__":
    run()
