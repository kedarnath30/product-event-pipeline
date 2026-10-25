"""End-to-end orchestrator for the product-event pipeline (local, free, reproducible).

Replays the 30-day synthetic stream the way a production schedule would see it:

  1. generate   synthetic Segment-style messages -> hourly NDJSON landing queue
  2. ingest     micro-batches received before the cut-over hour (default 2026-09-29T00)
  3. dbt build  full build + all tests, with the stream clock set to the cut-over
  4. ingest     the rest of the queue: newer events *and* late mobile events
                (event time before the cut-over, received after it)
  5. dbt build  incremental: staging appends only newly loaded messages, marts rebuild
  6. dq         trend-aware data-quality monitor -> results/dq_*
  7. sql        analysis queries in sql/ -> results/sql_*.csv
  8. figures    PNG charts -> figures/

Writes results/pipeline_run.json with timings and headline numbers.

  python -m src.pipeline                 # full run from scratch
  python -m src.pipeline --skip-generate # reuse the existing landing queue
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import dq_monitor, figures, generator, ingester, run_sql
from .config import DBT_DIR, DUCKDB_PATH, LANDING_DIR, RESULTS_DIR, STREAM_END

CUTOVER = "2026-09-29T00"


def _dbt_bin() -> str:
    cand = Path(sys.executable).parent / "dbt"
    return str(cand) if cand.exists() else (shutil.which("dbt") or "dbt")


def dbt_build(as_of: str, db: Path, full_refresh: bool = False) -> dict:
    """Run `dbt build` with the pipeline clock; return a summary parsed from run_results.json."""
    cmd = [_dbt_bin(), "build", "--profiles-dir", ".", "--vars", json.dumps({"pipeline_as_of": as_of})]
    if full_refresh:
        cmd.append("--full-refresh")
    env = {**os.environ, "PEP_DUCKDB_PATH": str(db.resolve())}
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=DBT_DIR, env=env, capture_output=True, text=True)
    secs = round(time.perf_counter() - t0, 1)
    rr_path = DBT_DIR / "target" / "run_results.json"
    summary = dict(exit_code=proc.returncode, seconds=secs, pipeline_as_of=as_of)
    if rr_path.exists():
        rr = json.loads(rr_path.read_text())["results"]
        tests = [r for r in rr if r["unique_id"].startswith("test.")]
        unit = [r for r in rr if r["unique_id"].startswith("unit_test.")]
        models = [r for r in rr if r["unique_id"].startswith("model.")]
        summary.update(
            models=len(models), models_ok=sum(r["status"] == "success" for r in models),
            tests=len(tests), tests_pass=sum(r["status"] == "pass" for r in tests),
            tests_warn=sum(r["status"] == "warn" for r in tests),
            tests_fail=sum(r["status"] in ("fail", "error") for r in tests),
            unit_tests=len(unit), unit_tests_pass=sum(r["status"] == "pass" for r in unit),
            nodes=len(rr), nodes_ok=sum(r["status"] in ("success", "pass") for r in rr),
            failures=[r["unique_id"] for r in rr if r["status"] in ("fail", "error")],
        )
    if proc.returncode != 0:
        print(proc.stdout[-4000:], proc.stderr[-2000:], sep="\n")
        raise RuntimeError(f"dbt build failed (as_of={as_of}): {summary.get('failures')}")
    print(f"dbt build ok (as_of={as_of}): {summary.get('models')} models, "
          f"{summary.get('tests_pass')}/{summary.get('tests')} data tests + "
          f"{summary.get('unit_tests_pass')}/{summary.get('unit_tests')} unit tests pass, {secs}s")
    return summary


def _scalar(db: Path, sql: str):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()


def _headline(db: Path) -> dict:
    con = duckdb.connect(str(db), read_only=True)
    q = lambda s: con.execute(s).fetchone()
    try:
        funnel = con.execute("""select step_order, step_name, users, conversion_from_start
                                from marts.mart_funnel_conversion where segment='all'
                                order by step_order""").fetchall()
        h = dict(
            raw_events=q("select count(*) from raw.events")[0],
            quarantined=q("select count(*) from raw.events_quarantine")[0],
            staged_events_deduped=q("select count(*) from staging.stg_segment__events")[0],
            users=q("select count(*) from marts.dim_users")[0],
            sessions=q("select count(*) from marts.fct_sessions")[0],
            funnel=[dict(step_order=s, step=n, users=u, conversion_from_start=c) for s, n, u, c in funnel],
        )
    finally:
        con.close()
    return h


def run(skip_generate: bool = False, cutover: str = CUTOVER, db: Path = DUCKDB_PATH,
        landing: Path = LANDING_DIR) -> dict:
    t_all = time.perf_counter()
    report: dict = dict(started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        cutover=cutover, stream_end=STREAM_END, steps={})
    steps = report["steps"]

    def timed(name, fn, *a, **kw):
        t0 = time.perf_counter()
        out = fn(*a, **kw)
        print(f"[{name}] {time.perf_counter() - t0:.1f}s")
        extra = {k: v for k, v in (out if isinstance(out, dict) else {}).items() if k != "seconds"}
        steps[name] = dict(seconds=round(time.perf_counter() - t0, 1), **extra)
        return out

    # 1. generate
    if skip_generate and any(landing.glob("dt=*/hour=*/*.ndjson")):
        steps["generate"] = dict(skipped=True)
    else:
        timed("generate", generator.main, ["--landing", str(landing)])

    # fresh warehouse every full run -> deterministic numbers
    for p in (db, db.with_suffix(db.suffix + ".wal")):
        if p.exists():
            p.unlink()

    # 2-3. first window
    timed("ingest_until_cutover", ingester.run, landing, db, cutover, verbose=False)
    as_of_1 = cutover.replace("T", " ") + ":00:00"
    steps["dbt_build_initial"] = dbt_build(as_of_1, db, full_refresh=True)
    staged_before = _scalar(db, "select count(*) from staging.stg_segment__events")
    last_loaded = _scalar(db, "select max(loaded_at) from raw.events")

    # 4-5. rest of the stream, incl. late arrivals, then incremental build
    timed("ingest_remaining", ingester.run, landing, db, None, verbose=False)
    late = _scalar(db, f"""select count(*) from raw.events
                           where loaded_at > timestamp '{last_loaded}'
                             and event_ts < timestamp '{as_of_1}'""")
    as_of_2 = STREAM_END.replace("T", " ")
    steps["dbt_build_incremental"] = dbt_build(as_of_2, db)
    staged_after = _scalar(db, "select count(*) from staging.stg_segment__events")
    late_staged = _scalar(db, f"""select count(*) from staging.stg_segment__events
                                  where loaded_at > timestamp '{last_loaded}'
                                    and event_ts < timestamp '{as_of_1}'""")
    steps["dbt_build_incremental"].update(
        staged_rows_before=staged_before, staged_rows_appended=staged_after - staged_before,
        late_events_loaded_after_cutover=late, late_events_appended_to_staging=late_staged)

    # 6-8. monitoring, analyses, charts
    dq = timed("dq_monitor", dq_monitor.run, db, RESULTS_DIR)
    steps["dq_monitor"] = dict(seconds=steps["dq_monitor"]["seconds"], alerts=len(dq["alerts"]),
                               alerts_by_severity=dq["summary"]["alerts_by_severity"])
    timed("sql_analyses", run_sql.run, db)
    timed("figures", figures.run, db)

    report["headline"] = dict(
        _headline(db),
        quarantine_rate=dq["summary"]["quarantine_rate"],
        raw_duplicate_rate=dq["summary"]["raw_duplicate_rate"],
        late_over_1h_share=dq["summary"]["late_over_1h_share"],
        dq_alerts=len(dq["alerts"]),
    )
    report["total_seconds"] = round(time.perf_counter() - t_all, 1)
    report["note"] = "All data is synthetic (seeded generator); numbers describe the simulation, not a real product."
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "pipeline_run.json").write_text(json.dumps(report, indent=2, default=str))
    print(f"pipeline done in {report['total_seconds']}s -> results/pipeline_run.json")
    return report


def main(argv=None):
    ap = argparse.ArgumentParser(description="Run the full event pipeline end to end")
    ap.add_argument("--skip-generate", action="store_true", help="reuse the existing landing queue")
    ap.add_argument("--cutover", default=CUTOVER, help="first ingest window ends at this hour (UTC)")
    a = ap.parse_args(argv)
    run(a.skip_generate, a.cutover)


if __name__ == "__main__":
    main()
