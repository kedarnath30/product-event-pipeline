"""Micro-batch ingester: file queue -> validate -> DuckDB raw layer.

Each hourly NDJSON file in the landing queue is one micro-batch, committed in a
single DuckDB transaction together with its checkpoint row, so a file is
loaded exactly once even if the ingester crashes or is re-run (idempotent).
The raw layer itself is at-least-once (SDK retries create duplicates); dbt
staging de-duplicates on messageId.

Tables (schema `raw`):
  events             accepted messages (typed envelope columns + full JSON payload)
  events_quarantine  rejected messages with their violation codes
  schema_violations  one row per violation (block + warn), feeds DQ monitoring
  ingest_batches     per-batch metrics (counts, lateness, throughput)
  ingest_checkpoint  processed files (the exactly-once ledger)

Modes:
  python -m src.ingester                 # drain everything pending, then exit
  python -m src.ingester --until 2026-09-29T00   # only files received before that hour
  python -m src.ingester --follow        # keep polling the queue (near-real-time)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from .config import DUCKDB_PATH, LANDING_DIR
from .validator import validate

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.events (
    message_id VARCHAR, type VARCHAR, event_name VARCHAR,
    anonymous_id VARCHAR, user_id VARCHAR,
    event_ts TIMESTAMP, sent_at TIMESTAMP, received_at TIMESTAMP, loaded_at TIMESTAMP,
    library_name VARCHAR, app_version VARCHAR, device_type VARCHAR,
    batch_id INTEGER, source_file VARCHAR, payload JSON);
CREATE TABLE IF NOT EXISTS raw.events_quarantine (
    message_id VARCHAR, type VARCHAR, event_name VARCHAR, received_at TIMESTAMP, loaded_at TIMESTAMP,
    batch_id INTEGER, source_file VARCHAR, violation_codes VARCHAR[], payload VARCHAR);
CREATE TABLE IF NOT EXISTS raw.schema_violations (
    message_id VARCHAR, batch_id INTEGER, received_at TIMESTAMP, event_name VARCHAR,
    library_name VARCHAR, app_version VARCHAR,
    code VARCHAR, severity VARCHAR, path VARCHAR, detail VARCHAR);
CREATE TABLE IF NOT EXISTS raw.ingest_batches (
    batch_id INTEGER, source_file VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP,
    n_messages INTEGER, n_accepted INTEGER, n_quarantined INTEGER, n_warned INTEGER,
    min_received_at TIMESTAMP, max_received_at TIMESTAMP,
    lateness_p50_s DOUBLE, lateness_p95_s DOUBLE, lateness_max_s DOUBLE,
    n_late_over_1h INTEGER, seconds DOUBLE);
CREATE TABLE IF NOT EXISTS raw.ingest_checkpoint (
    source_file VARCHAR PRIMARY KEY, sha256 VARCHAR, batch_id INTEGER, processed_at TIMESTAMP);
"""


def _ts(v):
    if not isinstance(v, str) or not v:
        return None
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def connect(path: Path = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(DDL)
    return con


def pending_files(con, landing: Path, until: str | None = None) -> list[Path]:
    done = {r[0] for r in con.execute("select source_file from raw.ingest_checkpoint").fetchall()}
    files = []
    for p in sorted(landing.glob("dt=*/hour=*/*.ndjson")):
        rel = p.relative_to(landing).as_posix()
        if rel in done:
            continue
        if until:
            hour_key = f"{p.parent.parent.name[3:]}T{p.parent.name[5:]}"
            if hour_key >= until:
                continue
        files.append(p)
    return files


def ingest_file(con, path: Path, landing: Path) -> dict:
    t0 = time.perf_counter()
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    rel = path.relative_to(landing).as_posix()
    raw = path.read_bytes()
    batch_id = con.execute("select coalesce(max(batch_id), 0) + 1 from raw.ingest_batches").fetchone()[0]
    acc, quar, viol, lateness = [], [], [], []
    for line in raw.decode().splitlines():
        if not line.strip():
            continue
        try:
            m = json.loads(line)
        except json.JSONDecodeError:
            quar.append(dict(message_id=None, type=None, event_name=None, received_at=None,
                             violation_codes=["unparseable"], payload=line[:2000]))
            continue
        recv = _ts(m.get("receivedAt"))
        recv_aware = recv.replace(tzinfo=timezone.utc) if recv else None
        res = validate(m, recv_aware)
        ctx = m.get("context") or {}
        name = m.get("event") if m.get("type") == "track" else m.get("name")
        lib = (ctx.get("library") or {}).get("name")
        appv = (ctx.get("app") or {}).get("version")
        for v in res.violations:
            viol.append(dict(message_id=m.get("messageId"), batch_id=batch_id, received_at=recv, event_name=name,
                             library_name=lib, app_version=appv, code=v.code, severity=v.severity,
                             path=v.path, detail=v.detail))
        if res.accepted:
            ets = _ts(m["timestamp"])
            if recv and ets:
                lateness.append((recv - ets).total_seconds())
            acc.append(dict(message_id=m["messageId"], type=m["type"], event_name=name,
                            anonymous_id=m.get("anonymousId"), user_id=m.get("userId"),
                            event_ts=ets, sent_at=_ts(m.get("sentAt")), received_at=recv,
                            library_name=lib, app_version=appv,
                            device_type=(ctx.get("device") or {}).get("type"),
                            payload=json.dumps(m, separators=(",", ":"))))
        else:
            quar.append(dict(message_id=m.get("messageId"), type=m.get("type"), event_name=name,
                             received_at=recv, violation_codes=res.codes, payload=line))

    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    lat = pd.Series(lateness, dtype="float64")
    recvs = [r["received_at"] for r in acc + quar if r["received_at"] is not None]
    con.execute("BEGIN TRANSACTION")
    try:
        if acc:
            df = pd.DataFrame(acc).assign(loaded_at=loaded_at, batch_id=batch_id, source_file=rel)
            con.register("acc_df", df)
            con.execute("""insert into raw.events select message_id, type, event_name, anonymous_id, user_id,
                event_ts, sent_at, received_at, loaded_at, library_name, app_version, device_type,
                batch_id, source_file, payload::JSON from acc_df""")
            con.unregister("acc_df")
        if quar:
            df = pd.DataFrame(quar).assign(loaded_at=loaded_at, batch_id=batch_id, source_file=rel)
            con.register("q_df", df)
            con.execute("""insert into raw.events_quarantine select message_id, type, event_name, received_at,
                loaded_at, batch_id, source_file, violation_codes, payload from q_df""")
            con.unregister("q_df")
        if viol:
            con.register("v_df", pd.DataFrame(viol))
            con.execute("insert into raw.schema_violations select * from v_df")
            con.unregister("v_df")
        secs = time.perf_counter() - t0
        stats = dict(batch_id=batch_id, source_file=rel, started_at=started, finished_at=loaded_at,
                     n_messages=len(acc) + len(quar), n_accepted=len(acc), n_quarantined=len(quar),
                     n_warned=len({v["message_id"] for v in viol if v["severity"] == "warn"}),
                     min_received_at=min(recvs) if recvs else None, max_received_at=max(recvs) if recvs else None,
                     lateness_p50_s=float(lat.quantile(0.5)) if len(lat) else None,
                     lateness_p95_s=float(lat.quantile(0.95)) if len(lat) else None,
                     lateness_max_s=float(lat.max()) if len(lat) else None,
                     n_late_over_1h=int((lat > 3600).sum()), seconds=secs)
        con.register("b_df", pd.DataFrame([stats]))
        con.execute("insert into raw.ingest_batches select * from b_df")
        con.unregister("b_df")
        con.execute("insert into raw.ingest_checkpoint values (?, ?, ?, ?)",
                    [rel, hashlib.sha256(raw).hexdigest(), batch_id, loaded_at])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return stats


def run(landing: Path = LANDING_DIR, db: Path = DUCKDB_PATH, until: str | None = None,
        follow: bool = False, poll_seconds: float = 2.0, max_idle_polls: int | None = None,
        verbose: bool = True) -> dict:
    con = connect(db)
    totals = dict(batches=0, messages=0, accepted=0, quarantined=0, seconds=0.0)
    idle = 0
    try:
        while True:
            files = pending_files(con, landing, until)
            for f in files:
                s = ingest_file(con, f, landing)
                totals["batches"] += 1
                totals["messages"] += s["n_messages"]
                totals["accepted"] += s["n_accepted"]
                totals["quarantined"] += s["n_quarantined"]
                totals["seconds"] += s["seconds"]
                if verbose and (follow or totals["batches"] % 100 == 0):
                    print(f"batch {s['batch_id']:>4} {s['source_file']}: {s['n_accepted']} ok, "
                          f"{s['n_quarantined']} quarantined ({s['seconds']*1000:.0f} ms)")
            if not follow:
                break
            idle = 0 if files else idle + 1
            if max_idle_polls is not None and idle >= max_idle_polls:
                break
            time.sleep(poll_seconds)
    finally:
        con.close()
    totals["events_per_second"] = round(totals["messages"] / totals["seconds"], 1) if totals["seconds"] else None
    if verbose:
        print(json.dumps(totals, default=str))
    return totals


def main(argv=None):
    ap = argparse.ArgumentParser(description="Micro-batch ingester: landing queue -> DuckDB raw")
    ap.add_argument("--landing", type=Path, default=LANDING_DIR)
    ap.add_argument("--db", type=Path, default=DUCKDB_PATH)
    ap.add_argument("--until", help="only ingest files received before this hour, e.g. 2026-09-29T00")
    ap.add_argument("--follow", action="store_true", help="keep polling the landing queue")
    ap.add_argument("--poll-seconds", type=float, default=2.0)
    ap.add_argument("--max-idle-polls", type=int, default=None)
    a = ap.parse_args(argv)
    return run(a.landing, a.db, a.until, a.follow, a.poll_seconds, a.max_idle_polls)


if __name__ == "__main__":
    main()
