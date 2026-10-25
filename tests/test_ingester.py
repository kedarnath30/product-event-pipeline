"""Micro-batch ingester: exactly-once per file, partial + resume == full, bad lines quarantined."""
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from src import ingester
from src.generator import Sim, write_landing

START = datetime(2026, 9, 15, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    path = tmp_path_factory.mktemp("landing")
    msgs = Sim(11, daily_visitors=40).run(START, 2, START + timedelta(days=2))
    write_landing(msgs, path)
    return path, len(msgs)


def counts(db):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return {t: con.execute(f"select count(*) from raw.{t}").fetchone()[0]
                for t in ("events", "events_quarantine", "ingest_batches", "ingest_checkpoint")}
    finally:
        con.close()


def test_rerun_is_idempotent(landing, tmp_path):
    path, n = landing
    db = tmp_path / "w.duckdb"
    first = ingester.run(path, db, verbose=False)
    assert first["messages"] == n and first["batches"] > 0
    before = counts(db)
    second = ingester.run(path, db, verbose=False)
    assert second["batches"] == 0 and second["messages"] == 0
    assert counts(db) == before
    assert before["events"] + before["events_quarantine"] == n
    assert before["ingest_batches"] == before["ingest_checkpoint"] == first["batches"]


def test_partial_then_resume_equals_full(landing, tmp_path):
    path, n = landing
    full, split = tmp_path / "full.duckdb", tmp_path / "split.duckdb"
    ingester.run(path, full, verbose=False)
    a = ingester.run(path, split, until="2026-09-16T00", verbose=False)
    b = ingester.run(path, split, verbose=False)
    assert 0 < a["batches"] and 0 < b["batches"]
    assert a["messages"] + b["messages"] == n
    c_full, c_split = counts(full), counts(split)
    assert c_full["events"] == c_split["events"]
    assert c_full["events_quarantine"] == c_split["events_quarantine"]
    con = duckdb.connect(str(split), read_only=True)
    cut = con.execute("select max(received_at) from raw.events where batch_id <= ?", [a["batches"]]).fetchone()[0]
    con.close()
    assert cut < datetime(2026, 9, 16)


def test_unparseable_line_goes_to_quarantine(tmp_path):
    land = tmp_path / "landing"
    f = land / "dt=2026-09-15" / "hour=00" / "events.ndjson"
    f.parent.mkdir(parents=True)
    f.write_text('{"not valid json\n')
    db = tmp_path / "q.duckdb"
    out = ingester.run(land, db, verbose=False)
    assert out["quarantined"] == 1 and out["accepted"] == 0
    con = duckdb.connect(str(db), read_only=True)
    codes = con.execute("select violation_codes from raw.events_quarantine").fetchone()[0]
    con.close()
    assert codes == ["unparseable"]
