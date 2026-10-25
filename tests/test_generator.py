"""Synthetic stream: deterministic, right-censored at the stream clock, mostly valid."""
import json
from datetime import datetime, timedelta, timezone

from src.generator import Sim, write_landing
from src.validator import validate

START = datetime(2026, 9, 15, tzinfo=timezone.utc)
END = START + timedelta(days=2)


def _run(seed=7):
    return Sim(seed, daily_visitors=60).run(START, 2, END)


def test_same_seed_same_stream():
    a, b = _run(), _run()
    assert [m["messageId"] for m in a] == [m["messageId"] for m in b]
    assert [m["messageId"] for m in _run(seed=8)] != [m["messageId"] for m in a]


def test_nothing_received_after_stream_end():
    end = END.strftime("%Y-%m-%dT%H:%M:%S")
    assert all(m["receivedAt"] < end for m in _run())


def test_message_mix_and_validity():
    msgs = _run()
    types = {m["type"] for m in msgs}
    assert types == {"track", "identify", "page"}
    ok = 0
    for m in msgs:
        recv = datetime.fromisoformat(m["receivedAt"].replace("Z", "+00:00"))
        ok += validate(m, recv).accepted
    rate = ok / len(msgs)
    assert 0.95 < rate < 1.0, rate  # mostly valid, but the generator injects some mess


def test_write_landing_partitions_by_received_hour(tmp_path):
    msgs = _run()
    n_files = write_landing(msgs, tmp_path)
    files = sorted(tmp_path.glob("dt=*/hour=*/events.ndjson"))
    assert len(files) == n_files > 24
    total = 0
    for f in files:
        hour = f"{f.parent.parent.name[3:]}T{f.parent.name[5:]}"
        rows = [json.loads(line) for line in f.read_text().splitlines()]
        assert all(r["receivedAt"][:13] == hour for r in rows)
        assert [r["receivedAt"] for r in rows] == sorted(r["receivedAt"] for r in rows)
        total += len(rows)
    assert total == len(msgs)
