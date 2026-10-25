"""Tracking-plan validator (Segment Protocols-style).

Every incoming message is checked against the JSON Schema for its type/event,
plus a few rules JSON Schema can't express well (naming convention, PII,
timestamp sanity). Each problem becomes a violation with a stable `code` and a
`severity`:

* block -> the event is quarantined (never reaches raw.events)
* warn  -> the event is accepted but the violation is logged (schema drift we
           want to see, e.g. an unplanned property a new app release added)
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from .config import SCHEMA_DIR

OBJECT_ACTION = re.compile(r"^[A-Z][a-z]+( [A-Z][a-z]+)* [A-Z][a-z]+$")  # "Offer Accepted"
PII_KEYS = re.compile(r"(^|_)(email|phone|ssn|first_name|last_name|full_name|address|dob)($|_)", re.I)
MAX_CLOCK_SKEW = timedelta(hours=1)

# jsonschema keyword -> violation code
KEYWORD_CODES = {
    "required": "missing_required",
    "type": "wrong_type",
    "enum": "invalid_enum",
    "const": "invalid_enum",
    "additionalProperties": "unexpected_property",
    "pattern": "invalid_format",
    "minimum": "out_of_range", "maximum": "out_of_range",
    "minItems": "out_of_range", "maxItems": "out_of_range", "minLength": "invalid_format",
    "anyOf": "missing_identity",
}
WARN_CODES = {"unexpected_property"}  # accepted but logged as drift


@dataclass
class Violation:
    code: str
    severity: str
    path: str
    detail: str


@dataclass
class Result:
    accepted: bool
    violations: list[Violation] = field(default_factory=list)

    @property
    def codes(self) -> list[str]:
        return sorted({v.code for v in self.violations})


def _slug(event: str) -> str:
    return event.lower().replace(" ", "_")


@lru_cache(maxsize=1)
def _registry_and_validators() -> tuple[dict[str, Draft202012Validator], set[str]]:
    resources = []
    schemas: dict[str, dict] = {}
    for p in sorted(Path(SCHEMA_DIR).rglob("*.json")):
        s = json.loads(p.read_text())
        resources.append((s["$id"], Resource.from_contents(s)))
        schemas[p.relative_to(SCHEMA_DIR).as_posix()] = s
    registry = Registry().with_resources(resources)
    validators = {k: Draft202012Validator(v, registry=registry) for k, v in schemas.items()}
    planned = {s["title"] for k, s in schemas.items() if k.startswith("track/")}
    return validators, planned


def planned_events() -> set[str]:
    return _registry_and_validators()[1]


def _parse_ts(v: Any) -> datetime | None:
    if not isinstance(v, str):
        return None
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _find_pii(obj: Any, path: str = "") -> list[str]:
    hits = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if PII_KEYS.search(k) and k not in {"email_domain"}:
                hits.append(f"{path}.{k}".lstrip("."))
            hits += _find_pii(v, f"{path}.{k}")
    return hits


def validate(msg: dict, received_at: datetime | None = None) -> Result:
    validators, planned = _registry_and_validators()
    vs: list[Violation] = []

    def add(code: str, path: str, detail: str):
        vs.append(Violation(code, "warn" if code in WARN_CODES else "block", path, detail[:300]))

    if not isinstance(msg, dict):
        add("unparseable", "$", "message is not a JSON object")
        return Result(False, vs)

    mtype = msg.get("type")
    if mtype == "track":
        event = msg.get("event")
        if not isinstance(event, str):
            add("missing_required", "event", "track call without event name")
            key = "common/envelope.json"
        elif event not in planned:
            code = "naming_convention" if not OBJECT_ACTION.match(event) else "unplanned_event"
            add(code, "event", f"'{event}' is not in the tracking plan")
            key = "common/envelope.json"
        else:
            key = f"track/{_slug(event)}.json"
    elif mtype in ("identify", "page"):
        key = f"{mtype}.json"
    else:
        add("invalid_enum", "type", f"unknown message type {mtype!r}")
        key = "common/envelope.json"

    seen = set()
    for err in validators[key].iter_errors(msg):
        # descend into allOf/anyOf wrappers to report the real keyword
        leaves = [err]
        if err.validator == "allOf" and err.context:
            leaves = err.context
        for e in leaves:
            code = KEYWORD_CODES.get(e.validator, "schema_violation")
            path = ".".join(str(p) for p in e.absolute_path) or "$"
            if code == "unexpected_property":
                extra = re.findall(r"'([^']+)'", e.message)
                for x in extra or ["?"]:
                    k = (code, f"{path}.{x}")
                    if k not in seen:
                        seen.add(k); add(code, f"{path}.{x}", e.message)
                continue
            if (code, path) in seen:
                continue
            seen.add((code, path))
            detail = "neither anonymousId nor userId is set" if code == "missing_identity" else e.message
            add(code, path, detail)

    for p in _find_pii(msg.get("traits", {}), "traits") + _find_pii(msg.get("properties", {}), "properties"):
        add("pii_detected", p, "PII-like key not allowed in analytics events")

    ts = _parse_ts(msg.get("timestamp"))
    if ts is None:
        add("invalid_timestamp", "timestamp", f"unparseable timestamp {msg.get('timestamp')!r}")
    elif received_at is not None and ts > received_at + MAX_CLOCK_SKEW:
        add("future_timestamp", "timestamp", f"{ts.isoformat()} is after receivedAt {received_at.isoformat()}")

    # pii found in additionalProperties is reported as pii, not as drift
    pii_paths = {v.path for v in vs if v.code == "pii_detected"}
    vs = [v for v in vs if not (v.code == "unexpected_property" and v.path in pii_paths)]
    accepted = not any(v.severity == "block" for v in vs)
    return Result(accepted, vs)
