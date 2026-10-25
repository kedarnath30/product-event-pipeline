"""Tracking-plan validator: accepted vs quarantined vs warn-only."""
import copy
from datetime import datetime, timezone

import pytest

from src.validator import planned_events, validate

RECEIVED = datetime(2026, 9, 10, 12, 0, 5, tzinfo=timezone.utc)
BASE = {
    "messageId": "0b6f5b62-6e43-4a43-9f0e-3c3b7c1d9a10",
    "type": "track",
    "event": "Offer Accepted",
    "anonymousId": "anon_14186637ad7a463c",
    "userId": "usr_ab12cd34ef",
    "timestamp": "2026-09-10T12:00:00.000Z",
    "sentAt": "2026-09-10T12:00:01.000Z",
    "context": {"library": {"name": "analytics-ios", "version": "4.1.6"},
                "device": {"type": "ios"}, "app": {"version": "3.3.0"}},
    "properties": {"application_id": "app_0123456789", "offer_amount": 85000},
}


def msg(**changes):
    m = copy.deepcopy(BASE)
    for k, v in changes.items():
        if v is None and k in m and k not in ("anonymousId", "userId"):
            m.pop(k)
        else:
            m[k] = v
    return m


def test_planned_events_are_the_seven_funnel_steps():
    assert planned_events() == {"Estimate Started", "Estimate Completed", "Account Created",
                                "Application Submitted", "Documents Uploaded", "Offer Viewed", "Offer Accepted"}


def test_valid_message_is_accepted_without_violations():
    r = validate(msg(), RECEIVED)
    assert r.accepted and r.violations == []


@pytest.mark.parametrize("mutate, code", [
    (lambda m: m["properties"].pop("offer_amount"), "missing_required"),
    (lambda m: m["properties"].__setitem__("offer_amount", "N/A"), "wrong_type"),
    (lambda m: m["context"]["device"].__setitem__("type", "tablet"), "invalid_enum"),
    (lambda m: m.__setitem__("event", "clicked_offer"), "naming_convention"),
    (lambda m: m.__setitem__("event", "Loan Funded"), "unplanned_event"),
    (lambda m: m.update(anonymousId=None, userId=None), "missing_identity"),
    (lambda m: m.__setitem__("timestamp", "yesterday"), "invalid_timestamp"),
    (lambda m: m.__setitem__("timestamp", "2026-09-11T12:00:00.000Z"), "future_timestamp"),
    (lambda m: m["properties"].__setitem__("email", "a@b.com"), "pii_detected"),
])
def test_blocking_violations_quarantine_the_message(mutate, code):
    m = msg()
    mutate(m)
    r = validate(m, RECEIVED)
    assert not r.accepted
    assert code in r.codes
    assert all(v.severity in ("block", "warn") for v in r.violations)


def test_pii_is_reported_as_pii_not_as_drift():
    m = msg()
    m["properties"]["phone_number"] = "555"
    r = validate(m, RECEIVED)
    assert "pii_detected" in r.codes and "unexpected_property" not in r.codes


def test_unplanned_property_is_warn_only_and_still_accepted():
    m = msg()
    m["properties"]["credit_score_band"] = "good"
    r = validate(m, RECEIVED)
    assert r.accepted
    assert [(v.code, v.severity) for v in r.violations] == [("unexpected_property", "warn")]


def test_small_clock_skew_is_tolerated():
    r = validate(msg(timestamp="2026-09-10T12:30:00.000Z", sentAt="2026-09-10T12:30:01.000Z"), RECEIVED)
    assert r.accepted


def test_non_object_is_unparseable():
    r = validate(["not", "a", "dict"])
    assert not r.accepted and r.codes == ["unparseable"]
