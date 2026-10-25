"""Synthetic event stream for a fictional home-equity fintech ("Keystone Equity").

Simulates what a Segment-style collector would land: track / identify / page
messages from web (analytics.js) and mobile apps (analytics-ios / -android),
with realistic sessions and funnel drop-off, plus the mess real pipelines see:

* malformed events (missing / mistyped / invalid properties, bad names, PII, no identity)
* late arrivals (mobile apps buffering events offline, flushed hours later)
* duplicate deliveries (SDK retries)
* a tracking outage (web CDN incident) and a schema-drift app release
* right-censoring: events received after the stream clock ends are not written yet

Output: newline-delimited JSON files partitioned by *receivedAt* hour, i.e. a
file-based queue (think Firehose -> S3 prefix), under data/landing/.
All data is synthetic. Fixed seed -> reproducible.
"""
from __future__ import annotations

import argparse
import copy
import json
import random
import shutil
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import LANDING_DIR, SEED, SIM_DAYS, SIM_START, STREAM_END

UTC = timezone.utc
STATES = ["AZ", "CA", "CO", "FL", "GA", "IL", "MA", "MI", "NC", "NJ", "NY", "OR", "PA", "TX", "VA", "WA"]
STATE_W = [4, 14, 6, 9, 5, 4, 5, 3, 4, 5, 8, 3, 4, 10, 5, 6]
SOURCES = {  # source: (weight, medium, funnel quality multiplier)
    "google": (0.34, "cpc", 1.00),
    "organic": (0.24, "organic", 1.05),
    "facebook": (0.15, "paid_social", 0.70),
    "partner_lender": (0.10, "referral", 1.45),
    "referral": (0.08, "referral", 1.15),
    "direct": (0.09, None, 1.00),
}
DEVICES = {"desktop": 0.44, "mobile_web": 0.24, "ios": 0.20, "android": 0.12}

# base step-to-step probabilities (multiplied by source/device factors, capped)
P = {
    "Estimate Started": 0.46,
    "Estimate Completed": 0.68,
    "Account Created": 0.44,
    "Application Submitted": 0.62,
    "Documents Uploaded": 0.72,
    "Offer Viewed": 0.82,
    "Offer Accepted": 0.36,
}
DEVICE_MULT = {"desktop": 1.0, "mobile_web": 0.85, "ios": 1.05, "android": 0.95}

OUTAGE = (datetime(2026, 9, 17, 14, tzinfo=UTC), datetime(2026, 9, 17, 21, tzinfo=UTC), 0.85)  # web events lost
IOS_340 = datetime(2026, 9, 19, 16, tzinfo=UTC)  # adds credit_score_band, sends equity_requested as string
IOS_341 = datetime(2026, 9, 24, 18, tzinfo=UTC)  # fixes the type bug, keeps the unplanned property

MALFORM_RATES = [  # (kind, probability per message)
    ("missing_required", 0.006), ("wrong_type", 0.003), ("invalid_enum", 0.003),
    ("bad_name", 0.0025), ("no_identity", 0.0015), ("bad_timestamp", 0.001),
    ("future_timestamp", 0.001),
]
DUPLICATE_RATE = 0.006
OFFLINE_RATE = 0.05  # share of app events buffered offline


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


@dataclass
class Visitor:
    anon: str
    device: str
    source: str
    state: str
    user_id: str | None = None


class Sim:
    def __init__(self, seed: int = SEED, daily_visitors: int = 520):
        self.r = random.Random(seed)
        self.daily = daily_visitors
        self.msgs: list[dict] = []
        self.stats = defaultdict(int)

    # ---------- ids ----------
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.r.getrandbits(128), version=4))

    def _b36(self, n=10) -> str:
        return "".join(self.r.choice("0123456789abcdefghijklmnopqrstuvwxyz") for _ in range(n))

    # ---------- message builders ----------
    def _context(self, v: Visitor, ts: datetime, campaign_source: str | None, path: str | None = None) -> dict:
        if v.device in ("desktop", "mobile_web"):
            lib = {"name": "analytics.js", "version": "4.1.0"}
            ctx = {"library": lib, "device": {"type": v.device},
                   "userAgent": "Mozilla/5.0 (Windows NT 10.0)" if v.device == "desktop" else "Mozilla/5.0 (iPhone)"}
            if path:
                ctx["page"] = {"path": path}
        elif v.device == "ios":
            app = "3.3.0" if ts < IOS_340 else ("3.4.0" if ts < IOS_341 else "3.4.1")
            # staged rollout: not every phone updates immediately
            if app != "3.3.0" and self.r.random() < max(0.1, 0.6 - (ts - IOS_340).days * 0.08):
                app = "3.3.0"
            ctx = {"library": {"name": "analytics-ios", "version": "4.1.6"}, "device": {"type": "ios"}, "app": {"version": app}}
        else:
            ctx = {"library": {"name": "analytics-android", "version": "4.10.1"}, "device": {"type": "android"},
                   "app": {"version": "3.3.2"}}
        if campaign_source and campaign_source != "direct":
            medium = "email" if campaign_source == "lifecycle_email" else SOURCES[campaign_source][1]
            ctx["campaign"] = {"source": campaign_source, "medium": medium, "name": f"{campaign_source}_fall26"}
        return ctx

    def _base(self, v: Visitor, ts: datetime, mtype: str, campaign: str | None, path=None) -> dict:
        return {"messageId": self._uuid(), "type": mtype, "anonymousId": v.anon, "userId": v.user_id,
                "timestamp": ts, "context": self._context(v, ts, campaign, path)}

    def page(self, v, ts, name, path, campaign):
        m = self._base(v, ts, "page", campaign, path)
        m["name"] = name
        m["properties"] = {"path": path, "title": f"Keystone Equity | {name}"}
        self.msgs.append(m)

    def track(self, v, ts, event, props, campaign):
        m = self._base(v, ts, "track", campaign)
        m["event"] = event
        m["properties"] = props
        app = m["context"].get("app", {}).get("version")
        if event == "Estimate Completed" and app in ("3.4.0", "3.4.1"):
            props = dict(props, credit_score_band=self.r.choice(["fair", "good", "excellent"]))
            if app == "3.4.0":
                props["equity_requested"] = str(int(props["equity_requested"]))
            m["properties"] = props
            self.stats["drift_injected"] += 1
        self.msgs.append(m)

    def identify(self, v, ts, traits, campaign):
        m = self._base(v, ts, "identify", campaign)
        m["traits"] = traits
        self.msgs.append(m)

    # ---------- behaviour ----------
    def _p(self, step, v):
        q = SOURCES[v.source][2] ** 0.5 * DEVICE_MULT[v.device]
        if step == "Documents Uploaded" and v.device == "mobile_web":
            q *= 0.8  # uploading docs from a phone browser is painful
        return min(0.95, P[step] * q)

    def _session_start(self, day: datetime) -> datetime:
        # US-centric diurnal curve, expressed in UTC (peak ~ 19:00-03:00 UTC)
        hour = int(self.r.choices(range(24), weights=[8, 7, 5, 3, 2, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 8, 9, 10, 11, 11, 10, 10, 9])[0])
        return day + timedelta(hours=hour, minutes=self.r.random() * 60)

    def _gap(self, lo=4, hi=90) -> timedelta:
        return timedelta(seconds=self.r.uniform(lo, hi))

    def visitor_journey(self, day: datetime):
        r = self.r
        src = r.choices(list(SOURCES), weights=[s[0] for s in SOURCES.values()])[0]
        dev = r.choices(list(DEVICES), weights=list(DEVICES.values()))[0]
        v = Visitor(anon="anon_" + self._uuid().replace("-", "")[:16], device=dev, source=src,
                    state=r.choices(STATES, weights=STATE_W)[0])
        t = self._session_start(day)
        self.page(v, t, "Home", "/", src)
        if r.random() < 0.35:
            t += self._gap(); self.page(v, t, "How It Works", "/how-it-works", src)
        if r.random() > self._p("Estimate Started", v):
            return
        t += self._gap(); self.page(v, t, "Estimate", "/estimate", src)
        t += self._gap(2, 20)
        self.track(v, t, "Estimate Started", {"property_state": v.state,
                   "entry_point": r.choices(["hero_cta", "nav", "blog", "partner_widget"],
                                            weights=[5, 3, 1, 2 if src == "partner_lender" else 0.2])[0]}, src)
        if r.random() > self._p("Estimate Completed", v):
            return
        home = round(r.lognormvariate(13.0, 0.45), -3)
        mort = round(home * r.uniform(0.0, 0.7), -3)
        ask = round(min(600000, max(15000, (home - mort) * r.uniform(0.05, 0.3))), -3)
        t += self._gap(30, 240)
        self.track(v, t, "Estimate Completed", {"estimated_home_value": home, "mortgage_balance": mort,
                   "equity_requested": ask,
                   "property_type": r.choices(["single_family", "condo", "townhouse", "multi_family"], weights=[70, 14, 12, 4])[0]}, src)
        if r.random() > self._p("Account Created", v):
            return
        t += self._gap(); self.page(v, t, "Sign Up", "/signup", src)
        method = r.choices(["email", "google", "apple"], weights=[55, 35, 10 if dev == "ios" else 3])[0]
        t += self._gap(20, 120)
        v.user_id = "usr_" + self._b36()
        self.track(v, t, "Account Created", {"signup_method": method}, src)
        band = "<300k" if home < 300e3 else "300-500k" if home < 500e3 else "500-800k" if home < 800e3 else "800k+"
        traits = {"created_at": iso(t), "state": v.state, "home_value_band": band, "signup_method": method}
        if r.random() < 0.004:  # a dev "helpfully" adds the email trait -> PII leak
            traits["email"] = f"{v.user_id}@example.com"; self.stats["pii_injected"] += 1
        t += timedelta(milliseconds=300)
        self.identify(v, t, traits, src)

        app_id = "app_" + self._b36()
        # application may happen now or in a later session (possibly another device)
        if r.random() > self._p("Application Submitted", v):
            return
        if r.random() < 0.45:
            t, v = self._return_session(v, t, 0.3, 3.0, "Dashboard", "/dashboard")
        t += self._gap(); self.page(v, t, "Application", "/apply", None)
        t += self._gap(120, 900)
        self.track(v, t, "Application Submitted", {"application_id": app_id, "equity_requested": ask,
                   "product": "hei" if r.random() < 0.85 else "heloc"}, None)
        if r.random() > self._p("Documents Uploaded", v):
            return
        if r.random() < 0.6:
            t, v = self._return_session(v, t, 0.2, 2.5, "Documents", "/documents")
        else:
            t += self._gap(); self.page(v, t, "Documents", "/documents", None)
        t += self._gap(60, 600)
        n = r.randint(2, 5)
        self.track(v, t, "Documents Uploaded", {"application_id": app_id, "document_count": n,
                   "document_types": r.sample(["id", "mortgage_statement", "insurance", "tax_return", "pay_stub"], n)}, None)
        if r.random() > self._p("Offer Viewed", v):
            return
        t, v = self._return_session(v, t, 1.0, 5.0, "Offer", "/offer", campaign="lifecycle_email")
        offer = round(ask * r.uniform(0.8, 1.0), -2)
        pct = round(min(30, offer / home * 100 * r.uniform(1.6, 2.2)), 2)
        t += self._gap()
        self.track(v, t, "Offer Viewed", {"application_id": app_id, "offer_amount": offer, "investment_percentage": pct}, "lifecycle_email")
        if r.random() > self._p("Offer Accepted", v):
            return
        if r.random() < 0.5:
            t, v = self._return_session(v, t, 0.5, 4.0, "Offer", "/offer", campaign="lifecycle_email")
        t += self._gap(60, 600)
        self.track(v, t, "Offer Accepted", {"application_id": app_id, "offer_amount": offer}, "lifecycle_email")

    def _return_session(self, v: Visitor, t, lo_days, hi_days, page_name, path, campaign=None):
        t = t + timedelta(days=self.r.uniform(lo_days, hi_days))
        if self.r.random() < 0.18:  # comes back on another device -> new anonymousId, logs in
            dev = self.r.choice([d for d in DEVICES if d != v.device])
            v = Visitor(anon="anon_" + self._uuid().replace("-", "")[:16], device=dev, source=v.source,
                        state=v.state, user_id=v.user_id)
            self.identify(v, t, {"created_at": iso(t - timedelta(days=lo_days)), "state": v.state}, campaign)
            self.stats["cross_device"] += 1
            t += timedelta(seconds=2)
        self.page(v, t, page_name, path, campaign)
        return t, v

    # ---------- delivery: sentAt/receivedAt, outage, malformations, duplicates ----------
    def _deliver(self, m: dict, stream_end: datetime) -> list[dict]:
        r = self.r
        ts: datetime = m["timestamp"]
        lib = m["context"]["library"]["name"]
        if lib == "analytics.js":
            if OUTAGE[0] <= ts < OUTAGE[1] and r.random() < OUTAGE[2]:
                self.stats["lost_in_outage"] += 1
                return []
            sent = ts + timedelta(milliseconds=r.uniform(5, 1500))
        else:
            sent = ts + timedelta(seconds=r.uniform(0.5, 30))  # app SDKs flush in batches
            if r.random() < OFFLINE_RATE:
                sent = ts + timedelta(hours=r.choice([r.uniform(1, 6), r.uniform(6, 36)]))
                self.stats["late_offline"] += 1
        recv = sent + timedelta(milliseconds=r.uniform(30, 900))
        m["sentAt"] = iso(sent)
        m["timestamp"] = iso(ts)
        m["receivedAt"] = iso(recv)
        self._malform(m)
        out = [m]
        if r.random() < DUPLICATE_RATE:  # SDK retry: same messageId delivered again
            d = copy.deepcopy(m)
            d["receivedAt"] = iso(recv + timedelta(seconds=r.uniform(1, 5400)))
            out.append(d); self.stats["duplicates"] += 1
        kept = []
        for x in out:
            if datetime.fromisoformat(x["receivedAt"].replace("Z", "+00:00")) >= stream_end:
                self.stats["in_flight_at_stream_end"] += 1
            else:
                kept.append(x)
        return kept

    def _malform(self, m: dict):
        r = self.r
        x = r.random()
        acc = 0.0
        for kind, p in MALFORM_RATES:
            acc += p
            if x < acc:
                break
        else:
            return
        props = m.get("properties") or m.get("traits") or {}
        if kind == "missing_required" and props:
            props.pop(r.choice(sorted(props)))
        elif kind == "wrong_type" and m["type"] == "track":
            num = [k for k, v in props.items() if isinstance(v, (int, float))]
            if not num:
                return
            props[r.choice(num)] = "N/A"
        elif kind == "invalid_enum":
            if m["type"] == "page":
                m["name"] = r.choice(["home", "Pricing Page", "estimate-v2"])
            else:
                m["context"]["device"]["type"] = r.choice(["tablet", "Desktop", "unknown"])
        elif kind == "bad_name" and m["type"] == "track":
            m["event"] = r.choice(["estimate_started", "clicked_offer", "OfferViewed", "Submit Application Btn"])
        elif kind == "no_identity":
            m["anonymousId"] = None; m["userId"] = None
        elif kind == "bad_timestamp":
            m["timestamp"] = r.choice(["2026-13-45T25:61:00Z", "", "yesterday"])
        elif kind == "future_timestamp":
            ts = datetime.fromisoformat(m["timestamp"].replace("Z", "+00:00"))
            m["timestamp"] = iso(ts + timedelta(days=r.choice([1, 2, 365])))  # wrong device clock
        else:
            return
        self.stats[f"malformed_{kind}"] += 1

    def run(self, start: datetime, days: int, stream_end: datetime) -> list[dict]:
        for d in range(days):
            day = start + timedelta(days=d)
            n = int(self.daily * (0.78 if day.weekday() >= 5 else 1.0) * (1 + 0.006 * d) * self.r.uniform(0.93, 1.07))
            for _ in range(n):
                self.visitor_journey(day)
            self.stats["visitors"] += n
        delivered = []
        for m in self.msgs:
            if m["timestamp"] >= stream_end:  # journeys that would continue after the clock stops
                self.stats["future_journey_steps_dropped"] += 1
                continue
            delivered += self._deliver(m, stream_end)
        self.stats["messages_generated"] = len(self.msgs)
        self.stats["messages_delivered"] = len(delivered)
        return delivered


def write_landing(msgs: list[dict], landing: Path) -> int:
    """Write messages into hourly NDJSON files by receivedAt (the queue)."""
    if landing.exists():
        shutil.rmtree(landing)
    buckets: dict[str, list[dict]] = defaultdict(list)
    for m in msgs:
        buckets[m["receivedAt"][:13]].append(m)
    for hour, ms in buckets.items():
        ms.sort(key=lambda m: m["receivedAt"])
        p = landing / f"dt={hour[:10]}" / f"hour={hour[11:13]}" / "events.ndjson"
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w") as f:
            for m in ms:
                f.write(json.dumps(m, separators=(",", ":")) + "\n")
    return len(buckets)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--days", type=int, default=SIM_DAYS)
    ap.add_argument("--daily-visitors", type=int, default=520)
    ap.add_argument("--landing", type=Path, default=LANDING_DIR)
    a = ap.parse_args(argv)
    start = datetime.fromisoformat(SIM_START).replace(tzinfo=UTC)
    end = datetime.fromisoformat(STREAM_END).replace(tzinfo=UTC)
    if a.days != SIM_DAYS:
        end = start + timedelta(days=a.days)
    sim = Sim(a.seed, a.daily_visitors)
    msgs = sim.run(start, a.days, end)
    files = write_landing(msgs, a.landing)
    stats = dict(sorted(sim.stats.items()), files_written=files)
    print(json.dumps(stats, indent=2))
    return stats


if __name__ == "__main__":
    main()
