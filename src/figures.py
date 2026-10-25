"""Charts for the README, read straight from the warehouse (synthetic data).

  figures/01_funnel.png               7-step funnel (users reaching each step, all sources)
  figures/02_dq_violation_trend.png   daily quarantine rate + blocking violations by code
  figures/03_lateness_by_sdk.png      share of events still not arrived after N hours, by SDK
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib
import matplotlib.ticker  # noqa: F401

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from .config import DUCKDB_PATH, FIGURES_DIR  # noqa: E402

BLUE, GREY, RED = "#2f5d8a", "#9aa5b1", "#c0392b"
plt.rcParams.update({"figure.dpi": 130, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.titleweight": "bold", "font.size": 10})


def funnel(con, out: Path):
    df = con.execute("""select step_order, step_name, users, conversion_from_previous, conversion_from_start
                        from marts.mart_funnel_conversion where segment='all' order by step_order""").df()
    fig, ax = plt.subplots(figsize=(8.5, 4.6))
    y = np.arange(len(df))[::-1]
    ax.barh(y, df.users, color=BLUE, height=0.65)
    top = df.users.max()
    for yi, r in zip(y, df.itertuples()):
        step = "" if np.isnan(r.conversion_from_previous) else f"   {r.conversion_from_previous:.0%} of prev"
        ax.text(r.users + top * 0.01, yi, f"{r.users:,}{step}", va="center", fontsize=9)
    ax.set_yticks(y, df.step_name)
    ax.set_xlim(0, top * 1.35)
    ax.set_xlabel("users (identity-stitched) reaching step, strict order")
    end = df.conversion_from_start.iloc[-1]
    ax.set_title(f"Home-equity funnel, Sep 2026 (synthetic): {end:.1%} start-to-accept")
    fig.tight_layout()
    fig.savefig(out / "01_funnel.png")
    plt.close(fig)


def dq_trend(con, out: Path):
    daily = con.execute("""
        with acc as (select cast(received_at as date) d, count(*) n from raw.events group by 1),
             qua as (select cast(received_at as date) d, count(*) n from raw.events_quarantine
                     where received_at is not null group by 1)
        select acc.d, acc.n + coalesce(qua.n, 0) messages, coalesce(qua.n, 0) / (acc.n + coalesce(qua.n, 0)) block_rate
        from acc left join qua using (d) order by 1""").df()
    codes = con.execute("""
        select cast(received_at as date) d, code, count(distinct message_id) n
        from raw.schema_violations where severity = 'block' and received_at is not null
        group by all""").df()
    piv = codes.pivot_table(index="d", columns="code", values="n", fill_value=0)
    piv = piv.reindex(daily.d, fill_value=0)
    order = piv.sum().sort_values(ascending=False).index
    keep = list(order[:5])
    if len(order) > 5:
        piv["other"] = piv[list(order[5:])].sum(axis=1)
        keep.append("other")
    piv = piv[keep]

    fig, (a1, a2) = plt.subplots(2, 1, figsize=(9, 6.2), sharex=True, gridspec_kw=dict(height_ratios=[1, 1.2]))
    a1.plot(daily.d, daily.block_rate * 100, color=BLUE, marker="o", ms=3)
    a1.axhline(2, color=GREY, ls="--", lw=1)
    a1.text(daily.d.iloc[0], 2.08, "warn 2%", color=GREY, fontsize=8)
    a1.axvspan(np.datetime64("2026-09-19"), np.datetime64("2026-09-24"), color=RED, alpha=0.08)
    a1.text(np.datetime64("2026-09-24T12"), daily.block_rate.max() * 100 * 0.95, "iOS 3.4.0 sends\nequity_requested as string",
            color=RED, fontsize=8, va="top")
    a1.set_ylabel("quarantined, % of msgs")
    a1.set_title("Data-quality: daily quarantine rate and blocking violations (synthetic)")
    bottom = np.zeros(len(piv))
    cmap = plt.get_cmap("tab10")
    for i, c in enumerate(piv.columns):
        a2.bar(piv.index, piv[c], bottom=bottom, color=cmap(i), label=c, width=0.8)
        bottom += piv[c].values
    a2.set_ylabel("messages blocked")
    a2.legend(ncol=3, fontsize=8, frameon=False, loc="upper left")
    a2.set_ylim(0, bottom.max() * 1.35)
    a2.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 8, 15, 22, 29]))
    a2.set_xlim(np.datetime64("2026-08-31T12"), np.datetime64("2026-09-30T12"))
    a2.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    fig.tight_layout()
    fig.savefig(out / "02_dq_violation_trend.png")
    plt.close(fig)


def lateness(con, out: Path):
    df = con.execute("""select library_name, arrival_lag_seconds / 3600.0 h
                        from staging.stg_segment__events where arrival_lag_seconds is not null""").df()
    grid = np.logspace(np.log10(1 / 60), np.log10(40), 80)
    fig, ax = plt.subplots(figsize=(8.5, 4.4))
    style = {"analytics.js": (GREY, "-"), "analytics-ios": (BLUE, "-"), "analytics-android": (RED, "--")}
    for lib, g in df.groupby("library_name"):
        h = np.sort(g.h.values)
        surv = 1 - np.searchsorted(h, grid, side="right") / len(h)
        color, ls = style.get(lib, (None, "-"))
        ax.plot(grid, surv * 100, ls=ls, lw=2, label=f"{lib} (n={len(h):,}, {np.mean(h > 1):.1%} > 1h)", color=color)
    ax.set_xscale("log")
    ax.set_xticks([1 / 60, 0.1, 1, 6, 24, 36], ["1m", "6m", "1h", "6h", "24h", "36h"])
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(decimals=0))
    ax.axvline(1, color=GREY, ls="--", lw=1)
    ax.set_xlabel("time between event (device clock) and receipt at the collector")
    ax.set_ylabel("share of events still not arrived")
    ax.set_title("Arrival lateness by SDK: mobile offline buffering (synthetic)")
    ax.legend(frameon=False, loc="upper right")
    ax.set_ylim(0, 7)
    fig.tight_layout()
    fig.savefig(out / "03_lateness_by_sdk.png")
    plt.close(fig)


def run(db: Path = DUCKDB_PATH, out: Path = FIGURES_DIR) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db), read_only=True)
    try:
        funnel(con, out)
        dq_trend(con, out)
        lateness(con, out)
    finally:
        con.close()
    files = sorted(p.name for p in out.glob("*.png"))
    print("figures:", ", ".join(files))
    return dict(figures=files)


if __name__ == "__main__":
    run()
