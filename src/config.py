"""Shared paths and settings. Everything is local and free: files + DuckDB."""
from __future__ import annotations
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "tracking_plan" / "schemas"
LANDING_DIR = Path(os.environ.get("PEP_LANDING_DIR", ROOT / "data" / "landing"))
DUCKDB_PATH = Path(os.environ.get("PEP_DUCKDB_PATH", ROOT / "warehouse" / "events.duckdb"))
RESULTS_DIR = ROOT / "results"
FIGURES_DIR = ROOT / "figures"
DBT_DIR = ROOT / "dbt"

# Simulation window (synthetic data). The "stream clock" ends at STREAM_END:
# anything whose receivedAt is later has not arrived yet and is not written.
SIM_START = "2026-09-01T00:00:00"
SIM_DAYS = 30
STREAM_END = "2026-10-01T00:00:00"
SEED = 42

FUNNEL_STEPS = [
    "Estimate Started",
    "Estimate Completed",
    "Account Created",
    "Application Submitted",
    "Documents Uploaded",
    "Offer Viewed",
    "Offer Accepted",
]
