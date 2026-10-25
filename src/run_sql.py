"""Run every sql/*.sql analysis query against the warehouse and save results/sql_*.csv."""
from __future__ import annotations

from pathlib import Path

import duckdb

from .config import DUCKDB_PATH, RESULTS_DIR, ROOT


def run(db: Path = DUCKDB_PATH) -> dict[str, int]:
    con = duckdb.connect(str(db), read_only=True)
    out = {}
    for f in sorted((ROOT / "sql").glob("*.sql")):
        df = con.execute(f.read_text()).df()
        df.to_csv(RESULTS_DIR / f"sql_{f.stem}.csv", index=False)
        out[f.stem] = len(df)
        print(f"{f.name}: {len(df)} rows")
    con.close()
    return out


if __name__ == "__main__":
    run()
