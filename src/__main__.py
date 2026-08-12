"""Entry point: python -m src

Reads venues.csv, ingests into warehouse.duckdb, prints the run summary.
Safe to run repeatedly. The second run only asks for days it has not seen.
"""

from __future__ import annotations

import sys

from . import ingest
from .quality import QualityGateFailed

VENUES_CSV = "venues.csv"
DB_PATH = "warehouse.duckdb"


def main() -> int:
    venues = ingest.read_venues(VENUES_CSV)
    con = ingest.connect(DB_PATH)

    try:
        summary = ingest.run(con, venues)
    except QualityGateFailed as exc:
        print(f"quality gate failed, nothing loaded: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()

    print(
        f"run {summary.run_id} {summary.status}: "
        f"{summary.requests} requests, "
        f"{summary.rows_fetched} fetched, "
        f"{summary.rows_loaded} loaded, "
        f"{summary.rows_quarantined} quarantined "
        f"({summary.reject_rate:.2%})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
