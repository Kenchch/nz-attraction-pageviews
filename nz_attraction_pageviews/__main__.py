"""Entry point: python -m nz_attraction_pageviews

Reads venues.csv, ingests into warehouse.duckdb, prints the run summary.
Safe to run repeatedly. The second run only asks for days it has not seen.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from . import ingest
from .quality import QualityGateFailed

VENUES_CSV = "venues.csv"
DB_PATH = "warehouse.duckdb"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--venues", default=VENUES_CSV)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--backfill-days", type=int, default=ingest.DEFAULT_BACKFILL_DAYS)
    parser.add_argument("--chunk-days", type=int, default=ingest.DEFAULT_CHUNK_DAYS)
    parser.add_argument("--max-reject-rate", type=float, default=ingest.DEFAULT_MAX_REJECT_RATE)
    parser.add_argument("--today", type=date.fromisoformat)
    args = parser.parse_args(argv)
    venues = ingest.read_venues(args.venues)
    con = ingest.connect(args.db)

    try:
        summary = ingest.run(
            con,
            venues,
            backfill_days=args.backfill_days,
            chunk_days=args.chunk_days,
            max_reject_rate=args.max_reject_rate,
            today=args.today,
        )
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
    if summary.note:
        print(summary.note)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
