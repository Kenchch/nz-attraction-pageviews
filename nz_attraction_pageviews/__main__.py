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
    parser.add_argument("command", nargs="?", choices=["ingest", "resolve"], default="ingest")
    parser.add_argument("venue_id", nargs="?")
    # "null" addresses the rows a timestamp_parses failure left with no day,
    # which had no way to be resolved at all before.
    parser.add_argument(
        "view_date",
        nargs="?",
        help="YYYY-MM-DD, or 'null' for rejects with no parseable date",
    )
    parser.add_argument("--resolution", default="Reviewed by operator")
    parser.add_argument(
        "--accept",
        action="store_true",
        help="record the day as never arriving, so the watermark may step over it",
    )
    parser.add_argument("--venues", default=VENUES_CSV)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--backfill-days", type=int, default=ingest.DEFAULT_BACKFILL_DAYS)
    parser.add_argument("--chunk-days", type=int, default=ingest.DEFAULT_CHUNK_DAYS)
    parser.add_argument("--max-reject-rate", type=float, default=ingest.DEFAULT_MAX_REJECT_RATE)
    parser.add_argument("--today", type=date.fromisoformat)
    args = parser.parse_args(argv)
    if args.command == "resolve":
        if not args.venue_id or args.view_date is None:
            parser.error("resolve requires a venue_id and YYYY-MM-DD date (or 'null')")
        if args.view_date.lower() == "null":
            view_date = None
        else:
            try:
                view_date = date.fromisoformat(args.view_date)
            except ValueError:
                parser.error(f"not a date: {args.view_date!r} (use YYYY-MM-DD or 'null')")
        resolution = ingest.ACCEPTED if args.accept else args.resolution
        con = ingest.connect(args.db)
        try:
            changed = ingest.resolve(con, args.venue_id, view_date, resolution)
        finally:
            con.close()
        if args.accept:
            print(
                f"{changed} rejection(s) accepted; the watermark may now step over "
                f"those days. No pageview data was loaded."
            )
        else:
            print(f"{changed} rejection(s) annotated; no data or watermarks changed")
        return 0 if changed else 1
    if args.venue_id or args.view_date:
        parser.error("venue_id and view_date are only accepted with resolve")
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
    # `degraded` exits 0. The run did its job: every venue that could load did,
    # and the ones that could not are named above and in run_log.note. Exiting
    # non-zero would make a scheduler retry a run that is already complete, and
    # a nightly job that alerts on a standing fault every night is a job whose
    # alerts get muted. Read run_log.status to find held venues, and
    # `resolve <venue> <date> --accept` to release one.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
