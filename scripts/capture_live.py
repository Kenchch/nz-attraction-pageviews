"""Capture a small, dated Wikimedia response and an actual ingest run log.

Run from the repository root:

    python scripts/capture_live.py --contact you@example.org

Writes to captures/<date>/ unless --output says otherwise; pass
--output fixtures/live to refresh the evidence committed with the repository.
"""

from __future__ import annotations

import argparse
import functools
import json
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from nz_attraction_pageviews import client, ingest


def main():
    # UTC, because the pipeline's day is a UTC day: a local date.today() in New
    # Zealand is a day ahead of it for half of every day.
    today_utc = datetime.now(timezone.utc).date()
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--today", type=date.fromisoformat, default=today_utc)
    parser.add_argument("--output", type=Path, help="default: captures/<today>")
    parser.add_argument(
        "--contact", help=f"email or URL for the User-Agent (default: ${client.CONTACT_ENV})"
    )
    args = parser.parse_args()
    output = args.output or Path("captures") / args.today.isoformat()
    output.mkdir(parents=True, exist_ok=True)
    opener = functools.partial(client.http_get, user_agent=client.build_user_agent(args.contact))

    end = args.today - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    start = end - timedelta(days=6)
    captures = []
    for article in ("Milford_Sound", "Te_Papa"):
        url = client.build_url(article, start, end)
        status, _, body = opener(url)
        if status != 200:
            raise client.ApiError(f"Capture returned {status}: {url}")
        payload = json.loads(body)
        path = output / f"{article}.json"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        captures.append({"article": article, "url": url, "status": status, "file": path.name})
    con = ingest.connect(":memory:")
    try:
        summary = ingest.run(
            con,
            ingest.read_venues("venues.csv"),
            today=args.today,
            backfill_days=7,
            chunk_days=7,
            opener=opener,
        )
    finally:
        con.close()
    evidence = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "captures": captures,
        "run_summary": asdict(summary),
        "storage": "fresh in-memory DuckDB; seven-day backfill across venues.csv",
    }
    (output / "run-log.json").write_text(
        json.dumps(evidence, indent=2, default=str) + "\n", encoding="utf-8"
    )
    print(json.dumps(evidence, indent=2, default=str))


if __name__ == "__main__":
    main()
