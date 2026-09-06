"""Capture a small, dated Wikimedia response and an actual ingest run log.

Run from the repository root: python scripts/capture_live.py
"""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from nz_attraction_pageviews import client, ingest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--today", type=date.fromisoformat, default=date.today())
    parser.add_argument("--output", type=Path, default=Path("fixtures/live"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    end = args.today - timedelta(days=2)
    start = end - timedelta(days=6)
    captures = []
    for article in ("Milford_Sound", "Te_Papa"):
        url = client.build_url(article, start, end)
        status, _, body = client.http_get(url)
        if status != 200:
            raise client.ApiError(f"Capture returned {status}: {url}")
        payload = json.loads(body)
        path = args.output / f"{article}.json"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        captures.append({"article": article, "url": url, "status": status, "file": path.name})
    con = ingest.connect(":memory:")
    try:
        summary = ingest.run(con, ingest.read_venues("venues.csv"), today=args.today,
                             backfill_days=7, chunk_days=7)
    finally:
        con.close()
    evidence = {"captured_at_utc": datetime.now(timezone.utc).isoformat(),
                "captures": captures, "run_summary": asdict(summary),
                "storage": "fresh in-memory DuckDB; seven-day backfill across venues.csv"}
    (args.output / "run-log.json").write_text(
        json.dumps(evidence, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2, default=str))


if __name__ == "__main__":
    main()
