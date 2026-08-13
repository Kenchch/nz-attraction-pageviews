"""Offline demo: two runs against a synthetic API, so you can see the behaviour
without a network connection or a single request to Wikimedia.

    python demo.py

Run 1 backfills 90 days. Run 2 happens three days later and asks only for the
three days it has not seen. The row count proves nothing was duplicated.
"""

from __future__ import annotations

import random
import zlib
from datetime import date, timedelta

from src import ingest

DB = "demo.duckdb"
RUN_1_DAY = date(2026, 3, 1)
RUN_2_DAY = date(2026, 3, 4)


def synthetic_api(article: str, start: date, end: date) -> list[dict]:
    """Stands in for client.fetch_window. Weekends get more traffic."""
    rows, cursor = [], start
    # crc32, not hash(): str hashing is salted per process, so hash() would make
    # the numbers below differ on every run despite the seed above.
    base = 200 + (zlib.crc32(article.encode()) % 800)
    while cursor <= end:
        weekend = 1.4 if cursor.weekday() >= 5 else 1.0
        rows.append(
            {
                "project": "en.wikipedia",
                "article": article,
                "granularity": "daily",
                "timestamp": f"{cursor:%Y%m%d}00",
                "access": "all-access",
                "agent": "user",
                "views": int(base * weekend * random.uniform(0.8, 1.2)),
            }
        )
        cursor += timedelta(days=1)
    return rows


def main() -> None:
    random.seed(7)
    venues = ingest.read_venues("venues.csv")
    con = ingest.connect(DB)
    con.execute(
        "DELETE FROM pageviews; DELETE FROM quarantine; DELETE FROM watermark; DELETE FROM run_log"
    )

    for label, day in (("run 1 (first sight)", RUN_1_DAY), ("run 2 (three days later)", RUN_2_DAY)):
        summary = ingest.run(
            con, venues, today=day, backfill_days=90, chunk_days=30, fetch=synthetic_api
        )
        print(
            f"{label:26} {summary.status:6} "
            f"requests={summary.requests:<3} fetched={summary.rows_fetched:<4} "
            f"loaded={summary.rows_loaded:<4} quarantined={summary.rows_quarantined}"
        )

    total, dupes = con.execute("""
        SELECT (SELECT count(*) FROM pageviews),
               (SELECT count(*) FROM (SELECT venue_id, view_date FROM pageviews
                                      GROUP BY 1, 2 HAVING count(*) > 1))
    """).fetchone()
    print(f"\n{total} rows, {dupes} duplicate (venue, date) pairs\n")

    print(f"{'venue':<18}{'days':>6}{'mean/day':>10}  window")
    for venue_id, days, mean, first, last in con.execute("""
        SELECT venue_id, count(*), round(avg(views)), min(view_date), max(view_date)
        FROM pageviews GROUP BY 1 ORDER BY 3 DESC
    """).fetchall():
        print(f"{venue_id:<18}{days:>6}{mean:>10.0f}  {first} to {last}")

    con.close()


if __name__ == "__main__":
    main()
