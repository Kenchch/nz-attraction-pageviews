"""Incremental ingest of NZ attraction pageviews into DuckDB.

Shape of a run:

    read venues.csv
      -> per venue, work out the start date from its watermark
      -> split the range into fixed windows (this API's version of pagination)
      -> fetch, apply acceptance criteria
      -> check the gate across the whole run
      -> load, quarantine, advance watermarks, write the run log

The load is a single transaction. Either the whole run lands or none of it does,
so a crash halfway through eight venues cannot leave three venues a day ahead of
the other five.
"""

from __future__ import annotations

import csv
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from . import client, quality

UTC = timezone.utc  # datetime.UTC only exists from 3.11; this keeps 3.10 working

DEFAULT_CHUNK_DAYS = 30
DEFAULT_BACKFILL_DAYS = 90
DEFAULT_MAX_REJECT_RATE = 0.05

# The API publishes with a lag. Asking for yesterday usually returns nothing,
# which is not an error but does waste a request on every run.
PUBLICATION_LAG_DAYS = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS pageviews (
    venue_id    VARCHAR NOT NULL,
    article     VARCHAR NOT NULL,
    view_date   DATE    NOT NULL,
    views       BIGINT  NOT NULL,
    run_id      VARCHAR NOT NULL,
    loaded_at   TIMESTAMP NOT NULL,
    PRIMARY KEY (venue_id, view_date)
);

CREATE TABLE IF NOT EXISTS quarantine (
    run_id     VARCHAR NOT NULL,
    venue_id   VARCHAR NOT NULL,
    article    VARCHAR NOT NULL,
    rule       VARCHAR NOT NULL,
    detail     VARCHAR,
    raw        VARCHAR,
    seen_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS watermark (
    venue_id   VARCHAR PRIMARY KEY,
    last_date  DATE NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id            VARCHAR PRIMARY KEY,
    started_at        TIMESTAMP NOT NULL,
    finished_at       TIMESTAMP,
    status            VARCHAR NOT NULL,
    venues            INTEGER NOT NULL,
    requests          INTEGER NOT NULL,
    rows_fetched      INTEGER NOT NULL,
    rows_loaded       INTEGER NOT NULL,
    rows_quarantined  INTEGER NOT NULL,
    reject_rate       DOUBLE,
    note              VARCHAR
);
"""


@dataclass(frozen=True)
class Venue:
    venue_id: str
    venue_name: str
    region: str
    wiki_article: str


@dataclass
class RunSummary:
    run_id: str
    status: str
    venues: int = 0
    requests: int = 0
    rows_fetched: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    reject_rate: float = 0.0
    note: str = ""


def connect(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute(SCHEMA)
    return con


def read_venues(path: str | Path) -> list[Venue]:
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"{path} has no venues")
    return [Venue(**row) for row in rows]


def plan_windows(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    """Split an inclusive range into windows of at most chunk_days."""
    if chunk_days < 1:
        raise ValueError("chunk_days must be at least 1")
    windows = []
    cursor = start
    while cursor <= end:
        stop = min(cursor + timedelta(days=chunk_days - 1), end)
        windows.append((cursor, stop))
        cursor = stop + timedelta(days=1)
    return windows


def get_watermark(con, venue_id: str) -> date | None:
    row = con.execute("SELECT last_date FROM watermark WHERE venue_id = ?", [venue_id]).fetchone()
    return row[0] if row else None


def start_date_for(con, venue: Venue, end: date, backfill_days: int) -> date:
    """Resume the day after the watermark, or backfill on first sight of a venue."""
    watermark = get_watermark(con, venue.venue_id)
    if watermark is None:
        return end - timedelta(days=backfill_days - 1)
    return watermark + timedelta(days=1)


def _venue_watermark(
    start: date,
    end: date,
    venue_clean: list[quality.CleanRow],
    venue_bad: list[quality.BadRow],
) -> date | None:
    """How far this venue may advance. None means leave the watermark where it is.

    The watermark is a promise that everything up to it has been dealt with, so it
    must never step over a day we failed to load. A day whose row was quarantined
    is not covered: the row is in `quarantine`, but the run-level gate is a rate
    across every venue, so one venue's bad patch can sit under the threshold and
    pass. If the watermark advanced anyway, those days would never be requested
    again and the quarantine would be a record of data we permanently lost.

    So a venue with nothing rejected advances to the end of the range it asked
    for - including a window that came back genuinely empty, which is what stops
    a quiet venue being re-requested forever. A venue with something rejected
    advances only across the unbroken run of days it actually loaded, and stops
    at the first gap. Next run resumes there and has another go at the bad day.
    """
    if not venue_bad:
        return end

    loaded = {row.view_date for row in venue_clean}
    frontier = None
    cursor = start
    while cursor <= end and cursor in loaded:
        frontier = cursor
        cursor += timedelta(days=1)
    return frontier


def run(
    con,
    venues: list[Venue],
    *,
    today: date | None = None,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    backfill_days: int = DEFAULT_BACKFILL_DAYS,
    max_reject_rate: float = DEFAULT_MAX_REJECT_RATE,
    fetch=client.fetch_window,
) -> RunSummary:
    today = today or datetime.now(UTC).date()
    end = today - timedelta(days=PUBLICATION_LAG_DAYS)

    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(UTC)
    summary = RunSummary(run_id=run_id, status="running", venues=len(venues))

    clean: list[quality.CleanRow] = []
    bad: list[quality.BadRow] = []
    new_watermarks: dict[str, date] = {}

    try:
        for venue in venues:
            start = start_date_for(con, venue, end, backfill_days)
            if start > end:
                continue  # already current, nothing to ask for

            venue_clean: list[quality.CleanRow] = []
            venue_bad: list[quality.BadRow] = []

            for window_start, window_end in plan_windows(start, end, chunk_days):
                items = fetch(venue.wiki_article, window_start, window_end)
                summary.requests += 1
                summary.rows_fetched += len(items)

                good, rejected = quality.check_window(
                    items,
                    venue_id=venue.venue_id,
                    article=venue.wiki_article,
                    start=window_start,
                    end=window_end,
                    today=today,
                )
                venue_clean.extend(good)
                venue_bad.extend(rejected)

            clean.extend(venue_clean)
            bad.extend(venue_bad)

            frontier = _venue_watermark(start, end, venue_clean, venue_bad)
            if frontier is not None:
                new_watermarks[venue.venue_id] = frontier

        summary.rows_quarantined = len(bad)
        summary.reject_rate = quality.enforce_gate(
            summary.rows_fetched, summary.rows_quarantined, max_reject_rate
        )

        _load(con, run_id, clean, bad, new_watermarks)
        summary.rows_loaded = len(clean)
        summary.status = "ok"

    except Exception as exc:
        summary.status = "failed"
        summary.note = f"{type(exc).__name__}: {exc}"
        _write_run_log(con, summary, started_at)
        raise

    _write_run_log(con, summary, started_at)
    return summary


def _load(con, run_id, clean, bad, new_watermarks) -> None:
    now = datetime.now(UTC)
    con.execute("BEGIN TRANSACTION")
    try:
        if clean:
            con.executemany(
                "INSERT OR REPLACE INTO pageviews VALUES (?, ?, ?, ?, ?, ?)",
                [(r.venue_id, r.article, r.view_date, r.views, run_id, now) for r in clean],
            )
        if bad:
            con.executemany(
                "INSERT INTO quarantine VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(run_id, r.venue_id, r.article, r.rule, r.detail, r.raw, now) for r in bad],
            )
        if new_watermarks:
            con.executemany(
                "INSERT OR REPLACE INTO watermark VALUES (?, ?, ?)",
                [(vid, last, now) for vid, last in new_watermarks.items()],
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def _write_run_log(con, summary: RunSummary, started_at: datetime) -> None:
    con.execute(
        "INSERT OR REPLACE INTO run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            summary.run_id,
            started_at,
            datetime.now(UTC),
            summary.status,
            summary.venues,
            summary.requests,
            summary.rows_fetched,
            summary.rows_loaded,
            summary.rows_quarantined,
            summary.reject_rate,
            summary.note,
        ],
    )
