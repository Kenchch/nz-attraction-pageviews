"""The warehouse: schema, migrations, and every statement that touches it.

Nothing here decides anything about pageviews. `ingest` decides; this reads
and writes what it decided, and `load` does the writing in one transaction.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from . import quality

UTC = timezone.utc  # datetime.UTC only exists from 3.11; this keeps 3.10 working

ACCEPTED = "accepted"  # an operator decided the day is never arriving
SUPERSEDED = "superseded"  # the day later loaded cleanly

SCHEMA = """
CREATE TABLE IF NOT EXISTS pageviews (
    venue_id    VARCHAR NOT NULL,
    article     VARCHAR NOT NULL,
    view_date   DATE    NOT NULL,
    views       BIGINT  NOT NULL,
    run_id      VARCHAR NOT NULL,
    loaded_at   TIMESTAMP NOT NULL,  -- UTC
    PRIMARY KEY (venue_id, view_date)
);

CREATE TABLE IF NOT EXISTS quarantine (
    run_id       VARCHAR NOT NULL,
    venue_id     VARCHAR NOT NULL,
    article      VARCHAR NOT NULL,
    -- NULL only for a `timestamp_parses` rejection, which has no day. The
    -- window it arrived in is the nearest thing it has to one.
    view_date    DATE,
    rule         VARCHAR NOT NULL,
    detail       VARCHAR,
    raw          VARCHAR,
    seen_at      TIMESTAMP NOT NULL,  -- UTC
    resolved_at  TIMESTAMP,           -- UTC; set with `resolution`
    resolution   VARCHAR,             -- NULL (open), 'accepted' or 'superseded'
    window_start DATE,
    window_end   DATE,
    note         VARCHAR              -- the operator's annotation, kept across reopening
);

CREATE TABLE IF NOT EXISTS watermark (
    venue_id   VARCHAR PRIMARY KEY,
    last_date  DATE NOT NULL,
    updated_at TIMESTAMP NOT NULL,  -- UTC
    article    VARCHAR              -- the title the coverage up to last_date is for
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id            VARCHAR PRIMARY KEY,
    started_at        TIMESTAMP NOT NULL,  -- UTC
    finished_at       TIMESTAMP,  -- UTC
    status            VARCHAR NOT NULL,
    venues            INTEGER NOT NULL,
    requests          INTEGER NOT NULL,  -- windows asked for
    rows_fetched      INTEGER NOT NULL,
    rows_loaded       INTEGER NOT NULL,
    rows_quarantined  INTEGER NOT NULL,
    reject_rate       DOUBLE,
    note              VARCHAR,
    http_requests     INTEGER            -- NULL when the fetcher was injected
);
"""

# `CREATE TABLE IF NOT EXISTS` does nothing to a table that already exists, so a
# warehouse built by an earlier version keeps the old shape. Adding a column is
# idempotent, and every insert here names its columns, so a migrated column
# landing at the end rather than where the DDL above puts it does not matter.
#
# The UPDATE moves free-text annotations out of `resolution`, where earlier
# versions kept them, into `note`: `resolution` now holds only decisions the
# code acts on. Those annotations never released anything, so the rows reopen.
MIGRATIONS = f"""
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS view_date DATE;
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP;
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS resolution VARCHAR;
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS window_start DATE;
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS window_end DATE;
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS note VARCHAR;
ALTER TABLE watermark ADD COLUMN IF NOT EXISTS article VARCHAR;
ALTER TABLE run_log ADD COLUMN IF NOT EXISTS http_requests INTEGER;
UPDATE quarantine
SET note = coalesce(note, resolution), resolution = NULL, resolved_at = NULL
WHERE resolution IS NOT NULL AND resolution NOT IN ('{ACCEPTED}', '{SUPERSEDED}');
"""


def utc_now() -> datetime:
    """Now, in UTC, with no offset attached - which is what the columns hold.

    DuckDB's TIMESTAMP is timezone-naive, and given an aware datetime it stores
    the session's LOCAL wall time. Dropping the offset after converting to UTC
    stores UTC. TIMESTAMPTZ would need pytz to read back.
    """
    return datetime.now(UTC).replace(tzinfo=None)


def connect(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute(SCHEMA)
    con.execute(MIGRATIONS)
    return con


@contextlib.contextmanager
def opened(db_path: str | Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """A connection for the length of a `with` block, closed however it ends."""
    con = connect(db_path)
    try:
        yield con
    finally:
        con.close()


def get_watermark(con, venue_id: str) -> date | None:
    row = watermark_row(con, venue_id)
    return row[0] if row else None


def watermark_row(con, venue_id: str) -> tuple[date, str | None] | None:
    return con.execute(
        "SELECT last_date, article FROM watermark WHERE venue_id = ?", [venue_id]
    ).fetchone()


def has_any_rows(con, venue_id: str) -> bool:
    row = con.execute("SELECT 1 FROM pageviews WHERE venue_id = ? LIMIT 1", [venue_id]).fetchone()
    return row is not None


def publication_frontier(con, clean: list[quality.CleanRow], end: date) -> date | None:
    """The newest day any venue has ever produced a row for, or None if none has.

    Read from the warehouse as well as from this run, because during a stall this
    run sees nothing at all. Bounded by `end`, so a future-dated row that predates
    the acceptance rules cannot pin the frontier open for ever.
    """
    stored = con.execute(
        "SELECT max(view_date) FROM pageviews WHERE view_date <= ?", [end]
    ).fetchone()[0]
    seen = [row.view_date for row in clean if row.view_date <= end]
    if stored is not None:
        seen.append(stored)
    return max(seen) if seen else None


def blocked_venues(con, venue_ids: list[str]) -> set[str]:
    """Configured venues with an open rejection their watermark has not passed.

    Only those still stand between the venue and a complete warehouse. A
    rejection behind the watermark (a stray day outside the requested window,
    say) is on record but blocks nothing, and one for a venue no longer in
    venues.csv is nobody's problem tonight.
    """
    if not venue_ids:
        return set()
    rows = con.execute(
        "SELECT DISTINCT q.venue_id FROM quarantine q "
        "LEFT JOIN watermark w ON w.venue_id = q.venue_id "
        "WHERE q.resolution IS NULL AND list_contains(?, q.venue_id) "
        "AND (w.last_date IS NULL "
        "     OR coalesce(q.view_date, q.window_start) IS NULL "
        "     OR coalesce(q.view_date, q.window_start) > w.last_date)",
        [venue_ids],
    ).fetchall()
    return {row[0] for row in rows}


def reject_key(venue_id, view_date, rule, window_start) -> tuple:
    """The identity of a rejection: one per venue, day and rule.

    A row with no day is identified by the window it arrived in instead, so an
    accepted one is recognised when the same window is asked for again, and a
    new one in a later window is not mistaken for it.
    """
    return (venue_id, view_date, rule, window_start if view_date is None else None)


_KEY_COLUMNS = "venue_id, view_date, rule, CASE WHEN view_date IS NULL THEN window_start END"
_KEY_MATCH = (
    "venue_id = ? AND view_date IS NOT DISTINCT FROM ? AND rule = ? "
    "AND (view_date IS NOT NULL OR window_start IS NOT DISTINCT FROM ?)"
)


def known_reject_keys(con) -> set[tuple]:
    """Every rejection already on record, as `reject_key` tuples."""
    return set(con.execute(f"SELECT {_KEY_COLUMNS} FROM quarantine").fetchall())


def store_rejects(con, run_id: str, bad: list[quality.BadRow]) -> None:
    """Keep one rejection per key (see `reject_key`); the caller owns the transaction.

    Seeing a rejection again reopens it if it had been superseded - the day
    loaded cleanly once and has gone bad again. An accepted one stays accepted:
    the API keeps answering the same way, and reopening it would undo the
    decision on the very next run. The operator's note survives either way.
    """
    already = known_reject_keys(con)
    now = utc_now()
    for row in bad:
        key = reject_key(row.venue_id, row.view_date, row.rule, row.window_start)
        if key in already:
            con.execute(
                f"UPDATE quarantine SET resolved_at = NULL, resolution = NULL "
                f"WHERE {_KEY_MATCH} AND resolution = ?",
                [*key, SUPERSEDED],
            )
            continue
        con.execute(
            "INSERT INTO quarantine (run_id, venue_id, article, view_date, rule, detail, raw, "
            "seen_at, window_start, window_end) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run_id,
                row.venue_id,
                row.article,
                row.view_date,
                row.rule,
                row.detail,
                row.raw,
                now,
                row.window_start,
                row.window_end,
            ],
        )
        already.add(key)


def supersede(con, run_id: str, now: datetime) -> None:
    """Close the open rejections for every day this run loaded cleanly.

    Without this a one-night upstream glitch kept the run `degraded` for ever,
    after the day had loaded and the watermark had passed it, and the only way
    out was `--accept` - which says the day is never coming, the opposite of
    what happened.
    """
    con.execute(
        "UPDATE quarantine SET resolved_at = ?, resolution = ? "
        "WHERE resolution IS NULL AND view_date IS NOT NULL AND EXISTS ("
        "  SELECT 1 FROM pageviews p WHERE p.run_id = ? "
        "  AND p.venue_id = quarantine.venue_id AND p.view_date = quarantine.view_date)",
        [now, SUPERSEDED, run_id],
    )


# What `resolve` can be pointed at: one day, a range of days, the rejects with
# no day, or all of a venue's rejects.
ALL_DAYS = "all"
Days = date | tuple[date, date] | None | str


def _day_filter(days: Days) -> tuple[str, list]:
    if days is None:
        return "view_date IS NULL", []
    if days == ALL_DAYS:
        return "TRUE", []
    if isinstance(days, tuple):
        first, last = days
        if first > last:
            raise ValueError(f"range {first}..{last} runs backwards")
        return "view_date BETWEEN ? AND ?", [first, last]
    if isinstance(days, date):
        return "view_date = ?", [days]
    raise ValueError(f"not a day, a range, None or {ALL_DAYS!r}: {days!r}")


def resolve(
    con, venue_id: str, days: Days, *, note: str | None = None, accept: bool = False
) -> int:
    """Record a decision about quarantined rows. Returns how many rows it changed.

    - A note is annotation. It releases nothing: the day stays a hard stop,
      because the data is still missing. It survives the row reopening.
    - `accept=True` is the operator saying the day is never coming. The
      watermark may then step over it, which is the only way out for a venue
      whose upstream keeps answering the same wrong thing. Only open rows are
      accepted; the note, if given, is kept with the decision.

    `days` is a date, a (first, last) range, None for the rows a
    `timestamp_parses` failure left with no day, or ALL_DAYS.
    """
    where, params = _day_filter(days)
    note = note.strip() if note else None
    if accept:
        rows = con.execute(
            f"UPDATE quarantine SET resolved_at = ?, resolution = ?, note = coalesce(?, note) "
            f"WHERE venue_id = ? AND {where} AND resolution IS NULL RETURNING rule",
            [utc_now(), ACCEPTED, note, venue_id, *params],
        ).fetchall()
    else:
        if not note:
            raise ValueError("A note is required to annotate a rejection")
        rows = con.execute(
            f"UPDATE quarantine SET note = ? WHERE venue_id = ? AND {where} RETURNING rule",
            [note, venue_id, *params],
        ).fetchall()
    return len(rows)


def accepted_days(con, venue_id: str) -> set[date]:
    """Days an operator has accepted as never arriving, for this venue."""
    return {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT view_date FROM quarantine "
            "WHERE venue_id = ? AND resolution = ? AND view_date IS NOT NULL",
            [venue_id, ACCEPTED],
        ).fetchall()
    }


def accepted_null_windows(con, venue_id: str) -> set[tuple[str, date | None]]:
    """(rule, window_start) of the dateless rejects an operator has accepted."""
    return set(
        con.execute(
            "SELECT DISTINCT rule, window_start FROM quarantine "
            "WHERE venue_id = ? AND resolution = ? AND view_date IS NULL",
            [venue_id, ACCEPTED],
        ).fetchall()
    )


def load(con, run_id, clean, bad, new_watermarks, summary, started_at, settle=None) -> None:
    """Write the run: rows, quarantine, watermarks, status and run log, atomically.

    `new_watermarks` maps venue_id to (last_date, article). `settle(con)`, if
    given, runs after everything but the run log is written, and sets the
    summary's status and note from what is now in the warehouse.

    The run log is inside the transaction with the data it describes, so
    `sum(rows_loaded)` cannot disagree with `count(*)`.
    """
    now = utc_now()
    try:
        con.execute("BEGIN TRANSACTION")
        if clean:
            con.executemany(
                "INSERT OR REPLACE INTO pageviews "
                "(venue_id, article, view_date, views, run_id, loaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(r.venue_id, r.article, r.view_date, r.views, run_id, now) for r in clean],
            )
        store_rejects(con, run_id, bad)
        supersede(con, run_id, now)
        if new_watermarks:
            con.executemany(
                "INSERT OR REPLACE INTO watermark (venue_id, last_date, updated_at, article) "
                "VALUES (?, ?, ?, ?)",
                [(vid, last, now, article) for vid, (last, article) in new_watermarks.items()],
            )
        if settle is not None:
            settle(con)
        write_run_log(con, summary, started_at)
        con.execute("COMMIT")
    except BaseException:
        # BaseException: Ctrl-C and SystemExit are when a half-written
        # transaction is likeliest. The ROLLBACK is guarded so its own failure
        # cannot replace the error that explains what went wrong.
        try:
            con.execute("ROLLBACK")
        except BaseException:
            pass
        raise


def write_run_log(con, summary, started_at: datetime) -> None:
    con.execute(
        "INSERT OR REPLACE INTO run_log "
        "(run_id, started_at, finished_at, status, venues, requests, "
        " rows_fetched, rows_loaded, rows_quarantined, reject_rate, note, http_requests) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            summary.run_id,
            started_at,
            utc_now(),
            summary.status,
            summary.venues,
            summary.requests,
            summary.rows_fetched,
            summary.rows_loaded,
            summary.rows_quarantined,
            summary.reject_rate,
            summary.note,
            summary.http_requests,
        ],
    )
