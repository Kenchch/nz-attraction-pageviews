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
import math
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

# A venue whose watermark is stuck on a bad day asks for everything from that
# day to today, every run, so the range and the rewrite grow without bound. This
# caps it. Hitting the cap means giving up on the days below it, so it is not
# free: the skipped span is written to `run_log.note` rather than passed over
# quietly. 180 days is long enough that only a genuinely stuck venue reaches it.
DEFAULT_MAX_LOOKBACK_DAYS = 180

# The API publishes with a lag. Asking for yesterday usually returns nothing,
# which is not an error but does waste a request on every run.
PUBLICATION_LAG_DAYS = 2

# How old an absent day must be before we believe it was genuinely quiet.
#
# The API omits days with no traffic rather than sending a zero, so an absent day
# means either "nobody looked" or "not published yet" - and no amount of asking
# can tell those apart, because an unpublished day 404s at every width and every
# slice just like a quiet one. Only time separates them. Below this age an absent
# day is treated as unsettled and re-asked next run; above it, as quiet.
#
# The cost is that every venue re-asks for its last few days each night. The
# alternative is picking one meaning and being silently wrong: believing "quiet"
# loses days whenever the lag runs long, and believing "unpublished" strands any
# venue quiet enough to have a gap - measured against the live API,
# `Te_Rerenga_Wairua` has 74 absent days in 90.
TRUST_LAG_DAYS = 7

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
    -- Nullable on purpose: a row rejected by `timestamp_parses` has no day to
    -- record. That is the one case where "which Tuesday" has no answer.
    view_date  DATE,
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

# `CREATE TABLE IF NOT EXISTS` does nothing to a table that already exists, so a
# warehouse built by an earlier version keeps the old shape and the next insert
# fails on the column count. Adding the column is idempotent, and every insert in
# this module names its columns, so it does not matter that a migrated column
# lands at the end rather than in the middle where the DDL above puts it.
MIGRATIONS = """
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS view_date DATE;
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
    con.execute(MIGRATIONS)
    return con


VENUE_COLUMNS = ("venue_id", "venue_name", "region", "wiki_article")


def read_venues(path: str | Path) -> list[Venue]:
    """Parse venues.csv, complaining with a line number when it cannot.

    This file is hand-edited, often in a spreadsheet, so it is the most likely
    thing in the project to be wrong. `Venue(**row)` turned every mistake into
    the same unhelpful TypeError from the dataclass constructor, naming a keyword
    rather than a line. utf-8-sig rather than utf-8 because Excel writes a BOM,
    which would otherwise ride along inside the first column name and make
    `venue_id` mysteriously missing.
    """
    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        missing = [column for column in VENUE_COLUMNS if column not in header]
        if missing:
            raise ValueError(f"{path}: missing column(s) {missing}; found {header}")
        # csv.DictReader keeps the *last* value for a repeated column name, so a
        # duplicated header silently discards the real value - or, for a repeated
        # `venue_id`, files every row under the wrong venue. Only the columns we
        # actually read matter: a spreadsheet export ends lines with stray commas,
        # which DictReader names `''` twice over, and two notes columns of
        # someone else's are still none of our business.
        repeated = sorted({column for column in VENUE_COLUMNS if header.count(column) > 1})
        if repeated:
            raise ValueError(f"{path}: repeated column(s) {repeated} in header {header}")

        venues: list[Venue] = []
        seen: dict[str, int] = {}
        for line, row in enumerate(reader, start=2):
            fields = {column: (row.get(column) or "").strip() for column in VENUE_COLUMNS}
            # NFC on the title only, so a macron typed as `u` + combining macron
            # matches the API's single codepoint (see `quality.normalise_title`).
            # Deliberately not on `venue_id`: it is the key every stored row is
            # written under, so re-spelling it would orphan an existing
            # warehouse's watermark and duplicate its history under a second id
            # that looks identical on screen. It buys nothing there either - the
            # id is ours, and never compared with anything the API sends.
            fields["wiki_article"] = quality.normalise_title(fields["wiki_article"])
            blank = [column for column, value in fields.items() if not value]
            if blank:
                raise ValueError(f"{path} line {line}: empty {blank}")

            venue_id = fields["venue_id"]
            if venue_id in seen:
                raise ValueError(
                    f"{path} line {line}: duplicate venue_id {venue_id!r}, "
                    f"already used on line {seen[venue_id]}"
                )
            seen[venue_id] = line
            # Only the four columns we know about, so an extra one someone added
            # for their own notes is tolerated rather than fatal.
            venues.append(Venue(**fields))

    if not venues:
        raise ValueError(f"{path} has no venues")
    return venues


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
    trusted_end: date,
) -> date | None:
    """How far this venue may advance. None means leave the watermark alone.

    The watermark promises that every day up to it has been dealt with, so it may
    not step over a day we failed to load. Two kinds of day fail that test, and
    they need opposite treatment.

    A quarantined day is a hard stop, whatever its age. The row is in
    `quarantine`, but the gate is a rate across every venue, so one venue's bad
    patch can sit under the threshold and pass; advancing anyway would turn the
    quarantine into a record of data we permanently lost. The watermark therefore
    stops the day before the earliest rejected one.

    An *absent* day is the ambiguous one, because the API omits days with no
    traffic instead of sending a zero. Absent means "quiet" or "not published
    yet" and nothing in the response distinguishes them. Two things do:

    - Anything before a day that did arrive is settled. Publication runs in date
      order, so a later day arriving proves the earlier one was published, and
      absence there can only mean quiet. Hence the watermark may always advance
      to the last day actually loaded, holes behind it included.
    - Past that, only age helps. An absent day older than `trusted_end` is taken
      as quiet; a more recent one is left alone and asked for again next run,
      which is what stops a long publication lag becoming a permanent hole.
    """
    if any(row.view_date is None for row in venue_bad):
        # A row rejected by `timestamp_parses` cannot be pinned to a day, so we
        # cannot know which day to stop before. Advancing past an unknown day is
        # the one thing we must not do.
        return None

    # Only a bad day *inside* the window is a day this run promised anything
    # about. A stray date outside it is still recorded in `quarantine`, but the
    # watermark passed that day runs ago; stopping for it permanently neither
    # recovers the day nor stops recurring, and would leave the venue re-fetching
    # its whole range every night until it hit the lookback cap.
    in_window = [row.view_date for row in venue_bad if start <= row.view_date <= end]
    ceiling = min(in_window) - timedelta(days=1) if in_window else end

    trusted = min(end, trusted_end)
    loaded = [row.view_date for row in venue_clean]
    frontier = max(max(loaded), trusted) if loaded else trusted

    frontier = min(frontier, ceiling)
    return frontier if frontier >= start else None


def run(
    con,
    venues: list[Venue],
    *,
    today: date | None = None,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    backfill_days: int = DEFAULT_BACKFILL_DAYS,
    max_reject_rate: float = DEFAULT_MAX_REJECT_RATE,
    max_lookback_days: int = DEFAULT_MAX_LOOKBACK_DAYS,
    trust_lag_days: int = TRUST_LAG_DAYS,
    fetch=client.fetch_window,
) -> RunSummary:
    # Validated rather than trusted, because the failure modes are silent. A
    # max_reject_rate of nan disables the gate outright - every comparison
    # against nan is False, so no rate is ever "too high" and a run that
    # rejected everything still reports ok. A backfill_days of 0 asks for an
    # empty range and looks like a venue with no traffic.
    for name, value in (
        ("chunk_days", chunk_days),
        ("backfill_days", backfill_days),
        ("max_lookback_days", max_lookback_days),
        ("trust_lag_days", trust_lag_days),
    ):
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValueError(f"{name} must be an integer >= 1, got {value!r}")
    if not isinstance(max_reject_rate, (int, float)) or isinstance(max_reject_rate, bool):
        raise ValueError(f"max_reject_rate must be a number, got {max_reject_rate!r}")
    if not math.isfinite(max_reject_rate) or not 0.0 <= max_reject_rate <= 1.0:
        raise ValueError(
            f"max_reject_rate must be a finite fraction in [0, 1], got {max_reject_rate!r}. "
            f"nan in particular disables the gate silently: every comparison against it "
            f"is False, so no run is ever rejected."
        )

    today = today or datetime.now(UTC).date()
    end = today - timedelta(days=PUBLICATION_LAG_DAYS)
    floor = end - timedelta(days=max_lookback_days - 1)
    calendar_trust_line = today - timedelta(days=trust_lag_days)

    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(UTC)
    summary = RunSummary(run_id=run_id, status="running", venues=len(venues))

    clean: list[quality.CleanRow] = []
    bad: list[quality.BadRow] = []
    new_watermarks: dict[str, date] = {}
    stalled: list[str] = []
    silent: list[str] = []
    fetched: list[tuple[Venue, date, list[quality.CleanRow], list[quality.BadRow]]] = []

    try:
        for venue in venues:
            start = start_date_for(con, venue, end, backfill_days)
            if start < floor:
                # Stuck on a bad day for longer than we are willing to re-ask.
                # Give up on the span below the floor, but say so out loud.
                stalled.append(f"{venue.venue_id}: gave up on {(floor - start).days} days")
                start = floor
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
            fetched.append((venue, start, venue_clean, venue_bad))

        # How far the upstream has demonstrably published. Publication is a
        # property of the API, not of one article, so the newest day seen for any
        # venue - this run or in any run before it - is evidence for all of them.
        #
        # The calendar trust line alone was a fixed bet that the lag never runs
        # longer than `trust_lag_days`. Once an absent day drifted past that line
        # the watermark stepped over it whether or not anything had ever been
        # published for that day, so a stall longer than
        # `trust_lag_days - PUBLICATION_LAG_DAYS` lost days permanently, silently,
        # with the run still reporting `ok`. Bounding the line by what has
        # actually been seen makes a stall cost a re-request instead.
        #
        # With no evidence anywhere - an empty warehouse and a run that fetched
        # nothing - the calendar line stands, so a set of venues that are all
        # genuinely quiet still makes progress. That is the failure in the other
        # direction, and it is the one the trust lag was introduced to avoid.
        observed = _publication_frontier(con, clean, end)
        trusted_end = calendar_trust_line
        if observed is not None:
            trusted_end = min(trusted_end, observed)

        for venue, start, venue_clean, venue_bad in fetched:
            frontier = _venue_watermark(start, end, venue_clean, venue_bad, trusted_end)
            # Never move a watermark backwards. A venue already current past the
            # trust line would otherwise be dragged back to it and re-fetch the
            # same days every night.
            current = get_watermark(con, venue.venue_id)
            if frontier is not None and (current is None or frontier > current):
                new_watermarks[venue.venue_id] = frontier

            # Two shapes worth naming, both of which look like `ok` otherwise.
            #
            # A venue that asked for its whole range and has never produced a row
            # is what a typo in `venues.csv` makes: the article does not exist,
            # every width and slice 404s, the window verifies as genuinely empty,
            # and the run retires the backfill. A venue that is simply quiet has
            # the same shape, and deserves the same second look.
            #
            # A venue whose watermark is falling further behind `end` every night
            # is stuck - on a day it cannot load, or on a frontier that is not
            # moving. Holding is the intended behaviour, since it re-requests
            # rather than losing the days, but it should not be something you have
            # to notice for yourself. A healthy venue sits within a day or two of
            # `end`, so the trust lag is a generous threshold.
            effective = new_watermarks.get(venue.venue_id, current)
            if not venue_clean and not venue_bad and not _has_any_rows(con, venue.venue_id):
                silent.append(f"{venue.venue_id}: no rows ever, check {venue.wiki_article!r}")
            elif effective is not None and (end - effective).days > trust_lag_days:
                silent.append(f"{venue.venue_id}: {(end - effective).days} days behind")

        summary.rows_quarantined = len(bad)
        # Record the rate before the gate can raise, so a failed run logs the
        # number that explains why it failed rather than 0.0.
        summary.reject_rate = quality.reject_rate(summary.rows_fetched, summary.rows_quarantined)
        summary.note = "; ".join([*stalled, *silent])
        quality.enforce_gate(summary.rows_fetched, summary.rows_quarantined, max_reject_rate)

        # Set before the load, because the load is what writes the run log now.
        summary.rows_loaded = len(clean)
        summary.status = "ok"
        _load(con, run_id, clean, bad, new_watermarks, summary, started_at)

    except Exception as exc:
        summary.status = "failed"
        summary.rows_loaded = 0
        # Append rather than replace. A run that abandoned days and *then* failed
        # is the run whose note is worth most, and overwriting it lost the half
        # that does not turn up in the traceback. `stalled` is read here rather
        # than from summary.note because the failure may have come mid-loop,
        # before the note was composed at all.
        summary.note = "; ".join([*stalled, *silent, f"{type(exc).__name__}: {exc}"])
        # Nothing was loaded on this path - the transaction rolled back, or never
        # opened - so this write stands alone and cannot contradict the data.
        _write_run_log(con, summary, started_at)
        raise

    return summary


def _publication_frontier(con, clean: list[quality.CleanRow], end: date) -> date | None:
    """The newest day any venue has ever produced a row for, or None if none has.

    Read from the warehouse as well as from this run, because during a stall this
    run sees nothing at all: every venue's watermark already consumed everything
    published, so each one asks for days beyond the frontier and gets an empty
    answer. Judging on this run alone would fall back to the calendar line in
    exactly the case the frontier exists to cover.
    """
    stored = con.execute(
        # Bounded by `end`: a row dated in the future - drift that predates the
        # acceptance rules, or a hand-inserted one - would otherwise be the newest
        # day in the table for ever, pinning the frontier above the calendar line
        # and quietly turning this whole mechanism back off.
        "SELECT max(view_date) FROM pageviews WHERE view_date <= ?",
        [end],
    ).fetchone()[0]
    seen = [row.view_date for row in clean if row.view_date <= end]
    if stored is not None:
        seen.append(stored)
    return max(seen) if seen else None


def _has_any_rows(con, venue_id: str) -> bool:
    row = con.execute("SELECT 1 FROM pageviews WHERE venue_id = ? LIMIT 1", [venue_id]).fetchone()
    return row is not None


def _load(con, run_id, clean, bad, new_watermarks, summary, started_at) -> None:
    """Write the run: rows, quarantine, watermarks and the run log, atomically.

    The run log is inside the transaction with the data it describes. Written
    afterwards, as its own statement, it could be lost while the data survived -
    a crash, or a `run_log` that has drifted a column - leaving rows in the
    warehouse that no run ever claims to have loaded, and `sum(rows_loaded)`
    quietly disagreeing with `count(*)`. A failed run has no data to disagree
    with, so `run` writes its log separately on that path.
    """
    now = datetime.now(UTC)
    try:
        con.execute("BEGIN TRANSACTION")
        if clean:
            con.executemany(
                "INSERT OR REPLACE INTO pageviews "
                "(venue_id, article, view_date, views, run_id, loaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(r.venue_id, r.article, r.view_date, r.views, run_id, now) for r in clean],
            )
        if bad:
            con.executemany(
                "INSERT INTO quarantine "
                "(run_id, venue_id, article, view_date, rule, detail, raw, seen_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (run_id, r.venue_id, r.article, r.view_date, r.rule, r.detail, r.raw, now)
                    for r in bad
                ],
            )
        if new_watermarks:
            con.executemany(
                "INSERT OR REPLACE INTO watermark (venue_id, last_date, updated_at) "
                "VALUES (?, ?, ?)",
                [(vid, last, now) for vid, last in new_watermarks.items()],
            )
        _write_run_log(con, summary, started_at)
        con.execute("COMMIT")
    except Exception:
        # Guarded: a ROLLBACK that itself fails (the BEGIN never took, say) must
        # not replace the error that explains what actually went wrong.
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise


def _write_run_log(con, summary: RunSummary, started_at: datetime) -> None:
    con.execute(
        "INSERT OR REPLACE INTO run_log "
        "(run_id, started_at, finished_at, status, venues, requests, "
        " rows_fetched, rows_loaded, rows_quarantined, reject_rate, note) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
