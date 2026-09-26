"""Incremental ingest of NZ attraction pageviews into DuckDB.

Shape of a run:

    read venues.csv
      -> plan: per venue, the start date from its watermark        (reads the warehouse)
      -> fetch: split each range into windows, fetch, apply the    (no warehouse connection)
         acceptance criteria; a venue whose request fails is set
         aside with its watermark untouched
      -> hold any venue whose NEW rejections are over the ceiling
      -> load: rows, quarantine, watermarks, status, run log       (one transaction)

The load is a single transaction. Either the whole run lands or none of it does,
so a crash halfway through eight venues cannot leave three venues a day ahead of
the other five. `run_at` opens the warehouse only for the plan and the load, so
the minutes spent on the network do not lock out `resolve` or a BI reader.

The status is `ok` only when nothing is unresolved: no venue failed, was held,
gave up days to the lookback cap, has never produced a row, or still has an
open rejection its watermark has not passed. Otherwise it is `degraded`.
"""

from __future__ import annotations

import contextlib
import csv
import functools
import logging
import math
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from . import client, quality

log = logging.getLogger(__name__)

UTC = timezone.utc  # datetime.UTC only exists from 3.11; this keeps 3.10 working

DEFAULT_CHUNK_DAYS = 30
DEFAULT_BACKFILL_DAYS = 90
DEFAULT_MAX_REJECT_RATE = 0.05

# A venue whose watermark is stuck on a bad day asks for everything from that
# day to today, every run. This caps it. Hitting the cap means giving up on the
# days below it, so the venue is named in `run_log.note` and the run is
# `degraded`, rather than the days passing over quietly.
DEFAULT_MAX_LOOKBACK_DAYS = 180

# One run's ceiling on HTTP calls. A normal night is a few dozen; a first
# backfill against an upstream answering `200 {"items": []}` everywhere is about
# 1,400 (see DESIGN.md, Limits). Past this the remaining venues are not asked,
# are named, and keep their watermarks.
DEFAULT_MAX_HTTP_REQUESTS = 2000

# Upper bound for any day-count parameter. Ten years is past anything useful and
# short of the date arithmetic overflowing.
MAX_DAYS = 3650

# The API publishes with a lag. Asking for yesterday usually returns nothing,
# which is not an error but does waste a request on every run.
PUBLICATION_LAG_DAYS = 2

# How old an absent day must be before we believe it was genuinely quiet.
#
# The API omits days with no traffic rather than sending a zero, so an absent
# day means either "nobody looked" or "not published yet", and no amount of
# asking can tell those apart. Only time separates them. Below this age an
# absent day is unsettled and re-asked next run; above it, quiet.
TRUST_LAG_DAYS = 7

# Two venues in a row failing to connect, before any venue has succeeded, is
# the network rather than the venues. The rest are not attempted.
TRANSPORT_BREAKER = 2

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


@dataclass(frozen=True)
class Venue:
    venue_id: str
    venue_name: str
    region: str
    wiki_article: str


@dataclass(frozen=True)
class Plan:
    venue: Venue
    start: date
    gave_up_days: int = 0


@dataclass
class Fetched:
    venue: Venue
    start: date
    clean: list[quality.CleanRow]
    bad: list[quality.BadRow]


@dataclass
class FetchOutcome:
    fetched: list[Fetched] = field(default_factory=list)
    failures: dict[str, str] = field(default_factory=dict)  # venue_id -> note


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
    http_requests: int | None = None


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


VENUE_COLUMNS = ("venue_id", "venue_name", "region", "wiki_article")


def read_venues(path: str | Path) -> list[Venue]:
    """Parse venues.csv, complaining with a line number when it cannot.

    The file is hand-edited, often in a spreadsheet. utf-8-sig because Excel
    writes a BOM, which would otherwise ride along inside `venue_id`. A line
    that is blank in all four columns - what a spreadsheet leaves at the end -
    is skipped.
    """
    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        missing = [column for column in VENUE_COLUMNS if column not in header]
        if missing:
            raise ValueError(f"{path}: missing column(s) {missing}; found {header}")
        # DictReader keeps the LAST value for a repeated column name, so a
        # duplicated `venue_id` would file every row under the wrong venue.
        # Only the columns we read matter: trailing commas make `''` columns.
        repeated = sorted({column for column in VENUE_COLUMNS if header.count(column) > 1})
        if repeated:
            raise ValueError(f"{path}: repeated column(s) {repeated} in header {header}")

        venues: list[Venue] = []
        seen: dict[str, int] = {}
        for line, row in enumerate(reader, start=2):
            fields = {column: (row.get(column) or "").strip() for column in VENUE_COLUMNS}
            if not any(fields.values()):
                continue
            # The title is normalised to MediaWiki's canonical spelling; the id
            # is not. It is the key every stored row is written under, and is
            # never compared with anything the API sends.
            fields["wiki_article"] = quality.normalise_title(fields["wiki_article"])
            blank = [column for column, value in fields.items() if not value]
            if blank:
                raise ValueError(f"{path} line {line}: empty {blank}")
            bad_chars = quality.invalid_title_chars(fields["wiki_article"])
            if bad_chars:
                raise ValueError(
                    f"{path} line {line}: wiki_article {fields['wiki_article']!r} contains "
                    f"{''.join(bad_chars)!r}, which no Wikipedia title can"
                )

            venue_id = fields["venue_id"]
            if venue_id in seen:
                raise ValueError(
                    f"{path} line {line}: duplicate venue_id {venue_id!r}, "
                    f"already used on line {seen[venue_id]}"
                )
            seen[venue_id] = line
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
    row = _watermark_row(con, venue_id)
    return row[0] if row else None


def _watermark_row(con, venue_id: str) -> tuple[date, str | None] | None:
    return con.execute(
        "SELECT last_date, article FROM watermark WHERE venue_id = ?", [venue_id]
    ).fetchone()


def start_date_for(con, venue: Venue, end: date, backfill_days: int) -> date:
    """Resume the day after the watermark, or backfill on first sight of a venue.

    Two watermarks are not a record of coverage for this venue, and the whole
    backfill window is asked for again:

    - One for a venue that has never produced a row. The frontier is a property
      of the API, so healthy siblings walk a dead venue's watermark along the
      trust line; a typo corrected in venues.csv would otherwise recover only
      the days since.
    - One recorded for a different title. Coverage of a redirect says nothing
      about the canonical article that replaced it.
    """
    row = _watermark_row(con, venue.venue_id)
    backfill = end - timedelta(days=backfill_days - 1)
    if row is None:
        return backfill
    watermark, article = row
    retitled = article is not None and article != venue.wiki_article
    if retitled or not _has_any_rows(con, venue.venue_id):
        return min(watermark + timedelta(days=1), backfill)
    return watermark + timedelta(days=1)


def _venue_watermark(
    start: date,
    end: date,
    venue_clean: list[quality.CleanRow],
    venue_bad: list[quality.BadRow],
    trusted_end: date | None,
    accepted: set[date] | None = None,
    accepted_null: set[tuple[str, date | None]] | None = None,
) -> date | None:
    """How far this venue may advance. None means leave the watermark alone.

    The watermark promises that every day up to it has been dealt with, so it may
    not step over a day we failed to load.

    - A rejected day inside the requested range is a hard stop, whatever its
      age, unless an operator has accepted it. A rejected day outside the range
      is recorded but does not stop anything: the watermark passed it runs ago.
    - A rejected row with no parseable day stops the watermark before the
      window it arrived in, since any day of that window could be the one it
      was for - unless an operator has accepted that rule for that window.
    - An absent day before one that arrived is settled: publication runs in date
      order. (`client.fetch_window` has already re-asked such holes narrowly.)
    - An absent day after the last one that arrived is settled only once it is
      older than `trusted_end`. With no evidence anywhere that the upstream
      has published (`trusted_end` is None), nothing is settled: in that case
      no venue loaded anything either, so the watermark stays where it is.
    """
    accepted = accepted or set()
    accepted_null = accepted_null or set()
    stops = []
    for row in venue_bad:
        if row.view_date is None:
            if (row.rule, row.window_start) not in accepted_null:
                stops.append(row.window_start or start)
        elif start <= row.view_date <= end and row.view_date not in accepted:
            stops.append(row.view_date)
    ceiling = min(stops) - timedelta(days=1) if stops else end

    if trusted_end is None:
        return None
    loaded = [row.view_date for row in venue_clean]
    trusted = min(end, trusted_end)
    frontier = max(max(loaded), trusted) if loaded else trusted
    frontier = min(frontier, ceiling)
    return frontier if frontier >= start else None


def validate_params(
    chunk_days: int,
    backfill_days: int,
    max_reject_rate: float,
    max_lookback_days: int,
    trust_lag_days: int,
    max_http_requests: int | None = DEFAULT_MAX_HTTP_REQUESTS,
) -> None:
    """Refuse parameters whose failure would be silent.

    A max_reject_rate of nan disables the hold outright, since every comparison
    against nan is False. A backfill_days of 0 asks for an empty range and looks
    like a quiet venue. A backfill longer than the lookback cap would be cut to
    the cap and the difference reported as "gave up".
    """
    for name, value in (
        ("chunk_days", chunk_days),
        ("backfill_days", backfill_days),
        ("max_lookback_days", max_lookback_days),
        ("trust_lag_days", trust_lag_days),
    ):
        if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= MAX_DAYS:
            raise ValueError(f"{name} must be an integer in 1..{MAX_DAYS}, got {value!r}")
    if backfill_days > max_lookback_days:
        raise ValueError(
            f"backfill_days {backfill_days} is more than max_lookback_days "
            f"{max_lookback_days}; raise the lookback or ask for fewer days"
        )
    if max_http_requests is not None and (
        not isinstance(max_http_requests, int)
        or isinstance(max_http_requests, bool)
        or max_http_requests < 1
    ):
        raise ValueError(f"max_http_requests must be an integer >= 1, got {max_http_requests!r}")
    if not isinstance(max_reject_rate, (int, float)) or isinstance(max_reject_rate, bool):
        raise ValueError(f"max_reject_rate must be a number, got {max_reject_rate!r}")
    # Range first: math.isfinite converts to float, and 10**1000 overflows.
    # nan fails the range test too.
    if not 0.0 <= max_reject_rate <= 1.0 or (
        isinstance(max_reject_rate, float) and not math.isfinite(max_reject_rate)
    ):
        raise ValueError(
            f"max_reject_rate must be a finite fraction in [0, 1], got {max_reject_rate!r}. "
            f"nan in particular disables the hold silently: every comparison against it "
            f"is False, so no venue is ever held."
        )


def decide_status(
    *,
    held: set[str],
    blocked: set[str],
    failed: set[str],
    gave_up: set[str],
    never_produced: set[str],
) -> tuple[str, list[str]]:
    """`ok` means the warehouse is complete for every venue; anything else is `degraded`.

    Returns the status and the venues that stop it being `ok`.
    """
    unresolved = sorted(held | blocked | failed | gave_up | never_produced)
    return ("degraded" if unresolved else "ok"), unresolved


def _plan(con, venues: list[Venue], *, end: date, floor: date, backfill_days: int) -> list[Plan]:
    plans = []
    for venue in venues:
        start = start_date_for(con, venue, end, backfill_days)
        gave_up = 0
        if start < floor:
            # Stuck on a bad day for longer than we are willing to re-ask.
            gave_up = (floor - start).days
            start = floor
        if start <= end:
            plans.append(Plan(venue, start, gave_up))
    return plans


def _fetch_all(
    plans: list[Plan],
    *,
    end: date,
    chunk_days: int,
    fetch,
    summary: RunSummary,
) -> FetchOutcome:
    """Fetch and check every planned venue. Touches no database."""
    outcome = FetchOutcome()
    transport_failures = 0
    tripped: str | None = None

    for plan in plans:
        venue = plan.venue
        if tripped:
            outcome.failures[venue.venue_id] = (
                f"{venue.venue_id}: not attempted, {tripped}; watermark held"
            )
            continue

        windows = plan_windows(plan.start, end, chunk_days)
        log.info(
            "%s: asking for %s..%s in %d window(s)", venue.venue_id, plan.start, end, len(windows)
        )
        venue_clean: list[quality.CleanRow] = []
        venue_bad: list[quality.BadRow] = []
        fetched_rows = 0
        try:
            for window_start, window_end in windows:
                # Counted before the call, so a window whose request raises is
                # still counted: the field is "windows asked for".
                summary.requests += 1
                items = fetch(venue.wiki_article, window_start, window_end)
                fetched_rows += len(items)
                good, rejected = quality.check_window(
                    items,
                    venue_id=venue.venue_id,
                    article=venue.wiki_article,
                    start=window_start,
                    end=window_end,
                )
                venue_clean.extend(good)
                venue_bad.extend(rejected)
        except client.ApiError as exc:
            # The venue's earlier windows from this run are discarded with it.
            log.warning("%s: %s; watermark held", venue.venue_id, exc)
            outcome.failures[venue.venue_id] = f"{venue.venue_id}: {exc}; watermark held"
            if isinstance(exc, client.RequestBudgetExceeded):
                tripped = "the request budget is spent"
            elif isinstance(exc, client.TransportError) and not outcome.fetched:
                transport_failures += 1
                if transport_failures >= TRANSPORT_BREAKER:
                    tripped = f"the network looks unreachable ({transport_failures} venues failed)"
            continue

        summary.rows_fetched += fetched_rows
        outcome.fetched.append(Fetched(venue, plan.start, venue_clean, venue_bad))

    return outcome


def _advance_watermarks(
    con,
    fetched: list[Fetched],
    *,
    end: date,
    trusted_end: date | None,
    trust_lag_days: int,
) -> tuple[dict[str, date], set[str], list[str]]:
    """New watermarks, the venues that have never produced a row, and notes."""
    new_watermarks: dict[str, date] = {}
    never_produced: set[str] = set()
    notes: list[str] = []
    for f in fetched:
        venue = f.venue
        frontier = _venue_watermark(
            f.start,
            end,
            f.clean,
            f.bad,
            trusted_end,
            accepted=accepted_days(con, venue.venue_id),
            accepted_null=accepted_null_windows(con, venue.venue_id),
        )
        current, stored_article = _watermark_row(con, venue.venue_id) or (None, None)
        retitled = stored_article is not None and stored_article != venue.wiki_article
        if retitled:
            notes.append(
                f"{venue.venue_id}: title changed from {stored_article!r} to "
                f"{venue.wiki_article!r}, re-fetched from {f.start}; rows before that "
                f"are still for the old title"
            )
        # Never backwards - except when the coverage it records is for another
        # title, in which case it is not coverage of this one at all.
        if frontier is not None and (current is None or retitled or frontier > current):
            new_watermarks[venue.venue_id] = frontier
        elif current is not None and stored_article is None:
            new_watermarks[venue.venue_id] = current  # record which title it is for

        has_history = _has_any_rows(con, venue.venue_id) and not retitled
        if not has_history and not f.clean:
            # A typo in venues.csv, an article that 404s everywhere, or a title
            # that resolves to a different article so every row is rejected.
            never_produced.add(venue.venue_id)
            detail = f", {len(f.bad)} row(s) rejected" if f.bad else ""
            notes.append(f"{venue.venue_id}: no rows ever{detail}, check {venue.wiki_article!r}")
            continue

        # Measured from the last day this venue actually produced, not from its
        # watermark, which walks the trust line on other venues' evidence and
        # so can never fall more than trust_lag_days behind.
        last = con.execute(
            "SELECT max(view_date) FROM pageviews WHERE venue_id = ?", [venue.venue_id]
        ).fetchone()[0]
        if f.clean:
            newest = max(r.view_date for r in f.clean)
            last = newest if last is None else max(last, newest)
        if last is not None and (end - last).days > trust_lag_days:
            effective = new_watermarks.get(venue.venue_id, current)
            notes.append(
                f"{venue.venue_id}: no rows for {(end - last).days} days "
                f"(watermark {effective}), check {venue.wiki_article!r}"
            )

    return new_watermarks, never_produced, notes


def _reject_key(venue_id, view_date, rule, window_start) -> tuple:
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


def _is_accepted(row: quality.BadRow, days: set[date], null_windows: set) -> bool:
    if row.view_date is None:
        return (row.rule, row.window_start) in null_windows
    return row.view_date in days


def _apply_gate(
    con,
    fetched: list[Fetched],
    bad: list[quality.BadRow],
    max_reject_rate: float,
) -> tuple[set[str], list[str]]:
    """Hold each venue whose NEWLY rejected share is over the ceiling.

    A held venue loads nothing and its watermark stays put; its rejected rows
    still reach `quarantine`. The other venues load.

    New, not raw: once a watermark stops before a permanently bad day, the next
    window starts at that day and is 1 of 1 rejected, which would hold the venue
    for ever for a fault costing one day. A standing rejection is the
    watermark's job and the status's to report. So the hold lifts on the next
    run, once the rejections are on record; the watermark still does not pass
    the bad days until they load cleanly or are accepted.
    """
    notes: list[str] = []
    already = set(con.execute(f"SELECT {_KEY_COLUMNS} FROM quarantine").fetchall())
    novel = [
        r
        for r in bad
        if _reject_key(r.venue_id, r.view_date, r.rule, r.window_start) not in already
    ]
    if len(novel) != len(bad):
        notes.append(
            f"{len(bad) - len(novel)} of {len(bad)} rejected rows were already quarantined"
        )

    held = set()
    for f in fetched:
        venue_id = f.venue.venue_id
        days, null_windows = accepted_days(con, venue_id), accepted_null_windows(con, venue_id)
        seen = len(f.clean) + len(f.bad)
        fresh = sum(
            1 for r in novel if r.venue_id == venue_id and not _is_accepted(r, days, null_windows)
        )
        if seen and quality.reject_rate(seen, fresh) > max_reject_rate:
            held.add(venue_id)
            notes.append(f"{venue_id}: {fresh}/{seen} newly rejected, above the ceiling, held")
    return held, notes


def _publication_frontier(con, clean: list[quality.CleanRow], end: date) -> date | None:
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


def _has_any_rows(con, venue_id: str) -> bool:
    row = con.execute("SELECT 1 FROM pageviews WHERE venue_id = ? LIMIT 1", [venue_id]).fetchone()
    return row is not None


def _blocked_venues(con, venue_ids: list[str]) -> set[str]:
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


@dataclass
class _Report:
    """The note's parts, kept apart so a failure part-way can still say all of them."""

    gave_up: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    other: list[str] = field(default_factory=list)

    def lines(self) -> list[str]:
        return [*self.gave_up, *self.failures, *self.other]


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
    max_http_requests: int | None = DEFAULT_MAX_HTTP_REQUESTS,
    fetch=None,
    opener=client.http_get,
) -> RunSummary:
    """One ingest run on an open connection, which is held throughout.

    `fetch(article, start, end)` defaults to `client.fetch_window` over `opener`,
    counted and bounded by `max_http_requests`. An injected `fetch` is not
    counted, and `http_requests` is then NULL.
    """
    return _run(
        lambda: contextlib.nullcontext(con),
        venues,
        today=today,
        chunk_days=chunk_days,
        backfill_days=backfill_days,
        max_reject_rate=max_reject_rate,
        max_lookback_days=max_lookback_days,
        trust_lag_days=trust_lag_days,
        max_http_requests=max_http_requests,
        fetch=fetch,
        opener=opener,
    )


def run_at(db_path: str | Path, venues: list[Venue], **options) -> RunSummary:
    """Like `run`, but opens the warehouse only to plan and to load.

    DuckDB lets one process write a file at a time, and a read-write connection
    locks out every other process, readers included. Holding it through the
    fetch locked out `resolve` and any BI tool for as long as the network took.
    """
    return _run(lambda: _opened(db_path), venues, **options)


@contextlib.contextmanager
def _opened(db_path: str | Path) -> Iterator[duckdb.DuckDBPyConnection]:
    con = connect(db_path)
    try:
        yield con
    finally:
        con.close()


def _run(
    session: Callable[[], contextlib.AbstractContextManager],
    venues: list[Venue],
    *,
    today: date | None = None,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    backfill_days: int = DEFAULT_BACKFILL_DAYS,
    max_reject_rate: float = DEFAULT_MAX_REJECT_RATE,
    max_lookback_days: int = DEFAULT_MAX_LOOKBACK_DAYS,
    trust_lag_days: int = TRUST_LAG_DAYS,
    max_http_requests: int | None = DEFAULT_MAX_HTTP_REQUESTS,
    fetch=None,
    opener=client.http_get,
) -> RunSummary:
    validate_params(
        chunk_days,
        backfill_days,
        max_reject_rate,
        max_lookback_days,
        trust_lag_days,
        max_http_requests,
    )
    today = today or datetime.now(UTC).date()
    end = today - timedelta(days=PUBLICATION_LAG_DAYS)
    floor = end - timedelta(days=max_lookback_days - 1)
    calendar_trust_line = today - timedelta(days=trust_lag_days)

    counter = None
    if fetch is None:
        counter = client.CountingOpener(opener, budget=max_http_requests)
        fetch = functools.partial(client.fetch_window, opener=counter)

    run_id = uuid.uuid4().hex[:12]
    started_at = utc_now()
    summary = RunSummary(run_id=run_id, status="running", venues=len(venues))
    report = _Report()

    try:
        with session() as con:
            plans = _plan(con, venues, end=end, floor=floor, backfill_days=backfill_days)
        report.gave_up = [
            f"{p.venue.venue_id}: gave up on {p.gave_up_days} days" for p in plans if p.gave_up_days
        ]

        outcome = _fetch_all(plans, end=end, chunk_days=chunk_days, fetch=fetch, summary=summary)
        summary.http_requests = counter.count if counter else None
        report.failures = list(outcome.failures.values())
        if venues and len(outcome.failures) == len(venues):
            raise client.ApiError(f"all {len(venues)} requested venues failed")

        with session() as con:
            _finish(
                con,
                venues,
                plans,
                outcome,
                summary=summary,
                report=report,
                started_at=started_at,
                end=end,
                calendar_trust_line=calendar_trust_line,
                trust_lag_days=trust_lag_days,
                max_reject_rate=max_reject_rate,
            )

    except Exception as exc:
        summary.status = "failed"
        summary.rows_loaded = 0
        summary.http_requests = counter.count if counter else None
        # Everything the run had to say, then what stopped it: a run that gave
        # up on days and then failed is the run whose note is worth most.
        summary.note = "; ".join([*report.lines(), f"{type(exc).__name__}: {exc}"])
        # Nothing was loaded on this path - the transaction rolled back, or
        # never opened - so this write stands alone. It can fail too (the
        # warehouse may be why the run failed), and must not hide the reason.
        try:
            with session() as con:
                _write_run_log(con, summary, started_at)
        except Exception as log_exc:
            log.error("could not record the failed run %s: %s", run_id, log_exc)
        raise

    return summary


def _finish(
    con,
    venues: list[Venue],
    plans: list[Plan],
    outcome: FetchOutcome,
    *,
    summary: RunSummary,
    report: _Report,
    started_at: datetime,
    end: date,
    calendar_trust_line: date,
    trust_lag_days: int,
    max_reject_rate: float,
) -> None:
    clean = [row for f in outcome.fetched for row in f.clean]
    bad = [row for f in outcome.fetched for row in f.bad]

    # How far the upstream has demonstrably published. Publication is a property
    # of the API, not of one article, so the newest day any venue has produced -
    # this run or before - is evidence for all of them. The calendar line alone
    # was a bet that the lag never outruns it; bounded by what has been seen, a
    # stall costs a re-request instead of the days. With no evidence anywhere,
    # nothing is trusted and no watermark moves.
    observed = _publication_frontier(con, clean, end)
    trusted_end = None if observed is None else min(calendar_trust_line, observed)

    new_watermarks, never_produced, watermark_notes = _advance_watermarks(
        con, outcome.fetched, end=end, trusted_end=trusted_end, trust_lag_days=trust_lag_days
    )

    summary.rows_quarantined = len(bad)
    summary.reject_rate = quality.reject_rate(summary.rows_fetched, summary.rows_quarantined)

    held, gate_notes = _apply_gate(con, outcome.fetched, bad, max_reject_rate)
    clean = [row for row in clean if row.venue_id not in held]
    for venue_id in held:
        new_watermarks.pop(venue_id, None)
    summary.rows_loaded = len(clean)
    report.other = [*watermark_notes, *gate_notes]

    def settle(con) -> None:
        # Inside the transaction, after tonight's rejects and watermarks are
        # written: a venue that first rejects a day tonight is unresolved
        # tonight, not from tomorrow.
        status, unresolved = decide_status(
            held=held,
            blocked=_blocked_venues(con, [v.venue_id for v in venues]),
            failed=set(outcome.failures),
            gave_up={p.venue.venue_id for p in plans if p.gave_up_days},
            never_produced=never_produced,
        )
        summary.status = status
        lines = report.lines()
        if unresolved:
            lines.append(f"unresolved: {', '.join(unresolved)}")
        summary.note = "; ".join(lines)

    articles = {f.venue.venue_id: f.venue.wiki_article for f in outcome.fetched}
    _load(
        con,
        summary.run_id,
        clean,
        bad,
        {venue_id: (last, articles[venue_id]) for venue_id, last in new_watermarks.items()},
        summary,
        started_at,
        settle=settle,
    )
    log.info(
        "run %s %s: %d loaded, %d quarantined",
        summary.run_id,
        summary.status,
        summary.rows_loaded,
        summary.rows_quarantined,
    )


def _store_rejects(con, run_id: str, bad: list[quality.BadRow]) -> None:
    """Keep one rejection per key (see `_reject_key`); the caller owns the transaction.

    Seeing a rejection again reopens it if it had been superseded - the day
    loaded cleanly once and has gone bad again. An accepted one stays accepted:
    the API keeps answering the same way, and reopening it would undo the
    decision on the very next run. The operator's note survives either way.
    """
    already = set(con.execute(f"SELECT {_KEY_COLUMNS} FROM quarantine").fetchall())
    now = utc_now()
    for row in bad:
        key = _reject_key(row.venue_id, row.view_date, row.rule, row.window_start)
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


def _supersede(con, run_id: str, now: datetime) -> None:
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


def _load(con, run_id, clean, bad, new_watermarks, summary, started_at, settle=None) -> None:
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
        _store_rejects(con, run_id, bad)
        _supersede(con, run_id, now)
        if new_watermarks:
            con.executemany(
                "INSERT OR REPLACE INTO watermark (venue_id, last_date, updated_at, article) "
                "VALUES (?, ?, ?, ?)",
                [(vid, last, now, article) for vid, (last, article) in new_watermarks.items()],
            )
        if settle is not None:
            settle(con)
        _write_run_log(con, summary, started_at)
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


def _write_run_log(con, summary: RunSummary, started_at: datetime) -> None:
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
