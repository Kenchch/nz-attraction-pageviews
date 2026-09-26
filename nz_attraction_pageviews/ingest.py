"""Incremental ingest of NZ attraction pageviews into DuckDB.

Shape of a run:

    read venues.csv
      -> plan: per venue, the start date from its watermark        (reads the warehouse)
      -> fetch: split each range into windows, fetch, apply the    (no warehouse connection)
         acceptance criteria; a venue whose request fails is set
         aside with its watermark untouched
      -> hold any venue whose NEW rejections are over the ceiling
      -> load: rows, quarantine, watermarks, status, run log       (one transaction)

The warehouse side lives in `store`, and the rules for where a venue starts
and how far its watermark may move in `watermark`; this module runs them in
order. The load is a single transaction. Either the whole run lands or none of it does,
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
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from . import client, quality, store
from . import watermark as watermark_rules
from .store import (  # re-exported: the package's public surface lives here
    ACCEPTED,
    ALL_DAYS,
    SUPERSEDED,
    Days,
    accepted_days,
    connect,
    get_watermark,
    resolve,
    utc_now,
)
from .watermark import plan_windows

log = logging.getLogger(__name__)

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


def start_date_for(con, venue: Venue, end: date, backfill_days: int) -> date:
    """Resume the day after the watermark, or backfill; see `watermark.start_date`."""
    row = store.watermark_row(con, venue.venue_id)
    watermark, article = row or (None, None)
    return watermark_rules.start_date(
        watermark,
        article,
        store.has_any_rows(con, venue.venue_id),
        venue.wiki_article,
        end,
        backfill_days,
    )


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
        frontier = watermark_rules.venue_frontier(
            f.start,
            end,
            f.clean,
            f.bad,
            trusted_end,
            accepted=store.accepted_days(con, venue.venue_id),
            accepted_null=store.accepted_null_windows(con, venue.venue_id),
        )
        current, stored_article = store.watermark_row(con, venue.venue_id) or (None, None)
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

        has_history = store.has_any_rows(con, venue.venue_id) and not retitled
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
    already = store.known_reject_keys(con)
    novel = [
        r
        for r in bad
        if store.reject_key(r.venue_id, r.view_date, r.rule, r.window_start) not in already
    ]
    if len(novel) != len(bad):
        notes.append(
            f"{len(bad) - len(novel)} of {len(bad)} rejected rows were already quarantined"
        )

    held = set()
    for f in fetched:
        venue_id = f.venue.venue_id
        days = store.accepted_days(con, venue_id)
        null_windows = store.accepted_null_windows(con, venue_id)
        seen = len(f.clean) + len(f.bad)
        fresh = sum(
            1
            for r in novel
            if r.venue_id == venue_id and not watermark_rules.is_accepted(r, days, null_windows)
        )
        if seen and quality.reject_rate(seen, fresh) > max_reject_rate:
            held.add(venue_id)
            notes.append(f"{venue_id}: {fresh}/{seen} newly rejected, above the ceiling, held")
    return held, notes


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
    return _run(lambda: store.opened(db_path), venues, **options)


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
    today = today or utc_now().date()
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
                store.write_run_log(con, summary, started_at)
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
    observed = store.publication_frontier(con, clean, end)
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
            blocked=store.blocked_venues(con, [v.venue_id for v in venues]),
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
    store.load(
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


__all__ = [
    "ACCEPTED",
    "ALL_DAYS",
    "SUPERSEDED",
    "Days",
    "RunSummary",
    "Venue",
    "accepted_days",
    "connect",
    "decide_status",
    "get_watermark",
    "plan_windows",
    "read_venues",
    "resolve",
    "run",
    "run_at",
    "start_date_for",
    "utc_now",
    "validate_params",
]
