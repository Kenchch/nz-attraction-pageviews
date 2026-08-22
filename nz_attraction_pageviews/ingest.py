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

import collections
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
    loaded_at   TIMESTAMP NOT NULL,  -- UTC
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
    seen_at    TIMESTAMP NOT NULL  -- UTC
);

CREATE TABLE IF NOT EXISTS watermark (
    venue_id   VARCHAR PRIMARY KEY,
    last_date  DATE NOT NULL,
    updated_at TIMESTAMP NOT NULL  -- UTC
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id            VARCHAR PRIMARY KEY,
    started_at        TIMESTAMP NOT NULL,  -- UTC
    finished_at       TIMESTAMP,  -- UTC
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
class Fetched:
    venue: Venue
    start: date
    clean: list[quality.CleanRow]
    bad: list[quality.BadRow]


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


def utc_now() -> datetime:
    """Now, in UTC, with no offset attached - which is what the columns hold.

    DuckDB's TIMESTAMP is timezone-NAIVE. Handing it an aware datetime does not
    store the offset and does not store UTC: it converts the instant to the
    SESSION's local wall time and keeps that. So `datetime.now(UTC)` - which
    every write site used, deliberately - was the one value in the schema
    guaranteed to be stored in local time, beside a view_date that genuinely is
    a UTC day. On an NZST host a row loaded at 09:53 UTC read back as 21:53,
    twelve hours ahead of every date in its own table; the same file read in
    another zone reported different absolute times for the same run; and in a
    DST zone the local clock steps back once a year, so `ORDER BY started_at
    DESC` - the query the README recommends - could order two runs an hour
    apart backwards.

    Dropping the offset AFTER converting to UTC stores the UTC wall time, which
    is what the column is documented to hold and what the rest of the schema
    already assumes. TIMESTAMPTZ would carry the offset properly, but DuckDB
    needs pytz to read one back, and a timezone bug is not worth a new runtime
    dependency.
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
    backfill = end - timedelta(days=backfill_days - 1)
    if watermark is None:
        return backfill
    if not _has_any_rows(con, venue.venue_id):
        # A watermark that has only ever walked on the trust line, for a venue
        # that has never produced a row, is not a record of coverage - it is
        # the OTHER venues' evidence wearing this venue's name. The frontier is
        # a property of the API, so a healthy sibling pushes this venue's
        # watermark forward night after night while its own article 404s.
        #
        # A typo in venues.csv therefore consumed the whole backfill window on
        # the first run, and read_venues keeps venue_id stable when the title
        # is corrected, so fixing the typo recovered only the days since. Keep
        # asking for the whole window until the venue produces something; the
        # moment it does, this stops.
        return min(watermark + timedelta(days=1), backfill)
    return watermark + timedelta(days=1)


def _venue_watermark(
    start: date,
    end: date,
    venue_clean: list[quality.CleanRow],
    venue_bad: list[quality.BadRow],
    trusted_end: date | None,
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

    `trusted_end` is None when nothing anywhere proves the upstream has
    published anything - see run(). Age is then no evidence at all, so an
    absent day cannot be called quiet and the watermark advances only as far as
    a day that actually arrived.
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

    loaded = [row.view_date for row in venue_clean]
    if trusted_end is None:
        # No evidence the upstream has published anything. Advance only to a
        # day that actually arrived, and not at all if none did.
        if not loaded:
            return None
        frontier = max(loaded)
    else:
        trusted = min(end, trusted_end)
        frontier = max(max(loaded), trusted) if loaded else trusted

    frontier = min(frontier, ceiling)
    return frontier if frontier >= start else None


def _validate_params(
    chunk_days: int,
    backfill_days: int,
    max_reject_rate: float,
    max_lookback_days: int,
    trust_lag_days: int,
) -> None:
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
    # Range first, isfinite second. math.isfinite converts its argument to a
    # float, so 10**1000 raised OverflowError out of the validator that exists
    # to give a clean rejection. The range test handles an int of any size, and
    # nan fails it too, since every comparison against nan is False.
    if not 0.0 <= max_reject_rate <= 1.0 or (
        isinstance(max_reject_rate, float) and not math.isfinite(max_reject_rate)
    ):
        raise ValueError(
            f"max_reject_rate must be a finite fraction in [0, 1], got {max_reject_rate!r}. "
            f"nan in particular disables the gate silently: every comparison against it "
            f"is False, so no run is ever rejected."
        )


def _fetch_all(
    con,
    venues: list[Venue],
    *,
    end: date,
    floor: date,
    chunk_days: int,
    backfill_days: int,
    today: date,
    fetch,
    summary: RunSummary,
    clean: list[quality.CleanRow],
    bad: list[quality.BadRow],
    stalled: list[str],
) -> tuple[list[Fetched], list[str]]:
    fetched: list[Fetched] = []
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
            # Counted BEFORE the call. The failure path writes this summary
            # to the run log, and incrementing afterwards meant the request
            # that raised was never counted - so the one run whose request
            # count matters read one short, and a venue that failed on its
            # first window logged `requests 0` while having gone to the
            # network. The field answers "how many windows did we ask for",
            # which is decided when we ask, not when we get an answer.
            summary.requests += 1
            items = fetch(venue.wiki_article, window_start, window_end)
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
        fetched.append(Fetched(venue, start, venue_clean, venue_bad))

    return fetched, stalled


def _advance_watermarks(
    con,
    fetched: list[Fetched],
    *,
    end: date,
    trusted_end: date | None,
    trust_lag_days: int,
    silent: list[str],
) -> tuple[dict[str, date], list[str]]:
    new_watermarks: dict[str, date] = {}
    for f in fetched:
        venue, start, venue_clean, venue_bad = f.venue, f.start, f.clean, f.bad
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
        #
        # The two conditions used to meet in the middle and leave a gap.
        # The first required venue_bad to be EMPTY, so a venue that was
        # returning rows and having all of them rejected was excluded from
        # "no rows ever"; the second reads `effective`, which is None for a
        # venue that has never set a watermark, so it was excluded there
        # too. A venue whose article title resolves to a DIFFERENT article
        # - the realistic hazard, since the API is title-exact and
        # redirects are silent - lands exactly there: every row fails
        # article_matches_request and the note was empty, so the only
        # signal was the gate - which does not report a venue, it stops the
        # pipeline. (An earlier version of this comment claimed the healthy
        # venues "dilute the reject rate below the gate". They do not:
        # one venue of eight is 12.5% against a 5% ceiling.) The test is
        # now "has this venue ever loaded a row", which is the question the
        # alert was always asking.
        effective = new_watermarks.get(venue.venue_id, current)
        if not _has_any_rows(con, venue.venue_id) and not venue_clean:
            detail = f", {len(venue_bad)} row(s) rejected" if venue_bad else ""
            silent.append(f"{venue.venue_id}: no rows ever{detail}, check {venue.wiki_article!r}")
        else:
            # Measured from the last day this venue actually PRODUCED, not
            # from its watermark. The watermark walks the trust line on
            # other venues' evidence whether or not this article is still
            # being served, so it sits at exactly
            # `trust_lag_days - PUBLICATION_LAG_DAYS` behind `end` and can
            # never satisfy `> trust_lag_days`. An article renamed or
            # deleted upstream was therefore skipped day after day by a
            # watermark that was, in this scenario, the thing telling the
            # lie - and the alert built to catch it could not fire.
            last = con.execute(
                "SELECT max(view_date) FROM pageviews WHERE venue_id = ?",
                [venue.venue_id],
            ).fetchone()[0]
            if venue_clean:
                newest = max(r.view_date for r in venue_clean)
                last = newest if last is None else max(last, newest)
            if last is not None and (end - last).days > trust_lag_days:
                silent.append(
                    f"{venue.venue_id}: no rows for {(end - last).days} days "
                    f"(watermark {effective}), check {venue.wiki_article!r}"
                )

    return new_watermarks, silent


def _apply_gate(
    con,
    fetched: list[Fetched],
    bad: list[quality.BadRow],
    max_reject_rate: float,
    silent: list[str],
) -> set[str]:
    # The gate fires on what is NEW.
    #
    # A day already sitting in `quarantine` for the same rule cannot be
    # lost a second time - _venue_watermark is already holding the
    # watermark short of it, which is the whole mechanism by which nothing
    # is lost. Counting it again every night turns one broken venue into a
    # permanent stoppage of the entire pipeline, and the rate it pins is
    # scale-invariant: one venue of eight rejecting everything gives
    # 1/8 = 12.5% however long the range or however many venues there are.
    # Growth does not dilute it; adding venues does not dilute it.
    #
    # Measured, with one venue whose title had drifted so the API answered
    # 200 with a different article:
    #
    #   night 0: GATE TRIPPED rate=0.125
    #   night 1: GATE TRIPPED rate=0.125
    #   ... every night, indefinitely
    #   rows stored for the SEVEN HEALTHY venues : 0
    #
    # Nothing loads, so no watermark moves, so every venue re-fetches the
    # same window forever - and once the oldest held day falls past
    # max_lookback_days, all eight venues give up on it together. A single
    # silent redirect upstream costs the whole warehouse.
    #
    # The gate exists to stop a bad EXTRACT reaching the warehouse. A day
    # that has already been quarantined is not part of this extract's
    # verdict; it is a standing problem, and `run_log.note` is what reports
    # a standing problem.
    already = {
        (r[0], r[1], r[2])
        for r in con.execute("SELECT venue_id, view_date, rule FROM quarantine").fetchall()
    }
    novel = [r for r in bad if (r.venue_id, r.view_date, r.rule) not in already]

    if len(novel) != len(bad):
        # Otherwise the log shows a rate above the ceiling on a run that
        # passed, and nothing says why.
        silent.append(
            f"{len(bad) - len(novel)} of {len(bad)} rejected rows were "
            f"already quarantined; gate saw {len(novel)}"
        )

    # ... and it fires per VENUE before it fires globally.
    #
    # Novel-only is necessary but not sufficient on its own: `quarantine`
    # is written by _load, which runs AFTER this, so a venue that breaks
    # all at once persists nothing, `already` stays empty, and every night
    # looks like the first. A venue whose NOVEL rejection rate is over the
    # ceiling is broken on its own terms and does not get a vote in "is
    # tonight's extract broadly bad". Its rows are still quarantined, its
    # watermark is still held short of them by _venue_watermark, and it is
    # named in the note. What changes is that it can no longer stop the
    # other seven venues from loading.
    #
    # Novel, not raw, for the per-venue test too. A venue stuck re-fetching
    # one known-bad day is 100% rejected every night on a window of one -
    # so a raw test would hold it forever, and if every venue is stuck on
    # its own bad day it would hold all of them and deadlock exactly as
    # before. Nothing is NEWLY wrong with such a venue; the standing
    # problem is what run_log.note reports.
    novel_by_venue = collections.Counter(r.venue_id for r in novel)
    held = set()
    for f in fetched:
        venue, venue_clean, venue_bad = f.venue, f.clean, f.bad
        seen = len(venue_clean) + len(venue_bad)
        fresh = novel_by_venue.get(venue.venue_id, 0)
        if seen and quality.reject_rate(seen, fresh) > max_reject_rate:
            held.add(venue.venue_id)
            silent.append(
                f"{venue.venue_id}: {fresh}/{seen} newly rejected, above the ceiling, held"
            )

    # Holding a venue has to mean holding it. Excluding it from the gate
    # while still loading its rows was the worst of both: the whole-run
    # gate could no longer refuse the extract, and `INSERT OR REPLACE`
    # wrote the venue's surviving rows straight over days the warehouse
    # already held from a good run. Measured with a venue 10/30 rejected:
    # 20 clean rows went into the warehouse under a note saying "held".
    #
    # Its clean rows are dropped and its watermark stays put, so every day
    # it covered is asked for again next run.
    #
    # `bad` is deliberately untouched: the venue's rejected rows still
    # reach `quarantine`, so next run they are in `already`, `novel` for
    # that venue is zero, it is no longer held, and it loads normally. That
    # self-healing path is the whole reason the per-venue test is on novel
    # rejections rather than raw ones.
    gate_fetched = sum(
        len(venue_clean) + len(venue_bad)
        for venue, venue_clean, venue_bad in ((f.venue, f.clean, f.bad) for f in fetched)
        if venue.venue_id not in held
    )
    gate_bad = [r for r in novel if r.venue_id not in held]

    # The whole-run gate the per-venue test replaced cannot fire on its
    # own. Every venue still in it is by construction at or under the
    # ceiling, and a weighted mean of values under a ceiling is under that
    # ceiling, so `enforce_gate` below is arithmetically unreachable. It is
    # kept because it is the correct expression of "the surviving rows are
    # acceptable" and costs nothing, but it is not what refuses a bad
    # extract.
    #
    # "Is tonight's extract broadly bad" therefore has to be asked about
    # VENUES rather than rows: one venue of eight is one bad venue, but
    # most of them at once is one bad extract. `max_reject_rate` is a
    # per-row ceiling and means nothing at this level, so this is a
    # majority. Venues that answered with nothing at all are not counted -
    # a quiet night is not a failed one.
    answering = [f.venue.venue_id for f in fetched if f.clean or f.bad]
    if answering and len(held) * 2 > len(answering):
        raise quality.QualityGateFailed(
            f"{len(held)} of {len(answering)} venues that answered rejected "
            f"above the {max_reject_rate:.0%} ceiling - this is a bad "
            f"extract, not {len(held)} bad venues."
        )
    quality.enforce_gate(gate_fetched, len(gate_bad), max_reject_rate)

    return held


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
    _validate_params(chunk_days, backfill_days, max_reject_rate, max_lookback_days, trust_lag_days)
    today = today or datetime.now(UTC).date()
    end = today - timedelta(days=PUBLICATION_LAG_DAYS)
    floor = end - timedelta(days=max_lookback_days - 1)
    calendar_trust_line = today - timedelta(days=trust_lag_days)

    run_id = uuid.uuid4().hex[:12]
    started_at = utc_now()
    summary = RunSummary(run_id=run_id, status="running", venues=len(venues))

    clean: list[quality.CleanRow] = []
    bad: list[quality.BadRow] = []
    new_watermarks: dict[str, date] = {}
    stalled: list[str] = []
    silent: list[str] = []
    fetched: list[tuple[Venue, date, list[quality.CleanRow], list[quality.BadRow]]] = []

    try:
        fetched, stalled = _fetch_all(
            con,
            venues,
            end=end,
            floor=floor,
            chunk_days=chunk_days,
            backfill_days=backfill_days,
            today=today,
            fetch=fetch,
            summary=summary,
            clean=clean,
            bad=bad,
            stalled=stalled,
        )

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
        # With no evidence anywhere - an empty warehouse and a run that
        # fetched nothing - the calendar line used to stand, on the reasoning
        # that a set of genuinely quiet venues should still make progress. That
        # is the same bet the frontier bound above exists to refuse, and it is
        # open exactly when the warehouse is empty, which an outage keeps true
        # indefinitely: every night the run reported `ok` and stepped every
        # watermark forward one day on a calendar date alone.
        #
        # Measured on a fresh deploy whose first night hit an upstream
        # incident: of the 89 days the API had and the run asked for, 5 were
        # stored and 84 were skipped permanently, both runs `ok`. "Quiet" and
        # "the upstream is down" look identical in a response, so with no
        # evidence the safe reading is the one that costs a re-request rather
        # than the data. None means "hold"; see _venue_watermark.
        observed = _publication_frontier(con, clean, end)
        trusted_end = None if observed is None else min(calendar_trust_line, observed)

        new_watermarks, silent = _advance_watermarks(
            con,
            fetched,
            end=end,
            trusted_end=trusted_end,
            trust_lag_days=trust_lag_days,
            silent=silent,
        )

        summary.rows_quarantined = len(bad)
        # Record the rate before the gate can raise, so a failed run logs the
        # number that explains why it failed rather than 0.0.
        summary.reject_rate = quality.reject_rate(summary.rows_fetched, summary.rows_quarantined)

        held = _apply_gate(con, fetched, bad, max_reject_rate, silent)

        clean = [r for r in clean if r.venue_id not in held]
        for venue_id in held:
            new_watermarks.pop(venue_id, None)

        summary.note = "; ".join([*stalled, *silent])

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
    except BaseException:
        # BaseException, not Exception: Ctrl-C and SystemExit are precisely when
        # a half-written transaction is likeliest, and `except Exception` let
        # both past without a ROLLBACK, leaving the connection open mid-write.
        #
        # This is the innermost cleanup and it re-raises, so cancellation still
        # propagates unchanged. The run-level handler above stays on Exception
        # on purpose - a user pressing Ctrl-C should not be written into
        # run_log as an ordinary failure.
        #
        # Guarded: a ROLLBACK that itself fails (the BEGIN never took, say) must
        # not replace the error that explains what actually went wrong.
        try:
            con.execute("ROLLBACK")
        except BaseException:
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
            utc_now(),
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
