"""Entry point: python -m nz_attraction_pageviews [ingest | resolve | check-venues]

ingest        read venues.csv, ingest into warehouse.duckdb, print the run summary.
              Safe to run repeatedly: the second run only asks for days it has not seen.
resolve       annotate or accept quarantined rejections for one venue.
check-venues  ask MediaWiki whether each title in venues.csv is canonical.
"""

from __future__ import annotations

import argparse
import functools
import logging
import sys
from datetime import date
from pathlib import Path

import duckdb

from . import client, ingest

VENUES_CSV = "venues.csv"
DB_PATH = "warehouse.duckdb"

# Distinct exit codes, so a scheduler can tell "try again later" from "fix the
# config" without parsing text. `degraded` is 0: see the end of _ingest.
EXIT_OK = 0
EXIT_NOTHING_MATCHED = 1  # resolve changed nothing; check-venues found a bad title
EXIT_USAGE = 2  # argparse's own
EXIT_CONFIG = 3  # venues.csv, parameters, contact, missing warehouse
EXIT_UPSTREAM = 4  # every venue failed: network, HTTP errors, request budget
EXIT_DRIFT = 5  # the API's response contract changed
EXIT_DATABASE = 6  # the warehouse could not be opened or written (locked, say)

log = logging.getLogger("nz_attraction_pageviews")


def _bounded_int(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an integer: {text!r}") from None
    if not 1 <= value <= ingest.MAX_DAYS:
        raise argparse.ArgumentTypeError(f"must be in 1..{ingest.MAX_DAYS}, got {value}")
    return value


def _positive_int(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an integer: {text!r}") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def _fraction(text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {text!r}") from None
    if not 0.0 <= value <= 1.0:  # nan fails this too
        raise argparse.ArgumentTypeError(f"must be a fraction in [0, 1], got {text}")
    return value


def _days(text: str) -> ingest.Days:
    """YYYY-MM-DD, YYYY-MM-DD..YYYY-MM-DD, 'null' or 'all'."""
    lowered = text.lower()
    if lowered == "null":
        return None
    if lowered == ingest.ALL_DAYS:
        return ingest.ALL_DAYS
    try:
        if ".." in text:
            first, last = (date.fromisoformat(part) for part in text.split("..", 1))
            if first > last:
                raise argparse.ArgumentTypeError(f"range runs backwards: {text!r}")
            return (first, last)
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"not a day: {text!r} (use YYYY-MM-DD, YYYY-MM-DD..YYYY-MM-DD, 'null' or 'all')"
        ) from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "command", nargs="?", choices=["ingest", "resolve", "check-venues"], default="ingest"
    )
    parser.add_argument("venue_id", nargs="?", help="resolve: the venue")
    parser.add_argument(
        "days",
        nargs="?",
        help="resolve: YYYY-MM-DD, YYYY-MM-DD..YYYY-MM-DD, 'null' (rejects with no "
        "parseable date) or 'all'",
    )
    parser.add_argument(
        "--note",
        "--resolution",
        dest="note",
        help="resolve: text to keep with the rejections (annotating requires one)",
    )
    parser.add_argument(
        "--accept",
        action="store_true",
        help="resolve: record the days as never arriving, so the watermark may step over them",
    )
    parser.add_argument("--venues", default=VENUES_CSV)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument(
        "--contact",
        help=f"email or URL for the Wikimedia User-Agent (default: ${client.CONTACT_ENV})",
    )
    parser.add_argument("--backfill-days", type=_bounded_int, default=ingest.DEFAULT_BACKFILL_DAYS)
    parser.add_argument("--chunk-days", type=_bounded_int, default=ingest.DEFAULT_CHUNK_DAYS)
    parser.add_argument(
        "--max-lookback-days", type=_bounded_int, default=ingest.DEFAULT_MAX_LOOKBACK_DAYS
    )
    parser.add_argument("--max-reject-rate", type=_fraction, default=ingest.DEFAULT_MAX_REJECT_RATE)
    parser.add_argument(
        "--max-http-requests", type=_positive_int, default=ingest.DEFAULT_MAX_HTTP_REQUESTS
    )
    parser.add_argument("--today", type=date.fromisoformat)
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="log each venue and window, not just retries"
    )
    return parser


def _fail(code: int, message: object) -> int:
    print(f"error: {message}", file=sys.stderr)
    return code


class _StderrHandler(logging.StreamHandler):
    """Writes to whatever sys.stderr is at the time, not when it was created."""

    def __init__(self):
        super().__init__()
        self.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, value):
        pass


def _configure_logging(verbose: bool) -> None:
    # On the package's logger, not the root: basicConfig does nothing when
    # something (a test runner, a host application) has configured the root.
    if not any(isinstance(h, _StderrHandler) for h in log.handlers):
        log.addHandler(_StderrHandler())
    log.setLevel(logging.INFO if verbose else logging.WARNING)


def main(argv=None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)

    if args.command == "resolve":
        if not args.venue_id or args.days is None:
            parser.error("resolve requires a venue_id and a day, range, 'null' or 'all'")
        try:
            args.days = _days(args.days)
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))
        return _resolve(args)
    if args.venue_id or args.days is not None:
        parser.error("venue_id and days are only accepted with resolve")
    if args.command == "check-venues":
        return _check_venues(args)
    return _ingest(args)


def _resolve(args) -> int:
    if not args.accept and not (args.note and args.note.strip()):
        args.note = "Reviewed by operator"
    if not Path(args.db).exists():
        # connect() would create an empty warehouse, and report that nothing
        # matched - true, and no help with a mistyped path.
        return _fail(EXIT_CONFIG, f"no warehouse at {args.db}")
    try:
        con = ingest.connect(args.db)
        try:
            changed = ingest.resolve(
                con, args.venue_id, args.days, note=args.note, accept=args.accept
            )
        finally:
            con.close()
    except duckdb.Error as exc:
        return _fail(EXIT_DATABASE, exc)

    if not changed:
        state = "open " if args.accept else ""
        print(
            f"no {state}rejection matched {args.venue_id} {_describe(args.days)}; nothing changed",
            file=sys.stderr,
        )
        return EXIT_NOTHING_MATCHED
    if args.accept:
        print(
            f"{changed} rejection(s) accepted; the watermark may now step over them. "
            f"No pageview data was loaded."
        )
    else:
        print(f"{changed} rejection(s) annotated; no data or watermarks changed")
    return EXIT_OK


def _describe(days: ingest.Days) -> str:
    if days is None:
        return "with no date"
    if isinstance(days, tuple):
        return f"{days[0]}..{days[1]}"
    return str(days)


def _opener(args):
    return functools.partial(client.http_get, user_agent=client.build_user_agent(args.contact))


def _check_venues(args) -> int:
    try:
        venues = ingest.read_venues(args.venues)
        opener = _opener(args)
    except (OSError, ValueError, client.ConfigError) as exc:
        return _fail(EXIT_CONFIG, exc)
    try:
        checks = client.check_titles([v.wiki_article for v in venues], opener=opener)
    except client.ApiError as exc:
        return _fail(EXIT_UPSTREAM, exc)

    problems = 0
    for venue in venues:
        check = checks[venue.wiki_article]
        if check.canonical is None:
            verdict = "MISSING: no such article"
        elif check.redirected:
            verdict = f"REDIRECT: use {check.canonical!r}"
        elif not check.ok:
            verdict = f"RENAMED: MediaWiki calls it {check.canonical!r}"
        else:
            verdict = "ok"
        problems += verdict != "ok"
        print(f"{venue.venue_id:<20} {venue.wiki_article:<40} {verdict}")
    if problems:
        print(
            "\nChange the title in venues.csv and run ingest: a venue whose title changes "
            "re-fetches its backfill window under the new title.",
            file=sys.stderr,
        )
    return EXIT_NOTHING_MATCHED if problems else EXIT_OK


def _ingest(args) -> int:
    # Everything that can be wrong with the configuration is checked before the
    # warehouse is opened, so a bad flag does not leave a new empty file behind.
    try:
        venues = ingest.read_venues(args.venues)
        ingest.validate_params(
            args.chunk_days,
            args.backfill_days,
            args.max_reject_rate,
            args.max_lookback_days,
            ingest.TRUST_LAG_DAYS,
            args.max_http_requests,
        )
        opener = _opener(args)
    except (OSError, ValueError, client.ConfigError) as exc:
        return _fail(EXIT_CONFIG, exc)

    try:
        summary = ingest.run_at(
            args.db,
            venues,
            backfill_days=args.backfill_days,
            chunk_days=args.chunk_days,
            max_reject_rate=args.max_reject_rate,
            max_lookback_days=args.max_lookback_days,
            max_http_requests=args.max_http_requests,
            today=args.today,
            opener=opener,
        )
    except client.SchemaDriftError as exc:
        return _fail(EXIT_DRIFT, f"the API's response no longer matches the contract: {exc}")
    except client.ApiError as exc:
        return _fail(EXIT_UPSTREAM, exc)
    except duckdb.Error as exc:
        return _fail(EXIT_DATABASE, f"{args.db}: {exc}")

    http = "" if summary.http_requests is None else f" ({summary.http_requests} HTTP calls)"
    print(
        f"run {summary.run_id} {summary.status}: "
        f"{summary.requests} requests{http}, "
        f"{summary.rows_fetched} fetched, "
        f"{summary.rows_loaded} loaded, "
        f"{summary.rows_quarantined} quarantined "
        f"({summary.reject_rate:.2%})"
    )
    if summary.note:
        print(summary.note)
    # `degraded` exits 0. The run did its job: every venue that could load did,
    # and the ones that could not are named above and in run_log.note. A
    # non-zero exit would make a scheduler retry a run that is already
    # complete, and an alert that fires every night gets muted. Read
    # run_log.status to find them, and `resolve` to release one.
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
