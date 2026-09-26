"""Talks to the Wikimedia pageviews API.

Three behaviours here are the reason this module exists as its own file:

1. The API returns 404 when an article simply had no traffic in the window.
   That is data, not a failure. Treating it as an error would abort a run over
   a quiet Tuesday at a small museum. But neither a 404 nor a 200 that leaves
   days out is proof of no traffic: whether a day comes back depends on the
   shape of the request, so an absence is verified before it is believed. See
   `fetch_window`.
2. It is rate limited. 429, 500, 502, 503 and 504 are retried with exponential
   backoff and jitter, and so are a dropped connection and a 200 whose body is
   not JSON at all (an HTML error page from a proxy, say). Every other status -
   400, 403, 501, 505 and the rest - is not retried, because asking again will
   not change the answer. Neither is a TLS certificate failure.
3. The response shape is a contract. JSON that parses but no longer matches
   EXPECTED_FIELDS is schema drift and is raised loudly, rather than silently
   becoming a NULL three tables later.

The network call is injected (`opener`) so the tests run offline.
"""

from __future__ import annotations

import http.client
import json
import logging
import math
import os
import random
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, timedelta

from . import __version__

log = logging.getLogger(__name__)

API_ROOT = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
PROJECT = "en.wikipedia"
ACCESS = "all-access"
AGENT = "user"
PROJECT_URL = "https://github.com/Kenchch/nz-attraction-pageviews"

# Wikimedia asks every client for a User-Agent it can use to reach the operator,
# and throttles anonymous ones. The operator is whoever runs this, not whoever
# wrote it, so the contact is required configuration rather than a constant.
CONTACT_ENV = "NZAP_CONTACT"

# The fields we agreed to consume. Extra fields are fine, missing ones are not.
EXPECTED_FIELDS = frozenset(
    {"project", "article", "granularity", "timestamp", "access", "agent", "views"}
)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
TRANSPORT_STATUS = 599  # our own marker for "the socket died", not an HTTP code

# How http_get tells _fetch_once why a transport failure happened, and whether
# retrying can help. Carried as pseudo-headers so the opener keeps its
# (status, headers, body) shape.
TRANSPORT_ERROR_HEADER = "x-transport-error"
TRANSPORT_PERMANENT_HEADER = "x-transport-permanent"

# Longest we will wait between attempts, however long the server asks for.
# Retry-After is a number from someone else's infrastructure, and `float` parses
# `inf` as happily as `30`. With max_attempts at 4 the whole retry sequence is
# bounded at a few minutes, after which the window fails and the watermark does
# not move, so the days are asked for again tomorrow rather than lost.
MAX_BACKOFF_SECONDS = 120.0

# Ceiling on how much of a response body we will read into memory. The largest
# legitimate body is one article's daily rows for one window, a few tens of KB;
# 2 MB is roughly forty years of them. Refused, not truncated: a truncated body
# would come back as drift, or as a valid prefix.
MAX_RESPONSE_BYTES = 2 * 1024 * 1024

# The socket timeout bounds each read, not the request: a server that sends one
# byte every few seconds never trips it. This bounds the whole body.
READ_CHUNK_BYTES = 64 * 1024
READ_DEADLINE_SECONDS = 60.0
SOCKET_TIMEOUT_SECONDS = 30

# How far back to extend the start date when re-asking a window that answered
# empty. No single pad is reliable (a 7 day and a 45 day widening each failed on
# a case the others caught), so two are tried. This is only the cheap first
# attempt - `_subdivide` is what makes the verification trustworthy.
VERIFY_PADS = (15, 30)

# Width of the narrow requests that re-ask the days a non-empty 200 left out.
# Seven is the widest span the live API has been seen to answer when a wider
# window covering the same days came back without them.
HOLE_SLICE_DAYS = 7


class ApiError(RuntimeError):
    """The API answered, or failed to, in a way that means this venue should stop."""


class TransportError(ApiError):
    """No usable HTTP answer at all: DNS, refused connection, TLS, a dropped socket."""


class MalformedResponse(ApiError):
    """A 200 whose body is not JSON: an HTML error page, a truncated or garbled body."""


class RequestBudgetExceeded(ApiError):
    """The run has made as many HTTP requests as it is allowed to."""


class SchemaDriftError(RuntimeError):
    """The payload is JSON, but no longer matches the contract in EXPECTED_FIELDS."""


class ConfigError(RuntimeError):
    """The client cannot run as configured - for now, no contact for the User-Agent."""


def build_user_agent(contact: str | None = None) -> str:
    """The User-Agent for live requests, naming whoever operates this copy.

    `contact` is an email address or URL; it falls back to $NZAP_CONTACT. With
    neither, live requests are refused rather than sent in someone else's name.
    """
    contact = (contact or os.environ.get(CONTACT_ENV) or "").strip()
    if not contact:
        raise ConfigError(
            f"no contact for the Wikimedia User-Agent: pass --contact or set {CONTACT_ENV} "
            f"to an email address or URL where Wikimedia can reach you"
        )
    return f"nz-attraction-pageviews/{__version__} (+{PROJECT_URL}; {contact})"


def build_url(article: str, start: date, end: date) -> str:
    """Build a per-article daily URL.

    The article title is path-quoted with safe="" so that titles like
    "Sky_Tower_(Auckland)" survive the round trip.
    """
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    quoted = urllib.parse.quote(article, safe="")
    return f"{API_ROOT}/{PROJECT}/{ACCESS}/{AGENT}/{quoted}/daily/{start:%Y%m%d}/{end:%Y%m%d}"


def _read_bounded(stream, url: str, *, clock=time.monotonic) -> bytes:
    """Read at most MAX_RESPONSE_BYTES within READ_DEADLINE_SECONDS, or refuse.

    One byte more than the ceiling is asked for in total, so "exactly at the
    limit" and "over the limit" are distinguishable; Content-Length is not
    trusted. `read1` returns whatever has arrived rather than blocking for a
    full chunk, which is what lets the deadline be checked while a slow server
    trickles.
    """
    read = getattr(stream, "read1", None) or stream.read
    deadline = clock() + READ_DEADLINE_SECONDS
    chunks: list[bytes] = []
    received = 0
    while received <= MAX_RESPONSE_BYTES:
        chunk = read(min(READ_CHUNK_BYTES, MAX_RESPONSE_BYTES + 1 - received))
        if not chunk:
            break
        chunks.append(chunk)
        received += len(chunk)
        if clock() > deadline:
            raise TimeoutError(
                f"{url}: body still arriving after {READ_DEADLINE_SECONDS:.0f}s "
                f"({received:,} bytes so far)"
            )
    if received > MAX_RESPONSE_BYTES:
        raise ApiError(
            f"{url}: response exceeds {MAX_RESPONSE_BYTES:,} bytes. The largest "
            f"real body here is a few tens of KB, so this is a wrong endpoint "
            f"or a broken one, not data."
        )
    return b"".join(chunks)


def _transport_failure(exc: BaseException) -> tuple[int, dict[str, str], bytes]:
    reason = getattr(exc, "reason", exc)
    headers = {TRANSPORT_ERROR_HEADER: f"{type(reason).__name__}: {reason}"}
    if isinstance(reason, ssl.SSLCertVerificationError):
        # A certificate that fails verification now will fail it in two
        # minutes too. Retrying only delays the report.
        headers[TRANSPORT_PERMANENT_HEADER] = "1"
    return TRANSPORT_STATUS, headers, b""


def http_get(url: str, *, user_agent: str | None = None) -> tuple[int, dict[str, str], bytes]:
    """Real network call. Swapped out in tests."""
    headers = {"User-Agent": user_agent or build_user_agent()}
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=SOCKET_TIMEOUT_SECONDS) as response:
            return response.status, dict(response.headers), _read_bounded(response, url)
    except urllib.error.HTTPError as exc:
        # An HTTPError IS the response, holding the same socket; `with exc`
        # closes it now rather than whenever the garbage collector gets to it.
        # Its body is bounded for the same reason the success body is.
        with exc:
            try:
                body = _read_bounded(exc, url)
            except (OSError, http.client.HTTPException) as read_exc:
                return _transport_failure(read_exc)
            return exc.code, dict(exc.headers or {}), body
    except (
        urllib.error.URLError,
        OSError,  # includes TimeoutError, from the socket or from _read_bounded
        # Not an OSError: IncompleteRead, from a truncated chunked response,
        # inherits straight from Exception.
        http.client.HTTPException,
    ) as exc:
        return _transport_failure(exc)


class CountingOpener:
    """Wraps an opener to count HTTP requests and stop at a budget.

    `run_log.requests` counts windows; one window can cost dozens of calls when
    the API is misbehaving, which is exactly when the real number matters.
    """

    def __init__(self, opener=http_get, *, budget: int | None = None):
        self.opener = opener
        self.budget = budget
        self.count = 0

    def __call__(self, url: str):
        if self.budget is not None and self.count >= self.budget:
            raise RequestBudgetExceeded(
                f"request budget of {self.budget} HTTP calls for this run is spent"
            )
        self.count += 1
        return self.opener(url)


def _backoff_seconds(attempt: int, headers: dict[str, str]) -> float:
    """Honour Retry-After if the server sent one, otherwise 2^attempt + jitter.

    Jitter matters when several venues are fetched in a loop: without it every
    retry lands on the same second and re-creates the burst that got us
    throttled. Both paths are capped at MAX_BACKOFF_SECONDS, and a header that
    is not a finite number falls through to our own backoff.
    """
    # Header names are case-insensitive (RFC 9110 5.1), and dict(response.headers)
    # keeps the server's casing verbatim.
    folded = {k.lower(): v for k, v in headers.items()}
    retry_after = folded.get("retry-after")
    if retry_after:
        try:
            requested = float(retry_after)
        except ValueError:
            requested = None  # an HTTP-date, or nonsense. Use our own backoff.
        if requested is not None and math.isfinite(requested):
            return max(0.0, min(requested, MAX_BACKOFF_SECONDS))
    return min((2.0**attempt) + random.uniform(0, 0.5), MAX_BACKOFF_SECONDS)


def _parse(body: bytes, article: str) -> list[dict]:
    """Decode a 200 and check it against the contract.

    A body that is not JSON at all is a transport problem, not drift: a proxy's
    HTML error page, a captive portal, a garbled or truncated read. That raises
    MalformedResponse, which is retried and then fails only this venue. JSON
    that parses and breaks the contract raises SchemaDriftError, which stops the
    run.
    """
    try:
        payload = json.loads(body.decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        # ValueError covers UnicodeDecodeError, JSONDecodeError and an integer
        # longer than Python will convert; RecursionError a body nested too deep.
        raise MalformedResponse(
            f"{article}: body is not valid UTF-8 JSON ({type(exc).__name__}: {exc})"
        ) from exc

    if not isinstance(payload, dict):
        raise SchemaDriftError(
            f"{article}: top-level JSON is {type(payload).__name__}, expected an object"
        )

    items = payload.get("items")
    if items is None:
        raise SchemaDriftError(f"{article}: response has no 'items' key")
    if not isinstance(items, list):
        raise SchemaDriftError(f"{article}: 'items' is {type(items).__name__}, expected list")

    for item in items:
        if not isinstance(item, dict):
            raise SchemaDriftError(f"{article}: item is {type(item).__name__}, expected an object")
        missing = EXPECTED_FIELDS - set(item)
        if missing:
            raise SchemaDriftError(f"{article}: item missing fields {sorted(missing)}")

        # These four describe WHAT was counted, and no acceptance rule reads
        # them: a de.wikipedia / monthly / desktop / spider row would otherwise
        # load as one clean New Zealand day. A mismatch means the request and
        # the response disagree about the question, so every row is suspect.
        for field, expected in (
            ("project", PROJECT),
            ("granularity", "daily"),
            ("access", ACCESS),
            ("agent", AGENT),
        ):
            if item[field] != expected:
                raise SchemaDriftError(
                    f"{article}: {field} is {item[field]!r}, expected {expected!r}. "
                    f"The response is not answering the question the URL asked."
                )
        if not isinstance(item["timestamp"], str):
            raise SchemaDriftError(
                f"{article}: timestamp is {type(item['timestamp']).__name__}, expected str"
            )
    return items


def _fetch_once(
    article: str,
    start: date,
    end: date,
    *,
    opener,
    max_attempts: int,
    sleep,
) -> list[dict] | None:
    """One window, with retries. Returns parsed items, or None if the API said 404."""
    url = build_url(article, start, end)
    last = "no attempt"
    transport = False

    for attempt in range(1, max_attempts + 1):
        status, headers, body = opener(url)

        if status == 200:
            try:
                return _parse(body, article)
            except MalformedResponse as exc:
                last, transport = f"status 200 with {exc}", False
        elif status == 404:
            return None
        elif status == TRANSPORT_STATUS:
            detail = headers.get(TRANSPORT_ERROR_HEADER, "connection failed")
            if headers.get(TRANSPORT_PERMANENT_HEADER):
                raise TransportError(f"{article}: {detail}; not retried")
            last, transport = f"transport error ({detail})", True
        elif status in RETRYABLE_STATUS:
            last, transport = f"status {status}", False
        else:
            # 400, 403, 501 and the rest: retrying will not change the answer.
            raise ApiError(f"{article}: HTTP {status} from {url}")

        if attempt < max_attempts:
            delay = _backoff_seconds(attempt, headers)
            log.warning(
                "%s %s..%s: %s; retry %d of %d in %.1fs",
                article,
                start,
                end,
                last,
                attempt,
                max_attempts - 1,
                delay,
            )
            sleep(delay)

    error = TransportError if transport else ApiError
    raise error(f"{article}: gave up after {max_attempts} attempts, last {last}")


def _day(item: dict) -> date | None:
    """The day an item is for, or None if its timestamp is not a date at all."""
    stamp = str(item.get("timestamp", ""))
    if not re.match(r"\d{8}", stamp):
        return None
    try:
        return date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8]))
    except ValueError:
        return None


def _within(item: dict, start: date, end: date) -> bool:
    """Is this item's day inside start..end?

    An unparseable timestamp is kept rather than dropped, so that the acceptance
    criteria quarantine it visibly instead of it vanishing here in silence.
    """
    day = _day(item)
    return day is None or start <= day <= end


def _slices(start: date, end: date, width: int) -> list[tuple[date, date]]:
    out, cursor = [], start
    while cursor <= end:
        stop = min(cursor + timedelta(days=width - 1), end)
        out.append((cursor, stop))
        cursor = stop + timedelta(days=1)
    return out


def _holes(items: list[dict], start: date, end: date) -> list[tuple[date, date]]:
    """Runs of days in start..end that are absent but have a later day present.

    Those are the days the watermark would trust as quiet ("a later day arrived,
    so this one was published"), so they are the ones worth re-asking. Days
    after the last one present are left alone: the watermark does not trust
    them, and they are asked for again next run anyway.
    """
    present = {d for d in map(_day, items) if d is not None and start <= d <= end}
    if not present:
        return []
    runs: list[tuple[date, date]] = []
    run_start = None
    cursor = start
    last = max(present)
    while cursor <= last:
        if cursor not in present:
            run_start = run_start or cursor
        elif run_start is not None:
            runs.append((run_start, cursor - timedelta(days=1)))
            run_start = None
        cursor += timedelta(days=1)
    return runs


def _subdivide(
    article: str,
    start: date,
    end: date,
    *,
    opener,
    max_attempts: int,
    sleep,
) -> list[dict]:
    """Halve an empty window and ask about each piece. Recurses only into the pieces
    that also come back empty, so a spurious empty costs a handful of requests and
    only a window that is empty all the way down pays for the whole tree.

    This is what makes an empty answer trustworthy. Widening alone is not enough:
    a live 30 day window for `Hobbiton_Movie_Set` answered 404 at its own width
    and at both pads, while every 7 day slice of it answered 200. An article that
    genuinely does not exist answers 404 at every slice, down to single days,
    which is what stops this from inventing data.
    """
    span = (end - start).days + 1
    if span <= 1:
        return []

    mid = start + timedelta(days=span // 2 - 1)
    found: list[dict] = []
    for piece_start, piece_end in ((start, mid), (mid + timedelta(days=1), end)):
        items = _fetch_once(
            article, piece_start, piece_end, opener=opener, max_attempts=max_attempts, sleep=sleep
        )
        if not items:
            # `None` is a 404, `[]` is a 200 naming no days at all. Neither is
            # evidence, so both are taken apart further.
            found.extend(
                _subdivide(
                    article,
                    piece_start,
                    piece_end,
                    opener=opener,
                    max_attempts=max_attempts,
                    sleep=sleep,
                )
            )
        else:
            found.extend(items)
    return found


def _fill_holes(
    article: str,
    start: date,
    end: date,
    items: list[dict],
    *,
    opener,
    max_attempts: int,
    sleep,
) -> list[dict]:
    """Re-ask the days a non-empty 200 left out in the middle, in narrow slices.

    The watermark trusts a hole behind a day that arrived as genuinely quiet,
    and never asks for it again. That is only safe if the hole was verified
    here; a wide 200 that omits days a narrower request returns is the 404 bug
    wearing a success code.
    """
    holes = _holes(items, start, end)
    if not holes:
        return items

    covered = {str(item.get("timestamp")) for item in items}
    extra: list[dict] = []
    for hole_start, hole_end in holes:
        for piece_start, piece_end in _slices(hole_start, hole_end, HOLE_SLICE_DAYS):
            found = _fetch_once(
                article,
                piece_start,
                piece_end,
                opener=opener,
                max_attempts=max_attempts,
                sleep=sleep,
            )
            if not found:
                found = _subdivide(
                    article,
                    piece_start,
                    piece_end,
                    opener=opener,
                    max_attempts=max_attempts,
                    sleep=sleep,
                )
            for item in found:
                stamp = str(item.get("timestamp"))
                if _within(item, piece_start, piece_end) and stamp not in covered:
                    extra.append(item)
                    covered.add(stamp)

    missing = sum((e - s).days + 1 for s, e in holes)
    log.info(
        "%s %s..%s: re-asked %d missing day(s) in %d hole(s), recovered %d",
        article,
        start,
        end,
        missing,
        len(holes),
        len(extra),
    )
    if not extra:
        return items
    return sorted(items + extra, key=lambda item: str(item.get("timestamp")))


def fetch_window(
    article: str,
    start: date,
    end: date,
    *,
    opener=http_get,
    max_attempts: int = 4,
    sleep=None,
    verify_pads: tuple[int, ...] = VERIFY_PADS,
) -> list[dict]:
    """Fetch one date window for one article. Returns [] when there was no traffic.

    No absence is taken at face value. The live API will leave days out of a
    window - with a 404, with `200 {"items": []}`, or with a 200 that simply
    skips some of them - while answering for those same days to a request
    shaped differently. Believing the absence loses the days permanently,
    because the caller advances its watermark past them and never asks again.

    - A non-empty 200 has its interior holes re-asked in narrow slices.
    - An empty answer is widened first, cheaply, and then subdivided until the
      pieces either give up their rows or are single days. Only a window empty
      at every width and every slice is accepted as quiet.

    `sleep` defaults to time.sleep, looked up at call time so tests can patch it.
    """
    sleep = sleep or time.sleep
    items = _fetch_once(article, start, end, opener=opener, max_attempts=max_attempts, sleep=sleep)
    if items:
        return _fill_holes(
            article, start, end, items, opener=opener, max_attempts=max_attempts, sleep=sleep
        )

    log.info("%s %s..%s answered empty; verifying", article, start, end)
    # Widening is a cheap way to get rows, never evidence that there are none.
    widened_rows: list[dict] = []
    for pad in verify_pads:
        widened = _fetch_once(
            article,
            start - timedelta(days=pad),
            end,
            opener=opener,
            max_attempts=max_attempts,
            sleep=sleep,
        )
        inside = [item for item in widened or [] if _within(item, start, end)]
        if inside:
            widened_rows = inside
            break
        # A pad that answers 200 with only padding days has not answered about
        # the window at all, so it falls through to the next pad exactly like
        # a 404 does.

    # Subdivide regardless of what the widening produced: a widened 200 can
    # mention some of the requested days and omit the rest. Subdivision only
    # recurses into pieces that come back empty, so when the data is there
    # this costs two requests.
    #
    # De-duplication is across the two sources only, never within one response.
    # A single response that repeats a day is drift for `one_row_per_date` to
    # judge, and collapsing it here would hide it.
    covered = {str(item.get("timestamp")) for item in widened_rows}
    extra = [
        item
        for item in _subdivide(
            article, start, end, opener=opener, max_attempts=max_attempts, sleep=sleep
        )
        if _within(item, start, end) and str(item.get("timestamp")) not in covered
    ]

    # A stable sort keeps a repeated day in the order it arrived, widened first.
    return sorted(widened_rows + extra, key=lambda item: str(item.get("timestamp")))


@dataclass(frozen=True)
class TitleCheck:
    """What MediaWiki says about one title from venues.csv."""

    title: str
    canonical: str | None  # None when the page does not exist
    redirected: bool

    @property
    def ok(self) -> bool:
        return self.canonical == self.title and not self.redirected


def check_titles(titles: list[str], *, opener=http_get) -> dict[str, TitleCheck]:
    """Resolve titles through the MediaWiki API: canonical, redirect, or missing.

    The pageviews API is title-exact and does not follow redirects, so a
    redirect title reports only the traffic that arrived through it - a
    plausible number, not an obviously wrong one. Nothing downstream can catch
    that, so it is checked here, on request.
    """
    results: dict[str, TitleCheck] = {}
    for offset in range(0, len(titles), 50):  # the API's per-request limit
        batch = titles[offset : offset + 50]
        query = urllib.parse.urlencode(
            {
                "action": "query",
                "format": "json",
                "formatversion": "2",
                "redirects": "1",
                "titles": "|".join(batch),
            }
        )
        url = f"https://{PROJECT}.org/w/api.php?{query}"
        status, _, body = opener(url)
        if status != 200:
            raise ApiError(f"title lookup: HTTP {status} from {url}")
        try:
            data = json.loads(body.decode("utf-8"))["query"]
        except (ValueError, RecursionError, KeyError, TypeError) as exc:
            raise MalformedResponse(f"title lookup: unreadable response ({exc})") from exc

        normalized = {n["from"]: n["to"] for n in data.get("normalized", [])}
        redirects = {r["from"]: r["to"] for r in data.get("redirects", [])}
        pages = {p["title"]: p for p in data.get("pages", [])}
        for title in batch:
            name = normalized.get(title, title)
            seen = set()
            redirected = False
            while name in redirects and name not in seen:
                seen.add(name)
                name = redirects[name]
                redirected = True
            page = pages.get(name, {})
            missing = page.get("missing") or page.get("invalid") or not page
            canonical = None if missing else name.replace(" ", "_")
            results[title] = TitleCheck(title, canonical, redirected)
    return results
