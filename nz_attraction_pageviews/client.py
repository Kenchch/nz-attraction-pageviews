"""Talks to the Wikimedia pageviews API.

Three behaviours here are the reason this module exists as its own file:

1. The API returns 404 when an article simply had no traffic in the window.
   That is data, not a failure. Treating it as an error would abort a run over
   a quiet Tuesday at a small museum. But a 404 is not proof of no traffic:
   some windows 404 while a wider window covering the same days returns them,
   so an empty answer is verified before it is believed. See `fetch_window`.
2. It is rate limited. 429 and 5xx are retried with exponential backoff and
   jitter; 4xx other than 404 is our own bad request and is not retried,
   because retrying a malformed URL just wastes someone else's capacity.
3. The response shape is a contract. A missing field is schema drift and is
   raised loudly, rather than silently becoming a NULL three tables later.

The network call is injected (`opener`) so the tests run offline.
"""

from __future__ import annotations

import http.client
import json
import math
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta

API_ROOT = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
PROJECT = "en.wikipedia"
ACCESS = "all-access"
AGENT = "user"

# Wikimedia asks for a contactable User-Agent. An anonymous one gets throttled.
USER_AGENT = (
    "nz-attraction-pageviews/0.1 "
    "(https://github.com/Kenchch/nz-attraction-pageviews; mailto:janfinq@gmail.com)"
)

# The fields we agreed to consume. Extra fields are fine, missing ones are not.
EXPECTED_FIELDS = frozenset(
    {"project", "article", "granularity", "timestamp", "access", "agent", "views"}
)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
TRANSPORT_STATUS = 599  # our own marker for "the socket died", not an HTTP code

# Longest we will wait between attempts, however long the server asks for.
# Retry-After is a number from someone else's infrastructure: a misconfigured
# proxy answering 86400 would otherwise park a nightly job for a day, and it
# parses `inf` just as happily. Honour the header up to this, then stop
# honouring it. With max_attempts at 4 the whole retry sequence is bounded at a
# few minutes, after which the window fails loudly and the watermark does not
# move, so the days are re-asked for tomorrow rather than lost.
MAX_BACKOFF_SECONDS = 120.0

# Ceiling on how much of a response body we will read into memory.
#
# `response.read()` with no argument reads until the server stops sending, and
# the server is not ours. The largest legitimate body here is one article's
# daily pageviews for one window: a full year is ~130 bytes per day, about
# 50 KB, and the ingest never asks for more than a few years at a time. 2 MB is
# roughly forty years of daily rows - far past anything real, and small enough
# that a redirected, misconfigured or hostile endpoint streaming an endless
# body is refused rather than paged into a scheduled job.
#
# Refused, not truncated: a truncated JSON body would come back as schema drift
# or, worse, as a valid prefix, and either would blame the payload for what is
# actually a transport problem.
MAX_RESPONSE_BYTES = 2 * 1024 * 1024

# How far back to extend the start date when re-asking a window that answered
# 404. Observed against the live API: a window can 404 while a wider window
# ending on the same day returns those exact days. No single pad is reliable
# (a 7 day and a 45 day widening each failed on a case the others caught), so
# two are tried. This is only the cheap first attempt - `_subdivide` is what
# actually makes the verification trustworthy.
VERIFY_PADS = (15, 30)


class ApiError(RuntimeError):
    """The API answered, and the answer means we should stop."""


class SchemaDriftError(RuntimeError):
    """The payload no longer matches the contract in EXPECTED_FIELDS."""


def build_url(article: str, start: date, end: date) -> str:
    """Build a per-article daily URL.

    The article title is path-quoted with safe="" so that titles like
    "Sky_Tower_(Auckland)" survive the round trip.
    """
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    quoted = urllib.parse.quote(article, safe="")
    return f"{API_ROOT}/{PROJECT}/{ACCESS}/{AGENT}/{quoted}/daily/{start:%Y%m%d}/{end:%Y%m%d}"


def _read_bounded(stream, url: str) -> bytes:
    """Read at most MAX_RESPONSE_BYTES, and refuse anything longer.

    One extra byte is requested so "exactly at the limit" and "over the limit"
    are distinguishable; Content-Length is deliberately not trusted, because a
    body that lies about its length is precisely the case this guards against.
    """
    body = stream.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise ApiError(
            f"{url}: response exceeds {MAX_RESPONSE_BYTES:,} bytes. The largest "
            f"real body here is a few tens of KB, so this is a wrong endpoint "
            f"or a broken one, not data."
        )
    return body


def http_get(url: str) -> tuple[int, dict[str, str], bytes]:
    """Real network call. Swapped out in tests."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, dict(response.headers), _read_bounded(response, url)
    except urllib.error.HTTPError as exc:
        # `with exc:` - an HTTPError IS the response, holding the same socket,
        # and it was only ever closed by the garbage collector. That is not
        # deterministic even on CPython: raising ApiError from _read_bounded
        # puts the frame holding `exc` in a traceback, so the socket outlives
        # the call by however long the exception is handled. pytest saw it as
        # a PytestUnraisableExceptionWarning on every Python version in CI -
        # a real descriptor leak surfacing as a test warning.
        #
        # The body is read for the same reason the success path is bounded: it
        # is the same socket, and an endpoint answering 500 with an endless
        # body is if anything likelier.
        with exc:
            try:
                body = _read_bounded(exc, url)
            except (OSError, http.client.HTTPException):
                # An error body can be truncated mid-read just as a success
                # body can. That is a transport failure, not a 4xx/5xx worth
                # reporting as one.
                return TRANSPORT_STATUS, {}, b""
            return exc.code, dict(exc.headers or {}), body
    except (
        urllib.error.URLError,
        TimeoutError,
        OSError,
        # http.client.HTTPException is NOT an OSError - IncompleteRead, which
        # is what a truncated chunked response raises, inherits straight from
        # Exception. So it escaped this handler entirely: one truncated
        # response killed the whole night's run for all eight venues, with
        # zero retries, on the one class of failure retries exist for.
        http.client.HTTPException,
    ):
        return TRANSPORT_STATUS, {}, b""


def _backoff_seconds(attempt: int, headers: dict[str, str]) -> float:
    """Honour Retry-After if the server sent one, otherwise 2^attempt + jitter.

    Jitter matters when several venues are being fetched in a loop: without it
    every retry lands on the same second and we re-create the burst that got us
    throttled in the first place.

    Both paths are capped at MAX_BACKOFF_SECONDS. Retry-After is a number chosen
    by someone else's infrastructure, and `float` accepts `inf` and `nan` as
    readily as `30`, so it is taken as a request rather than an instruction.
    A header we cannot make sense of falls through to our own backoff.
    """
    # HTTP header names are case-insensitive (RFC 9110 5.1) and this dict is
    # whatever the caller built - the real client passes dict(response.headers),
    # which preserves the server's casing verbatim. Checking two spellings
    # covered the two we happened to have seen; a proxy sending RETRY-AFTER or
    # Retry-after fell through to our own backoff, silently ignoring the delay
    # the server asked for. Fold the case once instead of guessing spellings.
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
    try:
        # Decoded explicitly. json.loads on bytes sniffs the encoding and copes
        # with a leading BOM, so a BOM-like prefix surfaced as a
        # JSONDecodeError and looked handled - but invalid bytes anywhere else
        # raised a bare UnicodeDecodeError out of this function, naming neither
        # the article nor the contract. Two of three malformed bodies tested
        # took that path.
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaDriftError(f"{article}: body is not valid UTF-8 JSON ({exc})") from exc

    if not isinstance(payload, dict):
        # payload.get() on a list or a string raises AttributeError from inside
        # this function, which names neither the article nor the problem.
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
            # Without this the field check below raises a bare TypeError from
            # somewhere inside a set operation, which is neither loud nor named.
            raise SchemaDriftError(f"{article}: item is {type(item).__name__}, expected an object")
        missing = EXPECTED_FIELDS - set(item)
        if missing:
            raise SchemaDriftError(f"{article}: item missing fields {sorted(missing)}")

        # Presence was never the risk. These four fields describe WHAT was
        # counted, and a response answering with different ones is not an error
        # anywhere downstream: every acceptance rule in quality.py judges the
        # date and the count, so a de.wikipedia / monthly / desktop / spider row
        # arrived as a clean row carrying a month of German bot traffic under a
        # single New Zealand day, loaded, and advanced the watermark past it.
        # Verified before this check existed: 1 clean row, 0 quarantined.
        #
        # This is drift, not a bad row, so it stops the run rather than being
        # quarantined - it means the request and the response disagree about the
        # question, and every other row in the response is equally suspect.
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
            # parse_timestamp would coerce an int and quietly succeed on some
            # values, so the type is checked where the contract is stated.
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
    last_status = None

    for attempt in range(1, max_attempts + 1):
        status, headers, body = opener(url)
        last_status = status

        if status == 200:
            return _parse(body, article)
        if status == 404:
            return None
        if status in RETRYABLE_STATUS or status == TRANSPORT_STATUS:
            if attempt < max_attempts:
                sleep(_backoff_seconds(attempt, headers))
                continue
            break
        # 400, 403, anything else: retrying will not change the answer.
        raise ApiError(f"{article}: HTTP {status} from {url}")

    raise ApiError(f"{article}: gave up after {max_attempts} attempts, last status {last_status}")


def _within(item: dict, start: date, end: date) -> bool:
    """Is this item's day inside start..end?

    An unparseable timestamp is kept rather than dropped, so that the acceptance
    criteria quarantine it visibly instead of it vanishing here in silence.
    """
    stamp = str(item.get("timestamp", ""))
    if len(stamp) < 8 or not stamp[:8].isdigit():
        return True
    return f"{start:%Y%m%d}" <= stamp[:8] <= f"{end:%Y%m%d}"


def _subdivide(
    article: str,
    start: date,
    end: date,
    *,
    opener,
    max_attempts: int,
    sleep,
) -> list[dict]:
    """Halve a 404 window and ask about each piece. Recurses only into the pieces
    that also 404, so a spurious 404 costs a handful of requests and only a window
    that is empty all the way down pays for the whole tree.

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
            # evidence, so both are taken apart further - see `fetch_window`.
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


def fetch_window(
    article: str,
    start: date,
    end: date,
    *,
    opener=http_get,
    max_attempts: int = 4,
    sleep=time.sleep,
    verify_pads: tuple[int, ...] = VERIFY_PADS,
) -> list[dict]:
    """Fetch one date window for one article. Returns [] when there was no traffic.

    A 404 is not taken at face value. The live API will answer 404 for a window
    while answering 200, for those same days, to a request shaped differently.
    Believing the 404 loses the days permanently, because the caller advances its
    watermark past them and never asks again.

    So an empty answer is checked twice over. First cheaply, by asking again with
    an earlier start and trimming the answer back to the range requested. Then, if
    that still comes back empty, by halving the window and asking about the pieces
    until either they give up their rows or they are single days. Only a window
    that is empty at every width and every slice is accepted as quiet.

    "Empty" means empty, not 404. A 200 carrying `items: []` says exactly what the
    404 says - no days - and deserves exactly as little trust. Verifying only the
    404 left the same hole this function exists to close, reachable by an upstream
    that reports nothing with a success code instead of an error one.
    """
    items = _fetch_once(article, start, end, opener=opener, max_attempts=max_attempts, sleep=sleep)
    if items:
        return items

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
        # Two ways a pad can fail to answer, and both have to fall through to
        # the next one.
        #
        # The pad 404s or returns nothing - the case this always handled.
        #
        # The pad returns 200 with rows, but every row is a padding day and none
        # lands in the requested window. Breaking on the raw response treated
        # that as an answer about days it never mentioned, and stopped before the
        # pad that would have produced them: with pads (15, 30), a padding-only
        # reply at 15 meant the 30 day window was never asked, and if every
        # subdivision slice 404s too the days are gone while the run reports ok.
        # That is the exact failure this module exists to prevent, arriving
        # through the success path instead of the 404 path.

    # Subdivide regardless of what the widening produced: a widened 200 can
    # mention some of the requested days and quietly omit the rest, which is the
    # same failure as the 404 wearing a success code. Subdivision only recurses
    # into pieces that 404, so when the data is there this costs two requests.
    #
    # De-duplication is across the two sources only, never within one response.
    # A single response that repeats a day is drift, and collapsing it here would
    # mean `one_row_per_date` never sees it - the same silent drop this module
    # exists to avoid.
    covered = {str(item.get("timestamp")) for item in widened_rows}
    extra = [
        item
        for item in _subdivide(
            article, start, end, opener=opener, max_attempts=max_attempts, sleep=sleep
        )
        if _within(item, start, end) and str(item.get("timestamp")) not in covered
    ]

    # A stable sort keeps a repeated day in the order it arrived, widened first,
    # so `one_row_per_date` quarantines the second exactly as on the direct path.
    return sorted(widened_rows + extra, key=lambda item: str(item.get("timestamp")))
