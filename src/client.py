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

import json
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
USER_AGENT = "nz-attraction-pageviews/0.1 (https://github.com/Kenchch/nz-attraction-pageviews)"

# The fields we agreed to consume. Extra fields are fine, missing ones are not.
EXPECTED_FIELDS = frozenset(
    {"project", "article", "granularity", "timestamp", "access", "agent", "views"}
)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
TRANSPORT_STATUS = 599  # our own marker for "the socket died", not an HTTP code

# How far back to extend the start date when re-asking a window that answered
# 404. Observed against the live API: a window can 404 while a wider window
# ending on the same day returns those exact days. No single pad is reliable
# (a 7 day and a 45 day widening each failed on a case the others caught), so
# two are tried. A genuinely absent article 404s at every pad, which is what
# keeps this from inventing data.
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


def http_get(url: str) -> tuple[int, dict[str, str], bytes]:
    """Real network call. Swapped out in tests."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers or {}), exc.read()
    except (urllib.error.URLError, TimeoutError, OSError):
        return TRANSPORT_STATUS, {}, b""


def _backoff_seconds(attempt: int, headers: dict[str, str]) -> float:
    """Honour Retry-After if the server sent one, otherwise 2^attempt + jitter.

    Jitter matters when several venues are being fetched in a loop: without it
    every retry lands on the same second and we re-create the burst that got us
    throttled in the first place.
    """
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return (2.0**attempt) + random.uniform(0, 0.5)


def _parse(body: bytes, article: str) -> list[dict]:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SchemaDriftError(f"{article}: body is not JSON ({exc})") from exc

    items = payload.get("items")
    if items is None:
        raise SchemaDriftError(f"{article}: response has no 'items' key")
    if not isinstance(items, list):
        raise SchemaDriftError(f"{article}: 'items' is {type(items).__name__}, expected list")

    for item in items:
        missing = EXPECTED_FIELDS - set(item)
        if missing:
            raise SchemaDriftError(f"{article}: item missing fields {sorted(missing)}")
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
    while answering 200, for those same days, to a request that starts earlier.
    Believing the 404 loses the days permanently, because the caller advances its
    watermark past them and never asks again. So an empty answer is re-asked with
    a wider start; anything the wider window returns is trimmed back to the range
    that was actually requested. Only when every widening also 404s do we accept
    that there was no traffic.
    """
    items = _fetch_once(
        article, start, end, opener=opener, max_attempts=max_attempts, sleep=sleep
    )
    if items is not None:
        return items

    for pad in verify_pads:
        widened = _fetch_once(
            article,
            start - timedelta(days=pad),
            end,
            opener=opener,
            max_attempts=max_attempts,
            sleep=sleep,
        )
        if widened is not None:
            return [item for item in widened if _within(item, start, end)]

    return []
