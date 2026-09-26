"""Client behaviour when the API answers badly: holes in a 200, bodies that are
not JSON, dead connections, slow bodies, and a run's request budget."""

from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta

import pytest

from nz_attraction_pageviews import client

START, END = date(2026, 1, 1), date(2026, 1, 30)


def body(*days: date, article: str = "Hobbiton_Movie_Set") -> bytes:
    return json.dumps(
        {
            "items": [
                {
                    "project": "en.wikipedia",
                    "article": article,
                    "granularity": "daily",
                    "timestamp": f"{d:%Y%m%d}00",
                    "access": "all-access",
                    "agent": "user",
                    "views": 90,
                }
                for d in days
            ]
        }
    ).encode()


def span(url: str) -> tuple[date, date]:
    tail = url.rsplit("/daily/", 1)[1].split("/")
    return tuple(datetime.strptime(t, "%Y%m%d").date() for t in tail)


def every_day(start: date, end: date) -> list[date]:
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


class WideAnswersPartly:
    """A wide request answers 200 with only the first `wide_gives` days; any
    request of `narrow` days or fewer answers with all of them."""

    def __init__(self, wide_gives: int, narrow: int = 7, quiet: frozenset = frozenset()):
        self.wide_gives, self.narrow, self.quiet = wide_gives, narrow, quiet
        self.urls: list[str] = []

    def __call__(self, url):
        self.urls.append(url)
        s, e = span(url)
        days = [d for d in every_day(s, e) if d not in self.quiet]
        if (e - s).days + 1 > self.narrow and len(days) > self.wide_gives:
            days = days[: self.wide_gives] + days[-1:]  # and the last, so the rest are holes
        return (200, {}, body(*days)) if days else (404, {}, b"")


def test_a_hole_in_the_middle_of_a_200_is_re_asked():
    """A direct 200 was trusted as it stood. The watermark then treats any hole
    before the last day that arrived as quiet and never asks again, so days the
    API would have given to a narrower request were lost for good."""
    api = WideAnswersPartly(wide_gives=5)
    rows = client.fetch_window("Hobbiton_Movie_Set", START, END, opener=api, sleep=lambda _: None)

    assert [r["timestamp"] for r in rows] == [f"{d:%Y%m%d}00" for d in every_day(START, END)]
    assert all((span(u)[1] - span(u)[0]).days < 7 for u in api.urls[1:]), "narrow re-asks"


def test_a_hole_that_stays_empty_is_left_empty():
    quiet = frozenset({date(2026, 1, 10), date(2026, 1, 11)})
    api = WideAnswersPartly(wide_gives=30, quiet=quiet)
    rows = client.fetch_window("Hobbiton_Movie_Set", START, END, opener=api, sleep=lambda _: None)

    got = {r["timestamp"] for r in rows}
    assert len(got) == 28
    assert not {f"{d:%Y%m%d}00" for d in quiet} & got


def test_a_200_with_no_holes_costs_one_request():
    api = WideAnswersPartly(wide_gives=30)
    client.fetch_window("Hobbiton_Movie_Set", START, END, opener=api)
    assert len(api.urls) == 1


def test_days_after_the_last_one_that_arrived_are_not_re_asked():
    """Those are the publication lag. The watermark does not trust them, so they
    are asked for again next run anyway."""
    calls = []

    def lagging(url):
        calls.append(url)
        return 200, {}, body(*every_day(START, END - timedelta(days=3)))

    client.fetch_window("Hobbiton_Movie_Set", START, END, opener=lagging)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "payload",
    [
        b"<html><body>Please log in to the Wi-Fi</body></html>",
        b'{"items": [{"views": ' + b"9" * 5000 + b"}]}",  # longer than int() will parse
        b"[" * 100_000,  # deeper than the parser will go
    ],
    ids=["html-page", "huge-integer", "deep-nesting"],
)
def test_a_200_that_is_not_json_is_retried_then_fails_the_venue_not_the_run(payload):
    """It raised SchemaDriftError, or ValueError or RecursionError, none of which
    is an ApiError - so one captive portal or CDN page aborted all eight venues
    with nothing loaded."""
    calls = []

    def opener(url):
        calls.append(url)
        return 200, {}, payload

    with pytest.raises(client.ApiError, match="not valid UTF-8 JSON"):
        client.fetch_window("Te_Papa", START, START, opener=opener, sleep=lambda _: None)
    assert len(calls) == 4, "retried like any other transient failure"


def test_a_garbled_200_that_recovers_is_used():
    responses = [(200, {}, b"<html>"), (200, {}, body(START))]
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", START, START, opener=lambda u: responses.pop(0), sleep=lambda _: None
    )
    assert len(rows) == 1


def test_a_transport_failure_says_what_failed(monkeypatch):
    """Every one of DNS, refused connection, TLS and a proxy's 403 came out as
    "last status 599"."""

    def no_dns(request, timeout=None):
        raise urllib.error.URLError(OSError("getaddrinfo failed"))

    monkeypatch.setattr(urllib.request, "urlopen", no_dns)
    with pytest.raises(client.TransportError, match="getaddrinfo failed"):
        client.fetch_window("Te_Papa", START, START, sleep=lambda _: None)


def test_a_certificate_failure_is_not_retried(monkeypatch):
    """It will fail the same way in two minutes; retrying only delays the report."""
    calls = []

    def bad_cert(request, timeout=None):
        calls.append(request)
        raise urllib.error.URLError(ssl.SSLCertVerificationError("certificate has expired"))

    monkeypatch.setattr(urllib.request, "urlopen", bad_cert)
    with pytest.raises(client.TransportError, match="not retried"):
        client.fetch_window("Te_Papa", START, START, sleep=lambda _: None)
    assert len(calls) == 1


def test_a_501_is_not_retried():
    """The docstring promised every 5xx was retried; only 500, 502, 503 and 504 are."""
    calls = []

    def opener(url):
        calls.append(url)
        return 501, {}, b""

    with pytest.raises(client.ApiError, match="HTTP 501"):
        client.fetch_window("Te_Papa", START, START, opener=opener, sleep=lambda _: None)
    assert len(calls) == 1


class Trickle:
    """A body that arrives a byte at a time, each byte well inside the socket
    timeout. `clock` advances a quarter of a second per read."""

    def __init__(self):
        self.now = 0.0

    def clock(self):
        return self.now

    def read1(self, n):
        self.now += 0.25
        return b"x"


def test_a_body_that_trickles_in_is_cut_off():
    """The socket timeout bounds each read, not the request: one byte every
    quarter second kept a single request going for 45 seconds with a one
    second timeout, and would have kept it going indefinitely."""
    trickle = Trickle()
    with pytest.raises(TimeoutError, match="still arriving"):
        client._read_bounded(trickle, "u", clock=trickle.clock)
    assert trickle.now <= client.READ_DEADLINE_SECONDS + 1


def test_a_trickling_body_is_a_transport_failure(monkeypatch):
    class Response(Trickle):
        status, headers = 200, {}

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(urllib.request, "urlopen", lambda request, timeout=None: Response())
    monkeypatch.setattr(client, "READ_DEADLINE_SECONDS", -1.0)
    status, headers, _ = client.http_get("https://example.invalid/x")
    assert status == client.TRANSPORT_STATUS
    assert "TimeoutError" in headers[client.TRANSPORT_ERROR_HEADER]


def test_the_counting_opener_counts_and_stops_at_its_budget():
    counter = client.CountingOpener(lambda url: (404, {}, b""), budget=5)
    with pytest.raises(client.RequestBudgetExceeded):
        client.fetch_window("No_Such_Article", START, END, opener=counter)
    assert counter.count == 5


def test_live_requests_need_a_contact(monkeypatch):
    """The User-Agent carried the author's own address, so anyone running a copy
    sent Wikimedia requests in the author's name."""
    monkeypatch.delenv(client.CONTACT_ENV)
    with pytest.raises(client.ConfigError, match="--contact"):
        client.build_user_agent()
    with pytest.raises(client.ConfigError):
        client.http_get("https://example.invalid/x")

    agent = client.build_user_agent("ops@example.org")
    assert "ops@example.org" in agent
    assert f"/{client.__version__} " in agent


def test_check_titles_follows_redirects_and_names_missing_pages():
    """The shapes MediaWiki answered with for venues.csv on 2026-09-26."""
    museum = "Museum_of_New_Zealand_Te_Papa_Tongarewa"
    answer = {
        "query": {
            "normalized": [
                {"from": t, "to": t.replace("_", " ")} for t in ("Te_Papa", museum, "No_Such_Page")
            ],
            "redirects": [{"from": museum.replace("_", " "), "to": "Te Papa"}],
            "pages": [
                {"pageid": 1225932, "ns": 0, "title": "Te Papa"},
                {"ns": 0, "title": "No Such Page", "missing": True},
            ],
        }
    }
    urls = []

    def opener(url):
        urls.append(url)
        return 200, {}, json.dumps(answer).encode()

    checks = client.check_titles(["Te_Papa", museum, "No_Such_Page"], opener=opener)

    assert checks["Te_Papa"].ok
    redirect = checks[museum]
    assert (redirect.ok, redirect.redirected, redirect.canonical) == (False, True, "Te_Papa")
    assert checks["No_Such_Page"].canonical is None
    assert len(urls) == 1 and "redirects=1" in urls[0]
