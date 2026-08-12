"""Client tests. No network: the opener is a stub that replays canned responses."""

from __future__ import annotations

import json
from datetime import date

import pytest

from src import client


def canned(views: int = 100) -> bytes:
    return json.dumps(
        {
            "items": [
                {
                    "project": "en.wikipedia",
                    "article": "Sky_Tower_(Auckland)",
                    "granularity": "daily",
                    "timestamp": "2026010100",
                    "access": "all-access",
                    "agent": "user",
                    "views": views,
                }
            ]
        }
    ).encode()


class Opener:
    """Replays a list of (status, headers, body) and records the URLs it was given."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.urls = []

    def __call__(self, url):
        self.urls.append(url)
        if len(self.responses) > 1:
            return self.responses.pop(0)
        return self.responses[0]


def test_url_quotes_parentheses_in_title():
    url = client.build_url("Sky_Tower_(Auckland)", date(2026, 1, 1), date(2026, 1, 31))
    assert "Sky_Tower_%28Auckland%29" in url
    assert url.endswith("/daily/20260101/20260131")


def test_url_rejects_backwards_range():
    with pytest.raises(ValueError):
        client.build_url("Milford_Sound", date(2026, 2, 1), date(2026, 1, 1))


def test_success_returns_items():
    opener = Opener((200, {}, canned(842)))
    rows = client.fetch_window(
        "Sky_Tower_(Auckland)", date(2026, 1, 1), date(2026, 1, 1), opener=opener
    )
    assert [r["views"] for r in rows] == [842]


def test_404_everywhere_means_no_traffic_not_failure():
    """A quiet article is a normal outcome, not a reason to abort the run."""
    opener = Opener((404, {}, b'{"detail": "not found"}'))
    rows = client.fetch_window(
        "Waitomo_Glowworm_Caves", date(2026, 1, 1), date(2026, 1, 5), opener=opener
    )
    assert rows == []
    expected_calls = 1 + len(client.VERIFY_PADS)
    assert len(opener.urls) == expected_calls, "the 404 should be verified, not trusted"


def days(*stamps: str, article: str = "Hobbiton_Movie_Set") -> bytes:
    return json.dumps(
        {
            "items": [
                {
                    "project": "en.wikipedia",
                    "article": article,
                    "granularity": "daily",
                    "timestamp": s,
                    "access": "all-access",
                    "agent": "user",
                    "views": 90,
                }
                for s in stamps
            ]
        }
    ).encode()


def test_spurious_404_is_recovered_from_a_wider_window():
    """The live API 404s some windows while serving those same days to a wider one.

    Believing the 404 loses the days for good: the caller advances its watermark
    past them and never asks again.
    """
    opener = Opener(
        (404, {}, b'{"detail": "not found"}'),
        (200, {}, days("2025121500", "2026010100", "2026010200")),
    )
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 2), opener=opener
    )
    assert [r["timestamp"] for r in rows] == ["2026010100", "2026010200"], (
        "days outside the requested window must be trimmed off"
    )
    assert "20251217/20260102" in opener.urls[1], "the retry should start earlier, not end later"


def test_verification_falls_through_to_the_second_pad():
    opener = Opener(
        (404, {}, b""),
        (404, {}, b""),
        (200, {}, days("2026010100")),
    )
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 1), opener=opener
    )
    assert [r["timestamp"] for r in rows] == ["2026010100"]
    assert len(opener.urls) == 3


def test_wider_window_with_nothing_in_range_is_still_empty():
    """Verification must not invent rows the requested window did not ask for."""
    opener = Opener((404, {}, b""), (200, {}, days("2025120100", "2025120200")))
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 5), opener=opener
    )
    assert rows == []


def test_verification_keeps_unparseable_timestamps_for_quarantine():
    """Dropping them here would hide the drift; quality.py should see and log it."""
    opener = Opener((404, {}, b""), (200, {}, days("not-a-date", "2026010100")))
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 1), opener=opener
    )
    assert [r["timestamp"] for r in rows] == ["not-a-date", "2026010100"]


def test_a_successful_window_is_not_re_requested():
    opener = Opener((200, {}, canned(842)))
    client.fetch_window("Sky_Tower_(Auckland)", date(2026, 1, 1), date(2026, 1, 1), opener=opener)
    assert len(opener.urls) == 1


def test_429_is_retried_then_succeeds():
    slept = []
    opener = Opener((429, {"Retry-After": "3"}, b""), (200, {}, canned()))
    rows = client.fetch_window(
        "Milford_Sound", date(2026, 1, 1), date(2026, 1, 1), opener=opener, sleep=slept.append
    )
    assert len(rows) == 1
    assert slept == [3.0], "should honour Retry-After rather than its own backoff"


def test_backoff_grows_when_no_retry_after_header():
    slept = []
    opener = Opener((503, {}, b""), (503, {}, b""), (200, {}, canned()))
    client.fetch_window(
        "Milford_Sound", date(2026, 1, 1), date(2026, 1, 1), opener=opener, sleep=slept.append
    )
    assert len(slept) == 2
    assert slept[1] > slept[0]


def test_gives_up_after_max_attempts():
    opener = Opener((500, {}, b""))
    with pytest.raises(client.ApiError, match="gave up after 3"):
        client.fetch_window(
            "Hobbiton_Movie_Set",
            date(2026, 1, 1),
            date(2026, 1, 1),
            opener=opener,
            max_attempts=3,
            sleep=lambda _: None,
        )
    assert len(opener.urls) == 3


def test_400_is_not_retried():
    """A malformed request will be malformed on the retry too."""
    opener = Opener((400, {}, b""))
    with pytest.raises(client.ApiError, match="HTTP 400"):
        client.fetch_window(
            "Bad Title", date(2026, 1, 1), date(2026, 1, 1), opener=opener, sleep=lambda _: None
        )
    assert len(opener.urls) == 1


def test_missing_field_raises_schema_drift():
    body = json.dumps({"items": [{"article": "Milford_Sound", "views": 5}]}).encode()
    opener = Opener((200, {}, body))
    with pytest.raises(client.SchemaDriftError, match="missing fields"):
        client.fetch_window("Milford_Sound", date(2026, 1, 1), date(2026, 1, 1), opener=opener)


def test_missing_items_key_raises_schema_drift():
    opener = Opener((200, {}, b'{"result": []}'))
    with pytest.raises(client.SchemaDriftError, match="no 'items' key"):
        client.fetch_window("Milford_Sound", date(2026, 1, 1), date(2026, 1, 1), opener=opener)
