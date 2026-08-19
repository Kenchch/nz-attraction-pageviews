"""Client tests. No network: the opener is a stub that replays canned responses."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest

from nz_attraction_pageviews import client


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
    assert len(opener.urls) > 1 + len(client.VERIFY_PADS), (
        "empty should only be believed after widening and subdividing"
    )


def test_a_single_day_404_is_taken_at_face_value():
    """There is nothing left to subdivide, so this is the one cheap case."""
    opener = Opener((404, {}, b""))
    rows = client.fetch_window("Milford_Sound", date(2026, 1, 1), date(2026, 1, 1), opener=opener)
    assert rows == []
    assert len(opener.urls) == 1 + len(client.VERIFY_PADS)


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


def test_wider_window_never_returns_days_outside_the_request():
    """Verification must not invent rows the requested window did not ask for."""
    opener = Opener(
        (404, {}, b""),
        (200, {}, days("2025120100", "2026010200", "2026010300")),
    )
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 5), opener=opener
    )
    assert [r["timestamp"] for r in rows] == ["2026010200", "2026010300"]


def test_a_wider_200_that_skips_every_requested_day_is_not_believed():
    """The premise of this module is that whether a response contains the days you
    asked for depends on the shape of the request. A 200 that mentions none of
    them is no more an answer about them than the 404 was - and trusting it is
    worse, because the caller reads empty as verified-quiet and advances its
    watermark over the lot."""
    opener = Opener((404, {}, b""), (200, {}, days("2025120100", "2025120200")))

    client.fetch_window("Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 5), opener=opener)

    assert len(opener.urls) > 1 + len(client.VERIFY_PADS), (
        "an empty trim should fall through to subdividing, not be taken as proof"
    )


def test_a_wider_200_that_skips_the_days_still_finds_them_by_subdividing():
    api = WidthSensitiveApi(max_span_days=2)
    rows = client.fetch_window("Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 4), opener=api)
    assert [r["timestamp"] for r in rows] == [
        "2026010100",
        "2026010200",
        "2026010300",
        "2026010400",
    ]


def test_verification_keeps_unparseable_timestamps_for_quarantine():
    """Dropping them here would hide the drift; quality.py should see and log it."""
    opener = Opener((404, {}, b""), (200, {}, days("not-a-date", "2026010100")))
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 1), opener=opener
    )
    assert sorted(r["timestamp"] for r in rows) == ["2026010100", "not-a-date"]


class WidthSensitiveApi:
    """Stands in for the live API's oddest habit: refusing a span while happily
    serving the same days in smaller pieces.

    `Hobbiton_Movie_Set` really did answer 404 for 2026-07-13..2026-08-11 and for
    both widened retries, while every 7 day slice of that range answered 200.
    """

    def __init__(self, max_span_days, *, exists=True):
        self.max_span_days = max_span_days
        self.exists = exists
        self.urls = []

    def __call__(self, url):
        self.urls.append(url)
        tail = url.rsplit("/daily/", 1)[1]
        start = datetime.strptime(tail.split("/")[0], "%Y%m%d").date()
        end = datetime.strptime(tail.split("/")[1], "%Y%m%d").date()

        if not self.exists or (end - start).days + 1 > self.max_span_days:
            return 404, {}, b'{"detail": "not found"}'

        stamps = []
        cursor = start
        while cursor <= end:
            stamps.append(f"{cursor:%Y%m%d}00")
            cursor += timedelta(days=1)
        return 200, {}, days(*stamps)


def test_a_window_answered_only_in_slices_is_recovered_whole():
    api = WidthSensitiveApi(max_span_days=7)
    start, end = date(2026, 7, 13), date(2026, 8, 11)

    rows = client.fetch_window("Hobbiton_Movie_Set", start, end, opener=api)

    assert len(rows) == 30, "every day of the window should come back"
    assert [r["timestamp"] for r in rows] == sorted(r["timestamp"] for r in rows), (
        "the pieces must be stitched back together in order"
    )
    assert rows[0]["timestamp"] == "2026071300"
    assert rows[-1]["timestamp"] == "2026081100"


def test_subdivision_returns_nothing_for_an_article_that_does_not_exist():
    """404 all the way down to single days is the one honest empty."""
    api = WidthSensitiveApi(max_span_days=7, exists=False)

    rows = client.fetch_window("No_Such_Article", date(2026, 1, 1), date(2026, 1, 8), opener=api)

    assert rows == []
    single_days = [u for u in api.urls if u.endswith(u.rsplit("/", 1)[1]) and _is_single_day(u)]
    assert single_days, "it should have gone all the way down before giving up"


def _is_single_day(url: str) -> bool:
    tail = url.rsplit("/daily/", 1)[1].split("/")
    return tail[0] == tail[1]


def test_a_widened_200_carrying_only_pad_days_still_recovers_the_window():
    """The widened retry succeeds and looks like an answer, but every row in it
    falls in the pad region. The requested 30 days are only reachable in slices."""

    class PadRegionOnly:
        def __init__(self):
            self.urls = []

        def __call__(self, url):
            self.urls.append(url)
            tail = url.rsplit("/daily/", 1)[1].split("/")
            s = datetime.strptime(tail[0], "%Y%m%d").date()
            e = datetime.strptime(tail[1], "%Y%m%d").date()
            span = (e - s).days + 1
            if s < date(2026, 7, 13):  # a widened request: answers, but only pad days
                return 200, {}, days("2026070100", "2026070200")
            if span > 7:  # the window itself, and its halves
                return 404, {}, b""
            stamps, cursor = [], s
            while cursor <= e:
                stamps.append(f"{cursor:%Y%m%d}00")
                cursor += timedelta(days=1)
            return 200, {}, days(*stamps)

    api = PadRegionOnly()
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 7, 13), date(2026, 8, 11), opener=api
    )

    assert len(rows) == 30, f"expected the whole window, got {len(rows)}"
    assert rows[0]["timestamp"] == "2026071300"
    assert rows[-1]["timestamp"] == "2026081100"
    assert all(r["timestamp"] >= "2026071300" for r in rows), "pad days must not leak in"


def test_first_pad_with_only_outside_rows_does_not_hide_second_pad():
    """A padding-only 200 at the first pad must not stop the second pad running.

    `test_a_widened_200_carrying_only_pad_days_still_recovers_the_window` above
    survives this bug, because its slices answer and subdivision rescues the
    window. Here nothing answers except the second pad: the direct window 404s,
    every slice down to single days 404s, and the first pad replies 200 with a
    row from the pad region only. Breaking on that raw 200 spent the first pad's
    silence as the second pad's evidence and returned zero rows for three days
    that exist - with the run still reporting ok, because rows that never
    arrived are never rejected.
    """

    class OnlySecondPadAnswers:
        def __init__(self):
            self.spans = []

        def __call__(self, url):
            tail = url.rsplit("/daily/", 1)[1].split("/")
            s = datetime.strptime(tail[0], "%Y%m%d").date()
            e = datetime.strptime(tail[1], "%Y%m%d").date()
            self.spans.append((e - s).days)
            if s == date(2026, 3, 10) - timedelta(days=client.VERIFY_PADS[0]):
                return 200, {}, days("2026030100")  # pad region only
            if s == date(2026, 3, 10) - timedelta(days=client.VERIFY_PADS[1]):
                return 200, {}, days("2026031000", "2026031100", "2026031200")
            return 404, {}, b""  # window and every slice

    api = OnlySecondPadAnswers()
    rows = client.fetch_window(
        "Waitomo_Glowworm_Cave", date(2026, 3, 10), date(2026, 3, 12), opener=api
    )

    second_pad_span = 2 + client.VERIFY_PADS[1]
    assert second_pad_span in api.spans, "the second pad was never asked"
    assert [r["timestamp"] for r in rows] == ["2026031000", "2026031100", "2026031200"]


def test_a_partial_widened_200_is_completed_by_subdividing():
    """Returning early on a partial answer left the omitted days for the watermark
    to notice. They are recoverable here, so they are recovered here."""

    class PartialThenSlices:
        def __init__(self):
            self.urls = []

        def __call__(self, url):
            self.urls.append(url)
            tail = url.rsplit("/daily/", 1)[1].split("/")
            s = datetime.strptime(tail[0], "%Y%m%d").date()
            e = datetime.strptime(tail[1], "%Y%m%d").date()
            if s < date(2026, 1, 1):  # widened: only the first two requested days
                return 200, {}, days("2026010100", "2026010200")
            if (e - s).days + 1 > 2:
                return 404, {}, b""
            stamps, cursor = [], s
            while cursor <= e:
                stamps.append(f"{cursor:%Y%m%d}00")
                cursor += timedelta(days=1)
            return 200, {}, days(*stamps)

    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 8), opener=PartialThenSlices()
    )

    assert [r["timestamp"] for r in rows] == [f"2026010{n}00" for n in range(1, 9)]


def test_recovery_keeps_a_repeated_day_for_the_quarantine():
    """Same payload, same outcome, whichever path it arrived by. Collapsing the
    repeat here would mean `one_row_per_date` never sees the drift."""
    opener = Opener((404, {}, b""), (200, {}, days("2026010100", "2026010100")))
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 1), opener=opener
    )
    assert [r["timestamp"] for r in rows] == ["2026010100", "2026010100"]


class WidenAndSliceApi:
    """Refuses the exact window, answers everything else - so the widening and
    the subdivision both supply the same days."""

    def __init__(self, start, end):
        self.start, self.end = start, end
        self.urls = []

    def __call__(self, url):
        self.urls.append(url)
        tail = url.rsplit("/daily/", 1)[1].split("/")
        start = datetime.strptime(tail[0], "%Y%m%d").date()
        end = datetime.strptime(tail[1], "%Y%m%d").date()
        if (start, end) == (self.start, self.end):
            return 404, {}, b'{"detail": "not found"}'

        stamps, cursor = [], start
        while cursor <= end:
            stamps.append(f"{cursor:%Y%m%d}00")
            cursor += timedelta(days=1)
        return 200, {}, days(*stamps)


def test_a_day_supplied_by_both_sources_is_returned_once():
    """Merging must not turn one day into two just because two requests saw it."""
    start, end = date(2026, 1, 1), date(2026, 1, 4)
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", start, end, opener=WidenAndSliceApi(start, end)
    )
    assert [r["timestamp"] for r in rows] == [
        "2026010100",
        "2026010200",
        "2026010300",
        "2026010400",
    ]


def test_subdivision_trims_days_nobody_asked_about():
    """A slice can answer with days outside its own range. The two verification
    paths should not disagree about what this function may return."""

    class Sloppy:
        def __call__(self, url):
            tail = url.rsplit("/daily/", 1)[1].split("/")
            if tail[0] == tail[1]:  # single day: answer, but add a stray
                return 200, {}, days(tail[0] + "00", "20200101" + "00")
            return 404, {}, b""

    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 2), opener=Sloppy()
    )
    assert [r["timestamp"] for r in rows] == ["2026010100", "2026010200"]


def test_subdivision_only_recurses_into_the_pieces_that_failed():
    """Cost control: a slice that answers is not taken apart any further."""
    api = WidthSensitiveApi(max_span_days=7)
    client.fetch_window("Hobbiton_Movie_Set", date(2026, 7, 13), date(2026, 8, 11), opener=api)
    assert len(api.urls) < 20, f"should not walk the whole tree, used {len(api.urls)}"


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


def test_absurd_retry_after_is_capped():
    """Retry-After comes from someone else's infrastructure. A misconfigured
    proxy answering 86400 would otherwise park a nightly job for a day."""
    assert client._backoff_seconds(1, {"Retry-After": "86400"}) == client.MAX_BACKOFF_SECONDS


def test_reasonable_retry_after_is_still_honoured_exactly():
    assert client._backoff_seconds(1, {"Retry-After": "30"}) == 30.0


def test_non_finite_retry_after_falls_back_to_our_own_backoff():
    """`float` accepts 'inf' and 'nan' as readily as '30'. Sleeping either is a hang."""
    for header in ("inf", "nan", "-inf"):
        seconds = client._backoff_seconds(1, {"Retry-After": header})
        assert 0 < seconds <= client.MAX_BACKOFF_SECONDS, f"{header} produced {seconds}"


def test_negative_retry_after_does_not_go_backwards():
    assert client._backoff_seconds(1, {"Retry-After": "-5"}) == 0.0


def test_our_own_backoff_is_capped_too():
    assert client._backoff_seconds(40, {}) == client.MAX_BACKOFF_SECONDS


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


@pytest.mark.parametrize("item", ["5", "null", '"a string"', "[]"])
def test_non_object_item_raises_schema_drift_not_a_bare_typeerror(item):
    """The module promises schema drift is raised loudly and by name. A TypeError
    from inside a set operation is neither."""
    opener = Opener((200, {}, b'{"items": [%s]}' % item.encode()))
    with pytest.raises(client.SchemaDriftError, match="expected an object"):
        client.fetch_window("Milford_Sound", date(2026, 1, 1), date(2026, 1, 1), opener=opener)


class EmptyWideApi:
    """The success-code twin of `WidthSensitiveApi`: anything wider than
    `max_span_days` answers 200 with no rows at all, narrower spans answer with
    the days they cover."""

    def __init__(self, max_span_days):
        self.max_span_days = max_span_days
        self.urls = []

    def __call__(self, url):
        self.urls.append(url)
        tail = url.rsplit("/daily/", 1)[1].split("/")
        start = datetime.strptime(tail[0], "%Y%m%d").date()
        end = datetime.strptime(tail[1], "%Y%m%d").date()
        if (end - start).days + 1 > self.max_span_days:
            return 200, {}, b'{"items": []}'

        stamps, cursor = [], start
        while cursor <= end:
            stamps.append(f"{cursor:%Y%m%d}00")
            cursor += timedelta(days=1)
        return 200, {}, days(*stamps)


def test_an_empty_200_is_verified_exactly_like_a_404():
    """`{"items": []}` says what the 404 says - no days - so it earns the same
    scrutiny. Verifying only the 404 left the identical hole open to any upstream
    that reports nothing with a success code."""
    opener = Opener((200, {}, b'{"items": []}'))
    rows = client.fetch_window(
        "Waitomo_Glowworm_Cave", date(2026, 1, 1), date(2026, 1, 5), opener=opener
    )
    assert rows == []
    assert len(opener.urls) > 1 + len(client.VERIFY_PADS), (
        "an empty 200 should widen and subdivide before it is believed"
    )


def test_a_window_that_answers_empty_but_slices_into_rows_is_recovered():
    api = EmptyWideApi(max_span_days=7)
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 7, 13), date(2026, 8, 11), opener=api
    )
    assert len(rows) == 30, "an empty 200 hides days exactly as a 404 does"
    assert [r["timestamp"] for r in rows] == sorted(r["timestamp"] for r in rows)


def test_a_200_with_rows_is_still_taken_at_face_value():
    """Only an *empty* answer is suspect; a window that answered costs one call."""
    opener = Opener((200, {}, canned(500)))
    rows = client.fetch_window(
        "Sky_Tower_(Auckland)", date(2026, 1, 1), date(2026, 1, 1), opener=opener
    )
    assert [r["views"] for r in rows] == [500]
    assert len(opener.urls) == 1


def test_max_backoff_is_two_minutes():
    """Pinned to the literal: asserting against the constant passes for any value."""
    assert client.MAX_BACKOFF_SECONDS == 120.0


def test_default_max_attempts_is_four():
    opener = Opener((500, {}, b""))
    with pytest.raises(client.ApiError):
        client.fetch_window(
            "Milford_Sound", date(2026, 1, 1), date(2026, 1, 1), opener=opener, sleep=lambda _: None
        )
    assert len(opener.urls) == 4, "the shipped default, not one a test passed in"


def test_backoff_does_not_sleep_after_the_last_attempt():
    slept = []
    opener = Opener((503, {}, b""))
    with pytest.raises(client.ApiError):
        client.fetch_window(
            "Milford_Sound",
            date(2026, 1, 1),
            date(2026, 1, 1),
            opener=opener,
            max_attempts=3,
            sleep=slept.append,
        )
    assert len(slept) == 2, "a sleep before giving up buys nothing and costs two minutes"


def test_backoff_is_the_documented_powers_of_two():
    slept = []
    opener = Opener((503, {}, b""), (503, {}, b""), (200, {}, canned()))
    client.fetch_window(
        "Milford_Sound", date(2026, 1, 1), date(2026, 1, 1), opener=opener, sleep=slept.append
    )
    assert 2.0 <= slept[0] < 2.5
    assert 4.0 <= slept[1] < 4.5


def test_backoff_jitter_actually_varies():
    """Without jitter every venue's retry lands on the same second."""
    assert len({client._backoff_seconds(2, {}) for _ in range(20)}) > 1


def test_an_empty_pad_is_not_evidence_either():
    """The pads differ in what they catch, so spending the first pad's silence as
    though it were the second pad's answer loses whatever only the second finds."""
    opener = Opener(
        (404, {}, b""),
        (200, {}, b'{"items": []}'),
        (200, {}, days("2026010100")),
    )
    rows = client.fetch_window(
        "Hobbiton_Movie_Set", date(2026, 1, 1), date(2026, 1, 1), opener=opener
    )
    assert [r["timestamp"] for r in rows] == ["2026010100"], "the second pad had it"
    assert len(opener.urls) == 3


def test_a_response_answering_a_different_question_is_drift_not_a_clean_row():
    """The four descriptor fields say WHAT was counted, and nothing downstream
    reads them.

    Field presence was checked; values were not. Every acceptance rule in
    quality.py judges the date and the count, so a reply describing German
    Wikipedia, monthly granularity, desktop access and spider traffic passed
    straight through: verified before this check existed as 1 clean row and 0
    quarantined, carrying a month of bot traffic under a single New Zealand day
    and advancing the watermark past it.

    It stops the run rather than quarantining the row: the request and the
    response disagree about the question, so every other row in the payload is
    equally suspect.
    """
    good = {
        "project": client.PROJECT,
        "article": "Te_Papa",
        "granularity": "daily",
        "timestamp": "2026030100",
        "access": client.ACCESS,
        "agent": client.AGENT,
        "views": 12,
    }
    assert client._parse(json.dumps({"items": [good]}).encode(), "Te_Papa") == [good]

    for field, wrong in (
        ("project", "de.wikipedia"),
        ("granularity", "monthly"),
        ("access", "desktop"),
        ("agent", "spider"),
    ):
        payload = {"items": [{**good, field: wrong}]}
        with pytest.raises(client.SchemaDriftError, match=field):
            client._parse(json.dumps(payload).encode(), "Te_Papa")

    with pytest.raises(client.SchemaDriftError, match="timestamp"):
        client._parse(
            json.dumps({"items": [{**good, "timestamp": 2026030100}]}).encode(), "Te_Papa"
        )


def test_a_top_level_json_that_is_not_an_object_is_named_drift():
    """payload.get() on a list raised AttributeError from inside _parse, naming
    neither the article nor the problem."""
    for body in (b"[]", b'"a string"', b"123"):
        with pytest.raises(client.SchemaDriftError, match="top-level JSON"):
            client._parse(body, "Te_Papa")


@pytest.mark.parametrize(
    "body",
    [
        b'\xff\xfe{"items":[]}',  # invalid bytes at the start
        b'{"items":[{"article":"\xff\xfe"}]}',  # invalid bytes inside a string
        b"\x80\x81\x82",  # not JSON and not UTF-8
    ],
)
def test_a_body_that_is_not_utf8_json_is_named_drift(body):
    """json.loads on bytes sniffs the encoding and copes with a leading BOM, so
    only the first of these surfaced as a JSONDecodeError and looked handled.
    The other two raised a bare UnicodeDecodeError out of _parse, naming neither
    the article nor the contract."""
    with pytest.raises(client.SchemaDriftError, match="UTF-8 JSON"):
        client._parse(body, "Te_Papa")


class _Stream:
    """Minimal stand-in for the object urlopen yields.

    read(n) honours n, and close() is recorded - a response that is never
    closed is a socket held until the garbage collector gets to it.
    """

    def __init__(self, body: bytes):
        self.body = body
        self.asked = None
        self.closed = False

    def read(self, n: int | None = None) -> bytes:
        self.asked = n
        return self.body if n is None else self.body[:n]

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("n", [0, 1024, client.MAX_RESPONSE_BYTES])
def test_a_body_up_to_the_limit_is_read_whole(n):
    """The ceiling has to be a ceiling, not a truncation point. Exactly at the
    limit is still a legitimate body and must come back byte for byte."""
    stream = _Stream(b"x" * n)
    assert client._read_bounded(stream, "u") == b"x" * n


def test_a_body_over_the_limit_is_refused_rather_than_truncated():
    """`response.read()` with no argument reads until the server stops sending,
    and the server is not ours: a redirected, misconfigured or hostile endpoint
    streaming an endless body would be paged into a scheduled job. One byte
    over is enough to prove the boundary is where it claims to be.

    Refused, not truncated - a truncated JSON body comes back as schema drift,
    or worse as a valid prefix, and both blame the payload for a transport
    problem.
    """
    stream = _Stream(b"x" * (client.MAX_RESPONSE_BYTES + 1))
    with pytest.raises(client.ApiError, match="exceeds"):
        client._read_bounded(stream, "https://example.invalid/big")
    # One byte more than the limit is requested, so "at" and "over" differ.
    assert stream.asked == client.MAX_RESPONSE_BYTES + 1


@pytest.mark.parametrize("size", [16, client.MAX_RESPONSE_BYTES + 1])
def test_an_error_response_is_bounded_and_closed(monkeypatch, size):
    """HTTPError IS the response - same socket, same unbounded read - and it was
    only ever released by the garbage collector.

    That is not deterministic even on CPython: raising ApiError out of
    _read_bounded puts the frame holding `exc` into a traceback, so the socket
    outlives the call for as long as the exception is being handled. CI saw it
    on every Python version as a PytestUnraisableExceptionWarning.

    Driven through http_get with urlopen patched, rather than by calling
    _read_bounded directly, because it is http_get's handling of the error
    response that is under test. Both sizes must close: the one that returns
    normally and the one that raises.
    """
    import urllib.error
    import urllib.request

    stream = _Stream(b"x" * size)
    exc = urllib.error.HTTPError("u", 500, "boom", {"Retry-After": "1"}, stream)

    def boom(request, timeout=None):
        raise exc

    monkeypatch.setattr(urllib.request, "urlopen", boom)

    if size > client.MAX_RESPONSE_BYTES:
        with pytest.raises(client.ApiError, match="exceeds"):
            client.http_get("https://example.invalid/x")
    else:
        status, headers, body = client.http_get("https://example.invalid/x")
        assert (status, body) == (500, b"x" * size)
        assert headers["Retry-After"] == "1"

    assert stream.closed, "the error response was left for the garbage collector"


@pytest.mark.parametrize(
    "spelling", ["Retry-After", "retry-after", "RETRY-AFTER", "Retry-after", "retry-After"]
)
def test_retry_after_is_found_whatever_the_server_capitalised_it_as(spelling):
    """HTTP header names are case-insensitive (RFC 9110 5.1), and this dict is
    dict(response.headers) - the server's casing, verbatim. Two spellings were
    checked by hand, so a proxy sending any other one fell through to our own
    backoff and the delay the server asked for was silently ignored."""
    assert client._backoff_seconds(1, {spelling: "30"}) == 30.0
