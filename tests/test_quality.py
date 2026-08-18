"""Acceptance criteria tests."""

from __future__ import annotations

import unicodedata
from datetime import date

import pytest

from nz_attraction_pageviews import quality

ARTICLE = "Milford_Sound"
START = date(2026, 1, 1)
END = date(2026, 1, 31)
TODAY = date(2026, 2, 10)


def item(timestamp="2026010100", views=120, article=ARTICLE):
    return {
        "project": "en.wikipedia",
        "article": article,
        "granularity": "daily",
        "timestamp": timestamp,
        "access": "all-access",
        "agent": "user",
        "views": views,
    }


def check(items):
    return quality.check_window(
        items, venue_id="milford-sound", article=ARTICLE, start=START, end=END, today=TODAY
    )


def test_good_row_passes():
    clean, bad = check([item()])
    assert bad == []
    assert clean[0].view_date == date(2026, 1, 1)
    assert clean[0].views == 120


def test_negative_views_quarantined():
    clean, bad = check([item(views=-4)])
    assert clean == []
    assert bad[0].rule == "views_non_negative"


def test_string_views_quarantined():
    clean, bad = check([item(views="120")])
    assert clean == []
    assert bad[0].rule == "views_is_integer"


def test_bool_is_not_an_integer():
    """True == 1 in Python. Without an explicit check this loads as 1 view."""
    clean, bad = check([item(views=True)])
    assert clean == []
    assert bad[0].rule == "views_is_integer"


def test_date_outside_window_quarantined():
    clean, bad = check([item(timestamp="2025120100")])
    assert clean == []
    assert bad[0].rule == "date_in_requested_window"


def test_unparseable_timestamp_quarantined():
    clean, bad = check([item(timestamp="2026-01-01")])
    assert clean == []
    assert bad[0].rule == "timestamp_parses"


def test_wrong_article_quarantined():
    clean, bad = check([item(article="Doubtful_Sound")])
    assert clean == []
    assert bad[0].rule == "article_matches_request"


def test_duplicate_date_keeps_first_quarantines_second():
    clean, bad = check([item(views=100), item(views=999)])
    assert [r.views for r in clean] == [100]
    assert bad[0].rule == "one_row_per_date"


def test_quarantined_row_keeps_the_raw_payload():
    _, bad = check([item(views=-1)])
    assert "-1" in bad[0].raw


def test_quarantined_row_records_its_day():
    """Without this, "why is Tuesday missing" is a string match against `raw`."""
    _, bad = check([item(timestamp="2026011500", views=-1)])
    assert bad[0].view_date == date(2026, 1, 15)


def test_unparseable_timestamp_quarantines_with_a_null_day():
    """The one rejection that genuinely has no day. Hence a nullable column."""
    _, bad = check([item(timestamp="2026-01-01")])
    assert bad[0].rule == "timestamp_parses"
    assert bad[0].view_date is None


def test_reject_rate_is_available_without_the_gate():
    assert quality.reject_rate(fetched=100, quarantined=9) == 0.09
    assert quality.reject_rate(fetched=0, quarantined=0) == 0.0


def test_gate_passes_under_threshold():
    assert quality.enforce_gate(fetched=100, quarantined=3, max_reject_rate=0.05) == 0.03


def test_gate_fails_over_threshold():
    with pytest.raises(quality.QualityGateFailed, match="Nothing was loaded"):
        quality.enforce_gate(fetched=100, quarantined=9, max_reject_rate=0.05)


def test_gate_handles_empty_run():
    assert quality.enforce_gate(fetched=0, quarantined=0, max_reject_rate=0.05) == 0.0


def test_zero_views_is_clean_not_quarantined():
    """A day the API does report as zero is data, not a defect."""
    clean, bad = check([item(views=0)])
    assert bad == []
    assert clean[0].views == 0


def test_views_too_large_for_the_column_is_quarantined():
    """`pageviews.views` is a BIGINT. Without a ceiling the row passes every other
    rule, becomes a CleanRow, and fails inside the driver instead - which aborts
    the load for every venue in the run, and does it again every night because no
    watermark advanced."""
    clean, bad = check([item(views=2**63)])
    assert clean == []
    assert bad[0].rule == "views_within_bigint"


def test_the_largest_value_the_column_holds_is_clean():
    clean, bad = check([item(views=2**63 - 1)])
    assert bad == []
    assert clean[0].views == 2**63 - 1


def test_a_title_differing_only_by_unicode_form_matches():
    """`ū` is one codepoint in NFC and two in NFD; both render identically. macOS
    text entry and some spreadsheets produce NFD and the API answers NFC, so
    without normalising, a venue like Tūrangi quarantines every row for ever."""
    nfd = unicodedata.normalize("NFD", "Tūrangi")
    nfc = unicodedata.normalize("NFC", "Tūrangi")
    assert nfd != nfc, "the two forms really are different strings"

    clean, bad = quality.check_window(
        [item(article=nfd)], venue_id="turangi", article=nfc, start=START, end=END, today=TODAY
    )
    assert bad == [], "the same title in two encodings is the same title"
    assert clean[0].views == 120


def test_a_non_ascii_title_mismatch_shows_the_escaped_form():
    """`got 'Tūrangi', asked for 'Tūrangi'` is a true and useless quarantine row."""
    _, bad = quality.check_window(
        [item(article="Tūrangi")], venue_id="v", article="Taupō", start=START, end=END, today=TODAY
    )
    assert bad[0].rule == "article_matches_request"
    assert "\\u016b" in bad[0].detail, bad[0].detail


def test_a_digit_timestamp_of_the_wrong_length_is_rejected():
    """Truncating it would turn schema drift into a plausible-looking date."""
    with pytest.raises(ValueError):
        quality.parse_timestamp("2026010100999")
    with pytest.raises(ValueError):
        quality.parse_timestamp("202601")


def test_the_first_rule_reported_is_the_root_cause():
    """A row outside the window *and* in the future is out of window first."""
    clean, bad = quality.check_window(
        [item(timestamp="2026061500")],
        venue_id="v",
        article=ARTICLE,
        start=START,
        end=END,
        today=date(2026, 2, 1),
    )
    assert clean == []
    assert bad[0].rule == "date_in_requested_window"


def test_a_rate_exactly_on_the_threshold_passes():
    """The boundary the gate is written to allow: `>` not `>=`."""
    assert quality.enforce_gate(fetched=100, quarantined=5, max_reject_rate=0.05) == 0.05


def test_a_non_string_article_is_quarantined_not_raised():
    """`_parse` checks the field is present, never its type, so a drifted null or
    number reaches the comparison. Raising there would abort the load for every
    venue in the run, and again every night, since no watermark advances."""
    for wrong in (None, 123, ["Milford_Sound"]):
        clean, bad = check([item(article=wrong)])
        assert clean == [], wrong
        assert bad[0].rule == "article_matches_request", wrong


@pytest.mark.parametrize(
    "timestamp,ok",
    [
        ("2026031000", True),  # the only daily shape
        ("2026031012", False),  # an hour, not a day
        ("2026031001", False),
        ("202603100", False),  # nine digits
        ("20260310AB", False),
        ("", False),
    ],
)
def test_only_a_daily_timestamp_is_accepted(timestamp, ok):
    """The trailing 00 is the "daily" in a daily timestamp.

    Checking only for ten digits let an hourly stamp through: 2026031012 parsed
    as 10 March and loaded as that day's total when it is one hour of it.
    `one_row_per_date` catches a *second* hour for the same day, but the first
    arrives looking exactly like a legitimate daily figure.

    A non-string timestamp is deliberately not tested here: parse_timestamp
    coerces with str(), and the type is enforced one layer earlier, by
    client._parse, so it never reaches this function as an int. See
    test_a_response_answering_a_different_question_is_drift_not_a_clean_row.
    """
    if ok:
        assert quality.parse_timestamp(timestamp) == date(2026, 3, 10)
    else:
        with pytest.raises(ValueError, match="YYYYMMDD00"):
            quality.parse_timestamp(timestamp)
