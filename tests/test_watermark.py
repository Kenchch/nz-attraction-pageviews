"""The watermark rules on their own: no warehouse, no fetcher, one case at a time."""

from datetime import date, timedelta

import pytest

from nz_attraction_pageviews import quality
from nz_attraction_pageviews.watermark import is_accepted, start_date, venue_frontier

START, END = date(2026, 2, 1), date(2026, 2, 10)
TRUSTED = date(2026, 2, 5)


def clean(*days):
    return [quality.CleanRow("v", "A", d, 1) for d in days]


def bad(day, rule="views_non_negative", window_start=START):
    return quality.BadRow("v", "A", day, rule, "", "{}", window_start, END)


def day(n):
    return START + timedelta(days=n)


@pytest.mark.parametrize(
    "watermark,article,has_rows,expected",
    [
        (None, None, False, date(2026, 2, 1)),  # first sight: backfill
        (date(2026, 2, 5), "A", True, date(2026, 2, 6)),  # resume
        (date(2026, 2, 5), None, True, date(2026, 2, 6)),  # older warehouse, no title yet
        (date(2026, 2, 5), "A", False, date(2026, 2, 1)),  # never produced: backfill again
        (date(2026, 2, 5), "Old", True, date(2026, 2, 1)),  # retitled: backfill again
        (date(2026, 1, 1), "Old", True, date(2026, 1, 2)),  # ...but never skip forward
    ],
)
def test_start_date(watermark, article, has_rows, expected):
    assert start_date(watermark, article, has_rows, "A", END, backfill_days=10) == expected


def test_everything_loaded_advances_to_the_end():
    assert venue_frontier(START, END, clean(*map(day, range(10))), [], TRUSTED) == END


def test_a_bad_day_stops_the_watermark_the_day_before():
    assert venue_frontier(START, END, clean(day(0)), [bad(day(4))], TRUSTED) == day(3)


def test_an_accepted_bad_day_does_not():
    rows = clean(*map(day, range(10)))
    assert venue_frontier(START, END, rows, [bad(day(4))], TRUSTED, accepted={day(4)}) == END


def test_a_bad_day_outside_the_range_does_not():
    rows = clean(*map(day, range(10)))
    assert venue_frontier(START, END, rows, [bad(START - timedelta(days=30))], TRUSTED) == END


def test_a_dateless_reject_stops_before_its_window():
    rows = clean(*map(day, range(10)))
    dateless = bad(None, "timestamp_parses", window_start=day(6))
    assert venue_frontier(START, END, rows, [dateless], TRUSTED) == day(5)
    accepted = {("timestamp_parses", day(6))}
    assert venue_frontier(START, END, rows, [dateless], TRUSTED, accepted_null=accepted) == END


def test_absent_days_are_trusted_only_up_to_the_trust_line():
    assert venue_frontier(START, END, clean(day(1)), [], TRUSTED) == TRUSTED


def test_with_no_evidence_of_publication_nothing_moves():
    assert venue_frontier(START, END, [], [], None) is None


def test_a_frontier_before_the_start_is_no_frontier():
    assert venue_frontier(START, END, [], [bad(START)], TRUSTED) is None


def test_is_accepted_matches_dated_rows_by_day_and_dateless_by_window():
    assert is_accepted(bad(day(2)), {day(2)}, set())
    assert not is_accepted(bad(day(2)), set(), set())
    dateless = bad(None, "timestamp_parses", window_start=day(6))
    assert is_accepted(dateless, set(), {("timestamp_parses", day(6))})
    assert not is_accepted(dateless, set(), {("timestamp_parses", day(0))})
