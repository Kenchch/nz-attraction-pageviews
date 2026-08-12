"""Ingest tests. Fetching is stubbed, so these run offline and deterministically."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from src import ingest, quality

TODAY = date(2026, 3, 1)
VENUES = [
    ingest.Venue("milford-sound", "Milford Sound", "Fiordland", "Milford_Sound"),
    ingest.Venue("te-papa", "Te Papa", "Wellington", "Museum_of_New_Zealand_Te_Papa_Tongarewa"),
]


@pytest.fixture
def con(tmp_path):
    connection = ingest.connect(tmp_path / "test.duckdb")
    yield connection
    connection.close()


class Recorder:
    """Stub fetcher. Returns one row per day and remembers what it was asked for."""

    def __init__(self, views=50):
        self.views = views
        self.calls = []

    def __call__(self, article, start, end):
        self.calls.append((article, start, end))
        rows, cursor = [], start
        while cursor <= end:
            rows.append(
                {
                    "project": "en.wikipedia",
                    "article": article,
                    "granularity": "daily",
                    "timestamp": f"{cursor:%Y%m%d}00",
                    "access": "all-access",
                    "agent": "user",
                    "views": self.views,
                }
            )
            cursor += timedelta(days=1)
        return rows

    @property
    def days_requested(self):
        return sum((end - start).days + 1 for _, start, end in self.calls)


def test_plan_windows_splits_and_covers_every_day():
    windows = plan = ingest.plan_windows(date(2026, 1, 1), date(2026, 1, 10), chunk_days=4)
    assert plan == [
        (date(2026, 1, 1), date(2026, 1, 4)),
        (date(2026, 1, 5), date(2026, 1, 8)),
        (date(2026, 1, 9), date(2026, 1, 10)),
    ]
    covered = sum((e - s).days + 1 for s, e in windows)
    assert covered == 10, "windows must tile the range with no gap and no overlap"


def test_plan_windows_single_day():
    assert ingest.plan_windows(date(2026, 1, 1), date(2026, 1, 1), 30) == [
        (date(2026, 1, 1), date(2026, 1, 1))
    ]


def test_first_run_backfills_and_sets_watermark(con):
    fetch = Recorder()
    summary = ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=fetch)

    assert summary.status == "ok"
    assert summary.rows_loaded == 20  # 2 venues x 10 days
    assert summary.rows_quarantined == 0

    expected_end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    assert ingest.get_watermark(con, "milford-sound") == expected_end


def test_second_run_only_asks_for_new_days(con):
    first = Recorder()
    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=first)

    later = TODAY + timedelta(days=3)
    second = Recorder()
    summary = ingest.run(con, VENUES, today=later, backfill_days=10, chunk_days=30, fetch=second)

    assert second.days_requested == 6, "3 new days per venue, not the whole 10 day window again"
    assert summary.rows_loaded == 6


def test_rerun_same_day_is_idempotent(con):
    fetch = Recorder()
    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=fetch)
    before = con.execute("SELECT count(*) FROM pageviews").fetchone()[0]

    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=fetch)
    after = con.execute("SELECT count(*) FROM pageviews").fetchone()[0]

    assert before == after == 20, "re-running must not duplicate rows"


def test_revised_views_overwrite_rather_than_duplicate(con):
    """Wikimedia restates figures occasionally. The primary key absorbs that."""
    ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=Recorder(views=50))
    con.execute("DELETE FROM watermark")  # force a re-fetch of the same days
    ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=Recorder(views=77))

    rows = con.execute("SELECT count(*), max(views) FROM pageviews").fetchone()
    assert rows == (10, 77)


def test_chunking_produces_multiple_requests(con):
    fetch = Recorder()
    ingest.run(con, VENUES, today=TODAY, backfill_days=70, chunk_days=30, fetch=fetch)
    assert len(fetch.calls) == 6, "70 days at 30 per window is 3 requests per venue"
    assert fetch.days_requested == 140


def test_bad_rows_are_quarantined_not_dropped(con):
    def fetch(article, start, end):
        return [
            {
                "project": "en.wikipedia",
                "article": article,
                "granularity": "daily",
                "timestamp": f"{start:%Y%m%d}00",
                "access": "all-access",
                "agent": "user",
                "views": -1,
            }
        ]

    summary = ingest.run(
        con, VENUES, today=TODAY, backfill_days=1, chunk_days=30, max_reject_rate=1.0, fetch=fetch
    )
    assert summary.rows_loaded == 0
    assert summary.rows_quarantined == 2
    rule = con.execute("SELECT DISTINCT rule FROM quarantine").fetchone()[0]
    assert rule == "views_non_negative"


def test_gate_failure_loads_nothing_and_is_logged(con):
    """A bad extract must leave the warehouse exactly as it was."""
    ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=Recorder())
    good_rows = con.execute("SELECT count(*) FROM pageviews").fetchone()[0]
    con.execute("DELETE FROM watermark")

    def poisoned(article, start, end):
        return [
            {
                "project": "en.wikipedia",
                "article": article,
                "granularity": "daily",
                "timestamp": f"{start:%Y%m%d}00",
                "access": "all-access",
                "agent": "user",
                "views": -5,
            }
        ]

    with pytest.raises(quality.QualityGateFailed):
        ingest.run(
            con,
            VENUES,
            today=TODAY,
            backfill_days=5,
            chunk_days=30,
            max_reject_rate=0.05,
            fetch=poisoned,
        )

    assert con.execute("SELECT count(*) FROM pageviews").fetchone()[0] == good_rows
    assert con.execute("SELECT count(*) FROM quarantine").fetchone()[0] == 0
    status = con.execute("SELECT status FROM run_log ORDER BY started_at DESC LIMIT 1").fetchone()[
        0
    ]
    assert status == "failed"


def test_run_log_records_every_run(con):
    ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=Recorder())
    row = con.execute(
        "SELECT status, venues, requests, rows_fetched, rows_loaded FROM run_log"
    ).fetchone()
    assert row == ("ok", 2, 2, 10, 10)


def test_venue_already_current_makes_no_request(con):
    fetch = Recorder()
    ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=fetch)
    idle = Recorder()
    summary = ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=idle)
    assert idle.calls == []
    assert summary.requests == 0


def test_read_venues_parses_the_shipped_csv():
    venues = ingest.read_venues("venues.csv")
    assert len(venues) == 8
    assert any(v.wiki_article == "Sky_Tower_(Auckland)" for v in venues)
