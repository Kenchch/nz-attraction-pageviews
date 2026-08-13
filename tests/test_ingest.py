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


class Poisoner:
    """Stub fetcher that returns negative views on a chosen set of days."""

    def __init__(self, article, bad_days):
        self.article = article
        self.bad_days = set(bad_days)

    def __call__(self, article, start, end):
        rows, cursor = [], start
        while cursor <= end:
            bad = article == self.article and cursor in self.bad_days
            rows.append(
                {
                    "project": "en.wikipedia",
                    "article": article,
                    "granularity": "daily",
                    "timestamp": f"{cursor:%Y%m%d}00",
                    "access": "all-access",
                    "agent": "user",
                    "views": -1 if bad else 100,
                }
            )
            cursor += timedelta(days=1)
        return rows


def test_watermark_does_not_step_over_quarantined_days(con):
    """The run-level gate is a rate across every venue, so one venue's bad patch
    can sit under the threshold and pass. The watermark must not advance past it
    anyway, or the quarantine becomes a record of data we permanently lost.
    """
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    first_day = end - timedelta(days=9)
    fetch = Poisoner("Milford_Sound", [first_day])

    summary = ingest.run(
        con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, max_reject_rate=1.0, fetch=fetch
    )

    assert summary.rows_quarantined == 1
    assert ingest.get_watermark(con, "milford-sound") is None, "must not advance over a lost day"
    assert ingest.get_watermark(con, "te-papa") == end, "a clean venue is unaffected"


def test_watermark_stops_at_the_first_bad_day_not_the_last(con):
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    first_day = end - timedelta(days=9)
    bad_day = first_day + timedelta(days=3)
    fetch = Poisoner("Milford_Sound", [bad_day])

    ingest.run(
        con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, max_reject_rate=1.0, fetch=fetch
    )

    assert ingest.get_watermark(con, "milford-sound") == bad_day - timedelta(days=1)


def test_next_run_recovers_days_the_watermark_refused_to_skip(con):
    """The whole point of not advancing: the bad days get another go."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    bad_day = end - timedelta(days=6)

    ingest.run(
        con,
        VENUES,
        today=TODAY,
        backfill_days=10,
        chunk_days=30,
        max_reject_rate=1.0,
        fetch=Poisoner("Milford_Sound", [bad_day]),
    )
    loaded = con.execute(
        "SELECT count(*) FROM pageviews WHERE venue_id = 'milford-sound'"
    ).fetchone()[0]
    assert loaded == 9, "the bad day is missing"

    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=Recorder())

    recovered = con.execute(
        "SELECT count(*) FROM pageviews WHERE venue_id = 'milford-sound'"
    ).fetchone()[0]
    assert recovered == 10, "the day that was quarantined should be picked up next run"
    assert ingest.get_watermark(con, "milford-sound") == end


class Laggy:
    """200, but nothing newer than `published_to`. What a publication lag longer
    than PUBLICATION_LAG_DAYS looks like: no error, no rejects, just fewer days
    than were asked for."""

    def __init__(self, published_to, skip=()):
        self.published_to = published_to
        self.skip = set(skip)

    def __call__(self, article, start, end):
        rows, cursor = [], start
        while cursor <= end:
            if cursor <= self.published_to and cursor not in self.skip:
                rows.append(
                    {
                        "project": "en.wikipedia",
                        "article": article,
                        "granularity": "daily",
                        "timestamp": f"{cursor:%Y%m%d}00",
                        "access": "all-access",
                        "agent": "user",
                        "views": 100,
                    }
                )
            cursor += timedelta(days=1)
        return rows


def test_watermark_stops_where_the_api_stopped_publishing(con):
    """PUBLICATION_LAG_DAYS is an assumption, not a guarantee. When the real lag
    is longer, the tail of the window is absent from a 200 - no rule fires,
    because the acceptance criteria only judge rows that turned up."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    published_to = end - timedelta(days=3)

    summary = ingest.run(
        con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=Laggy(published_to)
    )

    assert summary.rows_quarantined == 0, "nothing is rejected; the days simply never arrive"
    assert ingest.get_watermark(con, "milford-sound") == published_to


def test_days_absent_from_a_200_are_picked_up_once_they_publish(con):
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    published_to = end - timedelta(days=3)

    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=Laggy(published_to))
    before = con.execute(
        "SELECT count(*) FROM pageviews WHERE venue_id = 'milford-sound'"
    ).fetchone()[0]

    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=Recorder())

    after = con.execute(
        "SELECT count(*) FROM pageviews WHERE venue_id = 'milford-sound'"
    ).fetchone()[0]
    assert (before, after) == (7, 10), "the three late days should arrive on the next run"
    assert ingest.get_watermark(con, "milford-sound") == end


def test_a_hole_in_the_middle_of_a_200_stops_the_watermark(con):
    """Not only the tail. A day missing from the middle is just as unaccounted for."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    hole = end - timedelta(days=5)

    ingest.run(
        con,
        VENUES,
        today=TODAY,
        backfill_days=10,
        chunk_days=30,
        fetch=Laggy(end, skip=[hole]),
    )

    assert ingest.get_watermark(con, "milford-sound") == hole - timedelta(days=1)


def test_empty_window_still_advances_the_watermark(con):
    """A genuinely quiet venue must not be re-requested forever."""

    def nothing(article, start, end):
        return []

    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=nothing)
    expected_end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    assert ingest.get_watermark(con, "milford-sound") == expected_end


def test_quarantine_row_carries_the_day_it_belongs_to(con):
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    bad_day = end - timedelta(days=4)

    ingest.run(
        con,
        VENUES,
        today=TODAY,
        backfill_days=10,
        chunk_days=30,
        max_reject_rate=1.0,
        fetch=Poisoner("Milford_Sound", [bad_day]),
    )

    row = con.execute("SELECT venue_id, view_date, rule FROM quarantine").fetchone()
    assert row == ("milford-sound", bad_day, "views_non_negative")


def test_repeated_quarantine_of_one_day_is_countable_as_one_day(con):
    """A stuck day is re-quarantined every run, so the diagnostic query has to
    count distinct days rather than rows or it reads as a worsening problem."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    bad_day = end - timedelta(days=4)
    fetch = Poisoner("Milford_Sound", [bad_day])

    for _ in range(3):
        ingest.run(
            con,
            VENUES,
            today=TODAY,
            backfill_days=10,
            chunk_days=30,
            max_reject_rate=1.0,
            fetch=fetch,
        )

    rows, bad_days = con.execute("""
        SELECT count(*), count(DISTINCT (venue_id, view_date)) FROM quarantine
    """).fetchone()
    assert rows == 3
    assert bad_days == 1, "three runs, but still only one bad day"


def test_failed_gate_still_records_the_rate_that_failed_it(con):
    """reject_rate is the column you reach for when a run went wrong. Deriving it
    from the gate's return value logged 0.0 for exactly the runs that needed it."""

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
        ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=poisoned)

    status, rate = con.execute("SELECT status, reject_rate FROM run_log").fetchone()
    assert status == "failed"
    assert rate == 1.0


def test_lookback_is_capped_and_the_giving_up_is_recorded(con):
    """A venue stuck on a bad day would otherwise ask for a range that grows
    every night, for ever."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    con.execute(
        "INSERT OR REPLACE INTO watermark VALUES (?, ?, current_timestamp)",
        ["milford-sound", end - timedelta(days=400)],
    )

    fetch = Recorder()
    summary = ingest.run(
        con,
        VENUES,
        today=TODAY,
        backfill_days=10,
        chunk_days=400,
        max_lookback_days=180,
        fetch=fetch,
    )

    milford = [call for call in fetch.calls if call[0] == "Milford_Sound"]
    assert (end - milford[0][1]).days + 1 == 180, "must not ask for more than the cap"
    assert "milford-sound" in summary.note
    note = con.execute("SELECT note FROM run_log").fetchone()[0]
    assert "gave up on" in note


def test_warehouse_from_an_older_version_is_migrated(con, tmp_path):
    """CREATE TABLE IF NOT EXISTS leaves an existing table alone, so adding a
    column to the DDL breaks every warehouse built before it without this."""
    import duckdb

    db = tmp_path / "old.duckdb"
    old = duckdb.connect(str(db))
    old.execute("""
        CREATE TABLE quarantine (
            run_id VARCHAR, venue_id VARCHAR, article VARCHAR,
            rule VARCHAR, detail VARCHAR, raw VARCHAR, seen_at TIMESTAMP
        )
    """)
    old.close()

    migrated = ingest.connect(db)
    try:
        assert "view_date" in [c[0] for c in migrated.execute("DESCRIBE quarantine").fetchall()]

        end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
        ingest.run(
            migrated,
            VENUES,
            today=TODAY,
            backfill_days=3,
            chunk_days=30,
            max_reject_rate=1.0,
            fetch=Poisoner("Milford_Sound", [end]),
        )
        assert migrated.execute("SELECT view_date FROM quarantine").fetchone()[0] == end
    finally:
        migrated.close()


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
