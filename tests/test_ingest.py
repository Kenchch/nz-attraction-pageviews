"""Ingest tests. Fetching is stubbed, so these run offline and deterministically."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from nz_attraction_pageviews import client, ingest, quality

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


class StraysBeforeTheWindow(Recorder):
    """Every requested day, plus one row for a day a year before the window."""

    def __call__(self, article, start, end):
        rows = super().__call__(article, start, end)
        stray = start - timedelta(days=365)
        rows.append(
            {
                "project": "en.wikipedia",
                "article": article,
                "granularity": "daily",
                "timestamp": f"{stray:%Y%m%d}00",
                "access": "all-access",
                "agent": "user",
                "views": 100,
            }
        )
        return rows


def test_a_stray_day_before_the_window_does_not_freeze_the_watermark(con):
    """The stray row is quarantined, but it is not a day this run promised
    anything about - the watermark passed it runs ago. Holding the watermark for
    it would re-fetch the whole range every night and never recover anything."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)

    summary = ingest.run(
        con,
        VENUES,
        today=TODAY,
        backfill_days=10,
        chunk_days=30,
        max_reject_rate=1.0,
        fetch=StraysBeforeTheWindow(),
    )

    assert summary.rows_quarantined == 2, "the stray day is still recorded, not ignored"
    assert ingest.get_watermark(con, "milford-sound") == end


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


def test_a_hole_behind_a_day_that_arrived_is_trusted_as_quiet(con):
    """The API omits days with no traffic rather than sending a zero, so a hole is
    ambiguous. A later day arriving settles it: publication runs in date order, so
    the earlier day was published and its absence can only mean nobody looked."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    hole = end - timedelta(days=5)

    ingest.run(
        con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=Laggy(end, skip=[hole])
    )

    assert ingest.get_watermark(con, "milford-sound") == end, (
        "days after the hole arrived, so the hole is settled and must not stall the venue"
    )
    assert (
        con.execute(
            "SELECT count(*) FROM pageviews WHERE venue_id = 'milford-sound' AND view_date = ?",
            [hole],
        ).fetchone()[0]
        == 0
    )


class QuietFor:
    """Full data for every article except the named one, which returns nothing.

    Separates the two things that can settle an absent day: evidence that
    publication reached it (another venue's rows, since publication is a property
    of the upstream and not of one article) and the day's own age.
    """

    def __init__(self, quiet_article):
        self.quiet_article = quiet_article

    def __call__(self, article, start, end):
        if article == self.quiet_article:
            return []
        return Recorder()(article, start, end)


def test_an_absent_day_is_trusted_once_it_is_old_enough(con):
    """Nothing of this venue's own arrives to settle its absent days, so only age
    can - and the rest of the run proves publication is keeping up."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    trust_line = TODAY - timedelta(days=ingest.TRUST_LAG_DAYS)

    ingest.run(
        con, VENUES, today=TODAY, backfill_days=20, chunk_days=30, fetch=QuietFor("Milford_Sound")
    )

    assert ingest.get_watermark(con, "milford-sound") == trust_line, (
        "absent days older than the trust lag are quiet; newer ones stay unsettled"
    )
    assert trust_line < end


def test_the_trust_lag_cannot_step_over_days_nothing_published(con):
    """The calendar trust line alone was a bet that the lag never runs longer than
    TRUST_LAG_DAYS. A longer stall walked the watermark over days no venue had
    ever seen a row for, and the run still said `ok`. The line is now also bounded
    by the newest day the run actually observed, anywhere."""
    published_to = TODAY - timedelta(days=12)  # a stall well past the trust lag
    trust_line = TODAY - timedelta(days=ingest.TRUST_LAG_DAYS)

    summary = ingest.run(
        con, VENUES, today=TODAY, backfill_days=30, chunk_days=30, fetch=Laggy(published_to)
    )

    assert summary.status == "ok"
    assert published_to < trust_line, "the stall has to outrun the trust lag for this to bite"
    assert ingest.get_watermark(con, "milford-sound") == published_to, (
        "nothing published past here, so nothing may be settled past here"
    )


def test_a_long_stall_costs_a_re_request_not_the_days(con):
    """The point of holding: when the API catches up, the days are still asked for."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    published_to = TODAY - timedelta(days=12)

    ingest.run(con, VENUES, today=TODAY, backfill_days=30, chunk_days=30, fetch=Laggy(published_to))
    ingest.run(con, VENUES, today=TODAY, backfill_days=30, chunk_days=30, fetch=Recorder())

    recovered = con.execute(
        "SELECT count(*) FROM pageviews WHERE venue_id = 'milford-sound' AND view_date > ?",
        [published_to],
    ).fetchone()[0]
    assert recovered == (end - published_to).days, "every stalled day arrives once it publishes"
    assert ingest.get_watermark(con, "milford-sound") == end


def test_a_quiet_venue_is_not_dragged_backwards(con):
    """A venue already current past the trust line must not be pulled back to it."""
    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=Recorder())
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    assert ingest.get_watermark(con, "milford-sound") == end

    def nothing(article, start, end):
        return []

    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=nothing)

    assert ingest.get_watermark(con, "milford-sound") == end, "the watermark must not go backwards"


def test_a_sparse_venue_still_makes_progress(con):
    """Measured against the live API, `Te_Rerenga_Wairua` has 74 absent days in 90.
    Treating every absent day as unsettled would strand a venue like that."""
    anchor = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)

    def busy(day):
        # A rule rather than a frozen set: a set anchored on the first run's end
        # would also stop producing rows after it, which is a publication stall,
        # not a sparse venue - and the watermark is right to hold for that.
        return (anchor - day).days % 3 == 0

    def sparse(article, start, end):
        rows, cursor = [], start
        while cursor <= end:
            if busy(cursor):
                rows.append(
                    {
                        "project": "en.wikipedia",
                        "article": article,
                        "granularity": "daily",
                        "timestamp": f"{cursor:%Y%m%d}00",
                        "access": "all-access",
                        "agent": "user",
                        "views": 3,
                    }
                )
            cursor += timedelta(days=1)
        return rows

    ingest.run(con, VENUES, today=TODAY, backfill_days=90, chunk_days=30, fetch=sparse)
    first = ingest.get_watermark(con, "milford-sound")

    later = TODAY + timedelta(days=7)
    ingest.run(con, VENUES, today=later, backfill_days=90, chunk_days=30, fetch=sparse)

    assert first is not None
    assert ingest.get_watermark(con, "milford-sound") > first, "a quiet venue must still advance"


def test_empty_window_advances_the_watermark_as_far_as_the_trust_line(con):
    """A genuinely quiet venue must not be re-requested forever - but an empty
    window proves nothing about days too recent to have published yet, so it
    advances to the trust line rather than all the way to the end."""

    def nothing(article, start, end):
        return []

    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=nothing)

    assert ingest.get_watermark(con, "milford-sound") == TODAY - timedelta(
        days=ingest.TRUST_LAG_DAYS
    )


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


def test_a_failed_run_keeps_the_note_about_days_it_abandoned(con):
    """The run that both gave up on days and then failed is the one whose note is
    worth most. Replacing it with the exception lost the half that is not in the
    traceback."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    con.execute(
        "INSERT OR REPLACE INTO watermark VALUES (?, ?, current_timestamp)",
        ["milford-sound", end - timedelta(days=400)],
    )

    def poisoned(article, start, end):
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
                    "views": -1,
                }
            )
            cursor += timedelta(days=1)
        return rows

    with pytest.raises(quality.QualityGateFailed):
        ingest.run(con, VENUES, today=TODAY, chunk_days=400, max_lookback_days=180, fetch=poisoned)

    note = con.execute("SELECT note FROM run_log").fetchone()[0]
    assert "gave up on" in note, "the abandoned days must survive the failure"
    assert "QualityGateFailed" in note, "and so must the reason it failed"


def test_a_failure_partway_through_still_reports_what_was_abandoned(con):
    """The note is composed from `stalled` at the point of failure, so it works
    even when nothing reached the gate."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    con.execute(
        "INSERT OR REPLACE INTO watermark VALUES (?, ?, current_timestamp)",
        ["milford-sound", end - timedelta(days=400)],
    )

    def exploding(article, start, end):
        raise client.ApiError("HTTP 403")

    with pytest.raises(client.ApiError):
        ingest.run(con, VENUES, today=TODAY, chunk_days=400, max_lookback_days=180, fetch=exploding)

    note = con.execute("SELECT note FROM run_log").fetchone()[0]
    assert "gave up on" in note
    assert "ApiError" in note


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


def write_csv(tmp_path, text):
    path = tmp_path / "venues.csv"
    path.write_text(text, encoding="utf-8")
    return path


def test_read_venues_survives_the_bom_excel_writes(tmp_path):
    """A BOM rides along inside the first column name, so `venue_id` goes missing."""
    path = tmp_path / "venues.csv"
    path.write_bytes(b"\xef\xbb\xbfvenue_id,venue_name,region,wiki_article\na,A,R,Article_A\n")
    assert ingest.read_venues(path)[0].venue_id == "a"


def test_read_venues_tolerates_an_extra_column(tmp_path):
    path = write_csv(
        tmp_path,
        "venue_id,venue_name,region,wiki_article,notes\na,A,R,Article_A,mine\n",
    )
    assert ingest.read_venues(path)[0].wiki_article == "Article_A"


def test_read_venues_names_the_missing_column(tmp_path):
    path = write_csv(tmp_path, "venue_id,venue_name,region\na,A,R\n")
    with pytest.raises(ValueError, match="wiki_article"):
        ingest.read_venues(path)


def test_read_venues_reports_the_line_of_a_blank_field(tmp_path):
    path = write_csv(
        tmp_path,
        "venue_id,venue_name,region,wiki_article\na,A,R,Article_A\nb,B,R,\n",
    )
    with pytest.raises(ValueError, match="line 3"):
        ingest.read_venues(path)


def test_read_venues_rejects_a_duplicate_venue_id(tmp_path):
    """Two rows sharing a venue_id would silently share a watermark."""
    path = write_csv(
        tmp_path,
        "venue_id,venue_name,region,wiki_article\na,A,R,Article_A\na,B,R,Article_B\n",
    )
    with pytest.raises(ValueError, match="duplicate venue_id"):
        ingest.read_venues(path)


def test_read_venues_parses_the_shipped_csv():
    venues = ingest.read_venues("venues.csv")
    assert len(venues) == 8
    assert any(v.wiki_article == "Sky_Tower_(Auckland)" for v in venues)


def test_a_venue_that_has_never_returned_a_row_is_named_in_the_note(con):
    """The shape a typo in venues.csv makes: the article does not exist, every
    width and slice 404s, the window verifies as genuinely empty and the run
    reports `ok` having quietly retired the whole backfill. Nothing else in the
    summary tells that apart from a venue nobody reads."""

    def nothing_for_milford(article, start, end):
        if article == "Milford_Sound":
            return []
        return Recorder()(article, start, end)

    summary = ingest.run(
        con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=nothing_for_milford
    )

    assert summary.status == "ok"
    assert "milford-sound" in summary.note
    assert "Milford_Sound" in summary.note, "name the article, since that is what is wrong"
    assert "te-papa" not in summary.note


def test_a_venue_with_history_that_goes_quiet_is_not_flagged(con):
    """Only 'never produced a row' is a config smell; a quiet week is not."""
    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=Recorder())

    def nothing(article, start, end):
        return []

    later = TODAY + timedelta(days=3)
    summary = ingest.run(con, VENUES, today=later, backfill_days=10, chunk_days=30, fetch=nothing)
    assert summary.note == ""


def test_an_oversized_views_value_is_quarantined_without_taking_the_run_down(con):
    """It used to pass every rule, then fail inside the driver - aborting the load
    for every venue, every night, because no watermark advanced."""

    def one_absurd_day(article, start, end):
        rows = Recorder()(article, start, end)
        if article == "Milford_Sound":
            rows[0] = {**rows[0], "views": 2**64}
        return rows

    summary = ingest.run(
        con,
        VENUES,
        today=TODAY,
        backfill_days=5,
        chunk_days=30,
        max_reject_rate=1.0,
        fetch=one_absurd_day,
    )

    assert summary.status == "ok"
    assert summary.rows_quarantined == 1
    assert con.execute("SELECT rule FROM quarantine").fetchone()[0] == "views_within_bigint"
    assert (
        con.execute("SELECT count(*) FROM pageviews WHERE venue_id = 'te-papa'").fetchone()[0] == 5
    ), "the healthy venue is unaffected"


def test_the_run_log_lands_in_the_same_transaction_as_the_data(con, monkeypatch):
    """Written afterwards it could be lost while the data survived, leaving rows
    no run claims to have loaded."""
    original = ingest._write_run_log

    def explode(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("crash between the data and its log")

    monkeypatch.setattr(ingest, "_write_run_log", explode)

    with pytest.raises(RuntimeError):
        ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=Recorder())

    assert con.execute("SELECT count(*) FROM pageviews").fetchone()[0] == 0, (
        "the data must roll back with the log that describes it"
    )
    assert con.execute("SELECT count(*) FROM watermark").fetchone()[0] == 0


def test_repeated_column_in_the_header_is_rejected(tmp_path):
    """csv.DictReader keeps the last value, so a duplicated venue_id column files
    every row under the wrong venue - silently."""
    path = tmp_path / "v.csv"
    path.write_text(
        "venue_id,venue_name,region,wiki_article,venue_id\nreal,A,R,Art,other\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="repeated column"):
        ingest.read_venues(path)


def test_a_title_in_a_different_unicode_form_is_normalised_on_read(tmp_path):
    import unicodedata

    path = tmp_path / "v.csv"
    nfd = unicodedata.normalize("NFD", "Tūrangi")
    path.write_text(
        f"venue_id,venue_name,region,wiki_article\nt,Turangi,Waikato,{nfd}\n", encoding="utf-8"
    )
    venue = ingest.read_venues(path)[0]
    assert venue.wiki_article == unicodedata.normalize("NFC", "Tūrangi")


def test_plan_windows_rejects_a_chunk_size_that_would_not_advance():
    """chunk_days=0 makes the cursor stand still - an infinite loop, not an error."""
    with pytest.raises(ValueError):
        ingest.plan_windows(date(2026, 1, 1), date(2026, 1, 10), 0)
    with pytest.raises(ValueError):
        ingest.plan_windows(date(2026, 1, 1), date(2026, 1, 10), -1)


def test_watermark_stops_before_the_earliest_bad_day_of_several(con):
    """With one bad day 'first' and 'last' are the same day, so min/max cannot be
    told apart. Two bad days is what pins it."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    first_day = end - timedelta(days=9)
    earlier, later = first_day + timedelta(days=3), first_day + timedelta(days=7)

    ingest.run(
        con,
        VENUES,
        today=TODAY,
        backfill_days=10,
        chunk_days=30,
        max_reject_rate=1.0,
        fetch=Poisoner("Milford_Sound", [earlier, later]),
    )

    assert ingest.get_watermark(con, "milford-sound") == earlier - timedelta(days=1)


def test_a_bad_day_on_the_last_day_of_the_window_still_stops_the_watermark(con):
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)

    ingest.run(
        con,
        VENUES,
        today=TODAY,
        backfill_days=10,
        chunk_days=30,
        max_reject_rate=1.0,
        fetch=Poisoner("Milford_Sound", [end]),
    )

    assert ingest.get_watermark(con, "milford-sound") == end - timedelta(days=1)


def test_the_shipped_constants_are_what_the_readme_says():
    """Every test computes its expectation from these, so all of them pass for any
    value. The gate could be relaxed from 5% to 50% with a green suite."""
    assert ingest.PUBLICATION_LAG_DAYS == 2
    assert ingest.TRUST_LAG_DAYS == 7
    assert ingest.DEFAULT_MAX_REJECT_RATE == 0.05
    assert ingest.DEFAULT_CHUNK_DAYS == 30
    assert ingest.DEFAULT_BACKFILL_DAYS == 90
    assert ingest.DEFAULT_MAX_LOOKBACK_DAYS == 180


def test_the_defaults_are_what_a_run_with_no_overrides_uses(con):
    """Nothing else executes them: every other test passes each one explicitly."""
    fetch = Recorder()
    ingest.run(con, VENUES, today=TODAY, fetch=fetch)

    milford = [call for call in fetch.calls if call[0] == "Milford_Sound"]
    assert len(milford) == 3, "90 days in 30 day chunks"
    assert sum((end - start).days + 1 for _, start, end in milford) == 90


def test_a_spreadsheet_csv_with_trailing_commas_still_reads(tmp_path):
    """Excel ends every line with the same stray commas; DictReader names them all
    `''`. Only the four columns actually read can be ambiguous."""
    path = tmp_path / "v.csv"
    path.write_text(
        "venue_id,venue_name,region,wiki_article,,\nte-papa,Te Papa,Wellington,Te_Papa,,\n",
        encoding="utf-8",
    )
    assert ingest.read_venues(path)[0].wiki_article == "Te_Papa"


def test_a_venue_id_is_left_exactly_as_written(tmp_path):
    """It is the key every stored row is written under. Re-spelling it would
    orphan an existing warehouse's watermark and duplicate its history under an
    id that looks identical on screen."""
    import unicodedata

    nfd_id = unicodedata.normalize("NFD", "tūrangi")
    path = tmp_path / "v.csv"
    path.write_text(
        f"venue_id,venue_name,region,wiki_article\n{nfd_id},T,Waikato,Tūrangi\n", encoding="utf-8"
    )

    venue = ingest.read_venues(path)[0]
    assert venue.venue_id == nfd_id, "the id is ours and is never compared with the API"
    assert venue.wiki_article == unicodedata.normalize("NFC", "Tūrangi"), "the title is theirs"


def test_a_venue_falling_behind_is_named_in_the_note(con):
    """Holding is right - it re-requests rather than losing days - but a venue
    that stops advancing should not be something you have to notice yourself."""
    end = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
    ingest.run(con, VENUES, today=TODAY, backfill_days=10, chunk_days=30, fetch=Recorder())

    def nothing_new(article, start, end):
        return []

    later = TODAY + timedelta(days=ingest.TRUST_LAG_DAYS + 4)
    summary = ingest.run(
        con, VENUES, today=later, backfill_days=10, chunk_days=30, fetch=nothing_new
    )

    assert "milford-sound" in summary.note
    assert "days behind" in summary.note
    assert ingest.get_watermark(con, "milford-sound") == end, "and it is holding, not losing"


@pytest.mark.parametrize(
    "value",
    [
        10**1000,  # math.isfinite() converts to float and overflows
        float("nan"),  # every comparison against nan is False: no gate at all
        float("inf"),
        1.5,
        -0.1,
        "0.05",
    ],
)
def test_an_unusable_reject_rate_is_refused(value):
    """nan is the sharp one: it disables the gate outright, so a run that
    rejected everything still reports ok. 10**1000 used to reach math.isfinite()
    and raise OverflowError out of the validator whose job is a clean message."""
    with pytest.raises(ValueError, match="max_reject_rate"):
        ingest.run(None, [], max_reject_rate=value)


@pytest.mark.parametrize(
    "name", ["chunk_days", "backfill_days", "max_lookback_days", "trust_lag_days"]
)
@pytest.mark.parametrize("value", [0, -1, 1.5, True])
def test_an_unusable_interval_is_refused(name, value):
    """backfill_days=0 asks for an empty range and looks like a quiet venue."""
    with pytest.raises(ValueError, match=name):
        ingest.run(None, [], **{name: value})


def test_a_request_that_raises_is_still_counted(con):
    """`requests` was incremented after the fetch returned, so the request that
    raised was never counted.

    The failure path writes this same summary to run_log, which makes the one
    run whose request count matters the one that reads short: a venue that
    failed on its very first window logged `requests 0` while having gone to
    the network. The field answers "how many windows did we ask for", and that
    is decided when we ask.
    """

    def explodes(article, start, end):
        raise client.ApiError("upstream is down")

    with pytest.raises(client.ApiError):
        ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=explodes)

    status, requests = con.execute(
        "SELECT status, requests FROM run_log ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    assert status == "failed"
    assert requests == 1, "the request that raised was not counted"


@pytest.mark.parametrize("cancellation", [KeyboardInterrupt, SystemExit])
def test_cancelling_mid_write_still_rolls_back(con, monkeypatch, cancellation):
    """Ctrl-C is exactly when a half-written transaction is likeliest.

    The cleanup caught Exception, which neither KeyboardInterrupt nor
    SystemExit inherits from, so both went past without a ROLLBACK and left the
    connection open mid-write - with the pageviews rows already inserted and
    the run log not yet written.
    """
    rows_before = con.execute("SELECT count(*) FROM pageviews").fetchone()[0]

    def cancel(*a, **k):
        raise cancellation("user pressed Ctrl-C")

    monkeypatch.setattr(ingest, "_write_run_log", cancel)

    with pytest.raises(cancellation):
        ingest.run(con, VENUES, today=TODAY, backfill_days=5, chunk_days=30, fetch=Recorder())

    # The connection is usable and holds no partial write: a still-open
    # transaction would make this count include the abandoned inserts.
    assert con.execute("SELECT count(*) FROM pageviews").fetchone()[0] == rows_before
    con.execute("BEGIN TRANSACTION")
    con.execute("ROLLBACK")
