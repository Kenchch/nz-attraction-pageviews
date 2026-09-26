"""When a run may say `ok`: only when the warehouse is complete for every venue."""

from datetime import timedelta

import pytest
from helpers import TODAY, VENUES, Recorder

from nz_attraction_pageviews import client, ingest

END = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)


@pytest.fixture
def con(tmp_path):
    connection = ingest.connect(tmp_path / "status.duckdb")
    yield connection
    connection.close()


def run(con, fetch, venues=VENUES, **options):
    options = {"today": TODAY, "backfill_days": 10, "chunk_days": 30, **options}
    return ingest.run(con, venues, fetch=fetch, **options)


class BadOn(Recorder):
    """Every day for every venue, with negative views for Milford on `days`."""

    def __init__(self, *days):
        super().__init__()
        self.days = set(days)

    def __call__(self, article, start, end):
        rows = super().__call__(article, start, end)
        for r in rows:
            if article == "Milford_Sound" and r["timestamp"][:8] in {
                f"{d:%Y%m%d}" for d in self.days
            }:
                r["views"] = -1
        return rows


def test_decide_status_is_ok_only_with_nothing_unresolved():
    empty = {"held": set(), "blocked": set(), "failed": set(), "gave_up": set()}
    assert ingest.decide_status(**empty, never_produced=set()) == ("ok", [])
    for reason in ("held", "blocked", "failed", "gave_up", "never_produced"):
        args = {**empty, "never_produced": set(), reason: {"v"}}
        assert ingest.decide_status(**args) == ("degraded", ["v"]), reason


def test_a_first_rejection_under_the_ceiling_is_degraded_the_same_night(con):
    """The blocked query ran before tonight's quarantine was written, so the night
    a day first went bad said `ok`, and only the next one said `degraded`."""
    summary = run(con, BadOn(END - timedelta(days=3)), max_reject_rate=1.0)
    assert summary.status == "degraded"
    assert "unresolved: milford-sound" in summary.note


def test_a_day_that_later_loads_cleanly_stops_being_unresolved(con):
    """A one-night glitch used to leave every later run `degraded`, after the day
    had loaded and the watermark had passed it. The only way out was `--accept`,
    which says the day is never coming - the opposite of what happened."""
    bad_day = END - timedelta(days=3)
    assert run(con, BadOn(bad_day), max_reject_rate=1.0).status == "degraded"

    summary = run(con, Recorder())

    assert summary.status == "ok", summary.note
    assert ingest.get_watermark(con, "milford-sound") == END
    assert con.execute("SELECT resolution FROM quarantine").fetchone()[0] == ingest.SUPERSEDED


def test_a_venue_removed_from_venues_csv_no_longer_counts(con):
    run(con, BadOn(END - timedelta(days=3)), max_reject_rate=1.0)
    summary = run(con, Recorder(), venues=[VENUES[1]])
    assert summary.status == "ok", summary.note


def test_a_stray_day_behind_the_watermark_does_not_count(con):
    """Recorded, but not a day the watermark promised anything about."""

    class Stray(Recorder):
        def __call__(self, article, start, end):
            rows = super().__call__(article, start, end)
            return rows + [{**rows[0], "timestamp": f"{start - timedelta(days=365):%Y%m%d}00"}]

    summary = run(con, Stray(), max_reject_rate=1.0)
    assert summary.rows_quarantined == 2
    assert summary.status == "ok", summary.note


def test_giving_up_days_to_the_lookback_cap_is_degraded(con):
    """Those days are lost for good; that is not a clean night."""
    con.execute(
        "INSERT INTO watermark (venue_id, last_date, updated_at) VALUES (?, ?, now())",
        ["milford-sound", END - timedelta(days=400)],
    )
    summary = run(con, Recorder())
    assert "gave up on" in summary.note
    assert summary.status == "degraded"


def test_a_venue_that_failed_is_degraded_and_named(con):
    def fetch(article, start, end):
        if article == "Milford_Sound":
            raise client.ApiError("HTTP 403")
        return Recorder()(article, start, end)

    summary = run(con, fetch)
    assert summary.status == "degraded"
    assert "unresolved: milford-sound" in summary.note


def test_one_failed_venue_among_current_ones_is_not_a_failed_run(con):
    """ "Every venue failed" meant "no venue this run fetched successfully", so seven
    venues already up to date and one returning 400 failed the whole run."""
    eight = [ingest.Venue(f"v{i}", f"V{i}", "R", f"Article_{i}") for i in range(8)]
    run(con, Recorder(), venues=eight)

    def only_v0_asks(article, start, end):
        raise client.ApiError("HTTP 400")

    # v0 has to ask again; the other seven are current.
    con.execute("DELETE FROM watermark WHERE venue_id = 'v0'")
    summary = run(con, only_v0_asks, venues=eight)

    assert summary.status == "degraded"
    assert "v0: HTTP 400" in summary.note


def test_every_venue_failing_is_still_a_failed_run(con):
    def down(article, start, end):
        raise client.ApiError("HTTP 503 after retries")

    with pytest.raises(client.ApiError, match="all 2 requested venues failed"):
        run(con, down)
    status, note = con.execute("SELECT status, note FROM run_log").fetchone()
    assert status == "failed"
    assert note.count("HTTP 503") == 2, f"each failure once, not twice: {note}"


def test_rows_from_a_venue_that_failed_are_not_counted_as_fetched(con):
    """They were discarded, and counting them skewed the reject rate."""
    calls = []

    def fetch(article, start, end):
        calls.append(article)
        if article == "Milford_Sound" and calls.count(article) == 2:
            raise client.ApiError("HTTP 500 after retries")
        return Recorder()(article, start, end)

    summary = run(con, fetch, chunk_days=5)
    assert summary.rows_fetched == 10, "only te-papa's ten days"
