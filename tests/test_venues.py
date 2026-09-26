"""venues.csv as it is really edited, and what changing a title does to history."""

from datetime import timedelta

import pytest
from helpers import TODAY, Recorder

from nz_attraction_pageviews import client, ingest

END = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)
HEADER = "venue_id,venue_name,region,wiki_article\n"


@pytest.fixture
def con(tmp_path):
    connection = ingest.connect(tmp_path / "venues.duckdb")
    yield connection
    connection.close()


def write(tmp_path, body):
    path = tmp_path / "venues.csv"
    path.write_text(HEADER + body, encoding="utf-8")
    return path


def test_a_blank_line_at_the_end_is_skipped(tmp_path):
    """What a spreadsheet export leaves behind. It aborted the whole ingest."""
    venues = ingest.read_venues(write(tmp_path, "a,A,R,Article_A\n,,,\n\n"))
    assert [v.venue_id for v in venues] == ["a"]


def test_a_title_written_with_spaces_is_spelt_the_way_the_api_needs(tmp_path):
    venues = ingest.read_venues(write(tmp_path, "sky,Sky Tower,R,sky Tower (Auckland)\n"))
    assert venues[0].wiki_article == "Sky_Tower_(Auckland)"


def test_a_title_with_a_character_no_title_can_have_is_refused(tmp_path):
    with pytest.raises(ValueError, match=r"line 2.*'#'"):
        ingest.read_venues(write(tmp_path, "tp,Te Papa,R,Te_Papa#History\n"))


def test_a_backfill_longer_than_the_lookback_cap_is_refused():
    """It was cut to the cap without a word, and the difference reported as
    days given up, under an `ok`."""
    with pytest.raises(ValueError, match="max_lookback_days"):
        ingest.run(None, [], backfill_days=365, max_lookback_days=180)


def test_changing_a_title_re_fetches_the_backfill_under_the_new_one(con):
    """The watermark recorded coverage of the OLD title. Correcting a redirect to
    the canonical title only changed the rows from then on; everything before
    kept the redirect's much smaller numbers, and nothing said so."""
    redirect = ingest.Venue("te-papa", "Te Papa", "Wellington", "Museum_of_NZ_Te_Papa")
    ingest.run(con, [redirect], today=TODAY, backfill_days=10, chunk_days=30, fetch=Recorder(5))
    assert ingest.get_watermark(con, "te-papa") == END

    canonical = ingest.Venue("te-papa", "Te Papa", "Wellington", "Te_Papa")
    later = TODAY + timedelta(days=1)
    fetch = Recorder(80)
    summary = ingest.run(
        con, [canonical], today=later, backfill_days=10, chunk_days=30, fetch=fetch
    )

    start = fetch.calls[0][1]
    assert start == END + timedelta(days=1) - timedelta(days=9), "the whole backfill window"
    assert "title changed" in summary.note
    by_article = dict(
        con.execute(
            "SELECT article, count(*) FROM pageviews WHERE view_date >= ? GROUP BY 1", [start]
        ).fetchall()
    )
    assert by_article == {"Te_Papa": 10}, "every day in the window is the canonical title's"
    # Older than the window: kept, and the note says whose it still is.
    assert "still for the old title" in summary.note
    row = con.execute("SELECT last_date, article FROM watermark").fetchone()
    assert row == (END + timedelta(days=1), "Te_Papa")

    # And once re-fetched, it resumes normally.
    again = Recorder(80)
    ingest.run(con, [canonical], today=later, backfill_days=10, chunk_days=30, fetch=again)
    assert again.calls == []


def test_an_older_watermark_learns_its_title_without_a_re_fetch(con):
    """A warehouse from before the column has no title on record. That is not a
    change of title, so it is not a reason to re-fetch."""
    venue = ingest.Venue("te-papa", "Te Papa", "Wellington", "Te_Papa")
    ingest.run(con, [venue], today=TODAY, backfill_days=10, chunk_days=30, fetch=Recorder())
    con.execute("UPDATE watermark SET article = NULL")

    fetch = Recorder()
    ingest.run(con, [venue], today=TODAY + timedelta(1), backfill_days=10, fetch=fetch)

    assert [(s, e) for _, s, e in fetch.calls] == [(END + timedelta(1), END + timedelta(1))]
    assert con.execute("SELECT article FROM watermark").fetchone()[0] == "Te_Papa"


def test_the_run_counts_its_http_calls_and_stops_at_its_budget(con):
    """`requests` counts windows; an article that does not exist turns one
    window into dozens of calls, and nothing bounded a run's total."""

    def nothing_there(url):
        return 404, {}, b""

    venues = [ingest.Venue(f"v{i}", "V", "R", f"Gone_{i}") for i in range(3)]
    summary = ingest.run(
        con,
        venues,
        today=TODAY,
        backfill_days=10,
        chunk_days=30,
        max_http_requests=30,
        opener=nothing_there,
    )

    assert summary.http_requests == 30
    assert summary.requests < summary.http_requests
    assert "request budget" in summary.note
    assert summary.status == "degraded"
    logged = con.execute("SELECT http_requests FROM run_log").fetchone()[0]
    assert logged == 30


def test_an_injected_fetcher_is_not_counted(con):
    venue = ingest.Venue("a", "A", "R", "Article_A")
    summary = ingest.run(con, [venue], today=TODAY, backfill_days=3, fetch=Recorder())
    assert summary.http_requests is None


def test_the_budget_error_is_a_venue_failure_not_a_crash():
    assert issubclass(client.RequestBudgetExceeded, client.ApiError)
