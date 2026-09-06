import pytest
from test_ingest import TODAY, VENUES, Recorder

from nz_attraction_pageviews import client, ingest


@pytest.fixture
def con(tmp_path):
    connection = ingest.connect(tmp_path / "failure.duckdb")
    yield connection
    connection.close()


def test_one_http_400_does_not_stop_seven_healthy_venues(con):
    venues = [ingest.Venue(f"v{i}", f"Venue {i}", "NZ", f"Article_{i}") for i in range(8)]
    recorder = Recorder()

    def fetch(article, start, end):
        if article == "Article_0":
            raise client.ApiError("HTTP 400")
        return recorder(article, start, end)

    summary = ingest.run(con, venues, today=TODAY, backfill_days=3, fetch=fetch)
    assert summary.status == "ok"
    assert summary.rows_loaded == 21
    assert summary.requests == 8
    assert "v0: HTTP 400; watermark held" in summary.note
    assert ingest.get_watermark(con, "v0") is None
    assert con.execute("SELECT count(DISTINCT venue_id) FROM pageviews").fetchone()[0] == 7


def test_failed_later_window_discards_earlier_rows_for_that_venue(con):
    recorder = Recorder()
    calls = 0

    def fetch(article, start, end):
        nonlocal calls
        if article == VENUES[0].wiki_article:
            calls += 1
            if calls == 2:
                raise client.ApiError("HTTP 500 after retries")
        return recorder(article, start, end)

    summary = ingest.run(con, VENUES, today=TODAY, backfill_days=4, chunk_days=2, fetch=fetch)
    assert summary.rows_loaded == 4
    assert ingest.get_watermark(con, VENUES[0].venue_id) is None
    assert con.execute("SELECT DISTINCT venue_id FROM pageviews").fetchall() == [
        (VENUES[1].venue_id,)
    ]


def test_schema_drift_still_aborts_all_venues(con):
    recorder = Recorder()

    def fetch(article, start, end):
        if article == VENUES[1].wiki_article:
            raise client.SchemaDriftError("missing items")
        return recorder(article, start, end)

    with pytest.raises(client.SchemaDriftError):
        ingest.run(con, VENUES, today=TODAY, backfill_days=3, fetch=fetch)
    assert con.execute("SELECT count(*) FROM pageviews").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM watermark").fetchone()[0] == 0
