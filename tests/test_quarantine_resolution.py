from datetime import date

from nz_attraction_pageviews import ingest, quality
from nz_attraction_pageviews.__main__ import main


def test_repeated_rejects_are_deduplicated_even_with_null_dates(tmp_path):
    con = ingest.connect(tmp_path / "warehouse.duckdb")
    try:
        row = quality.BadRow("v", "Article", None, "timestamp_parses", "invalid", "{}")
        ingest._store_rejects(con, "one", [row, row])
        ingest._store_rejects(con, "two", [row])
        assert con.execute("SELECT count(*) FROM quarantine").fetchone()[0] == 1
    finally:
        con.close()


def test_resolution_does_not_advance_watermark_and_repeated_failure_reopens(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    day = date(2026, 2, 1)
    row = quality.BadRow("v", "Article", day, "views_non_negative", "negative", "{}")
    con = ingest.connect(db)
    ingest._store_rejects(con, "one", [row])
    con.close()
    assert (
        main(
            [
                "resolve",
                "v",
                day.isoformat(),
                "--db",
                str(db),
                "--resolution",
                "Source owner reviewed the issue",
            ]
        )
        == 0
    )
    con = ingest.connect(db)
    try:
        assert con.execute("SELECT resolved_at IS NOT NULL FROM quarantine").fetchone()[0]
        assert ingest.get_watermark(con, "v") is None
        assert con.execute("SELECT count(*) FROM pageviews").fetchone()[0] == 0
        ingest._store_rejects(con, "two", [row])
        assert con.execute("SELECT resolved_at, resolution FROM quarantine").fetchone() == (
            None,
            None,
        )
        assert con.execute("SELECT count(*) FROM quarantine").fetchone()[0] == 1
    finally:
        con.close()
