"""Resolving quarantined rejections: notes, acceptance, reopening, and the rows
a `timestamp_parses` failure leaves with no day."""

from datetime import date, timedelta

import pytest
from helpers import TODAY, VENUES, Recorder, row

from nz_attraction_pageviews import ingest, quality
from nz_attraction_pageviews.__main__ import EXIT_CONFIG, EXIT_NOTHING_MATCHED, main

DAY = date(2026, 2, 1)
END = TODAY - timedelta(days=ingest.PUBLICATION_LAG_DAYS)


@pytest.fixture
def con(tmp_path):
    connection = ingest.connect(tmp_path / "warehouse.duckdb")
    yield connection
    connection.close()


def negative(day=DAY, venue="v"):
    return quality.BadRow(venue, "Article", day, "views_non_negative", "negative", "{}")


def state(con):
    return con.execute(
        "SELECT resolved_at IS NOT NULL, resolution, note FROM quarantine"
    ).fetchall()


def test_repeated_rejects_are_deduplicated_even_with_null_dates(con):
    row = quality.BadRow("v", "Article", None, "timestamp_parses", "invalid", "{}", DAY, DAY)
    ingest._store_rejects(con, "one", [row, row])
    ingest._store_rejects(con, "two", [row])
    assert con.execute("SELECT count(*) FROM quarantine").fetchone()[0] == 1


def test_a_dateless_reject_in_a_new_window_is_a_new_rejection(con):
    """Identified by its window, so accepting one does not accept every future
    garbage row the venue ever sends."""
    first = quality.BadRow("v", "A", None, "timestamp_parses", "x", "{}", DAY, DAY)
    later = quality.BadRow("v", "A", None, "timestamp_parses", "x", "{}", DAY + timedelta(1), DAY)
    ingest._store_rejects(con, "one", [first, later])
    assert con.execute("SELECT count(*) FROM quarantine").fetchone()[0] == 2


def test_a_note_releases_nothing_and_survives_the_rejection_recurring(con):
    ingest._store_rejects(con, "one", [negative()])
    assert ingest.resolve(con, "v", DAY, note="Emailed Wikimedia, ticket T12345") == 1

    assert state(con) == [(False, None, "Emailed Wikimedia, ticket T12345")]
    assert ingest.accepted_days(con, "v") == set()

    ingest._store_rejects(con, "two", [negative()])  # the next night asks again
    assert state(con) == [(False, None, "Emailed Wikimedia, ticket T12345")], (
        "the operator's note was wiped by the rejection recurring"
    )


def test_a_day_can_be_accepted_after_it_was_annotated(con):
    """Annotating set resolved_at, and accept matched only rows with no
    resolved_at: 0 accepted, exit 1, and a message saying the watermark may now
    step over the day."""
    ingest._store_rejects(con, "one", [negative()])
    ingest.resolve(con, "v", DAY, note="looked at it")
    assert ingest.resolve(con, "v", DAY, accept=True) == 1
    assert ingest.accepted_days(con, "v") == {DAY}


def test_accepting_keeps_the_reason_given(con):
    ingest._store_rejects(con, "one", [negative()])
    ingest.resolve(con, "v", DAY, note="Upstream confirmed the day is lost", accept=True)
    assert state(con) == [(True, ingest.ACCEPTED, "Upstream confirmed the day is lost")]


def test_an_accepted_day_stays_accepted_when_it_recurs(con):
    ingest._store_rejects(con, "one", [negative()])
    ingest.resolve(con, "v", DAY, accept=True)
    ingest._store_rejects(con, "two", [negative()])
    assert state(con)[0][1] == ingest.ACCEPTED


def test_a_superseded_day_that_goes_bad_again_is_reopened(con):
    ingest._store_rejects(con, "one", [negative()])
    con.execute("UPDATE quarantine SET resolution = ?, resolved_at = now()", [ingest.SUPERSEDED])
    ingest._store_rejects(con, "two", [negative()])
    assert state(con) == [(False, None, None)]


def test_a_range_and_all_resolve_many_days_at_once(con):
    days = [DAY + timedelta(days=n) for n in range(5)]
    ingest._store_rejects(con, "one", [negative(d) for d in days])
    ingest._store_rejects(con, "one", [negative(DAY, venue="other")])

    assert ingest.resolve(con, "v", (days[1], days[3]), accept=True) == 3
    assert ingest.resolve(con, "v", ingest.ALL_DAYS, accept=True) == 2, "only the open ones"
    assert ingest.accepted_days(con, "other") == set(), "another venue is untouched"


def test_the_cli_says_so_when_nothing_matched(tmp_path, capsys):
    db = tmp_path / "w.duckdb"
    con = ingest.connect(db)
    ingest._store_rejects(con, "one", [negative()])
    con.close()

    code = main(["resolve", "v", "2026-02-02", "--accept", "--db", str(db)])

    out, err = capsys.readouterr()
    assert code == EXIT_NOTHING_MATCHED
    assert "step over" not in out, "claimed a release that did not happen"
    assert "nothing changed" in err


def test_the_cli_accepts_a_range_with_its_reason(tmp_path, capsys):
    db = tmp_path / "w.duckdb"
    con = ingest.connect(db)
    ingest._store_rejects(con, "one", [negative(DAY), negative(DAY + timedelta(1))])
    con.close()

    code = main(
        ["resolve", "v", "2026-02-01..2026-02-02", "--accept", "--note", "gone", "--db", str(db)]
    )

    assert code == 0
    assert "2 rejection(s) accepted" in capsys.readouterr().out
    con = ingest.connect(db)
    try:
        assert state(con) == [(True, ingest.ACCEPTED, "gone")] * 2
    finally:
        con.close()


def test_the_cli_will_not_create_a_warehouse_to_resolve_in(tmp_path):
    missing = tmp_path / "typo.duckdb"
    assert main(["resolve", "v", "2026-02-01", "--db", str(missing)]) == EXIT_CONFIG
    assert not missing.exists()


class GarbageInLastWindow(Recorder):
    """Every day, plus one row whose timestamp is not a date, in the window
    that ends at `END`."""

    def __call__(self, article, start, end):
        rows = super().__call__(article, start, end)
        if article == "Milford_Sound" and end == END:
            rows.append({**row(article, start), "timestamp": "2026-02-27T00"})
        return rows


def run(con, fetch):
    return ingest.run(
        con,
        VENUES,
        today=TODAY,
        backfill_days=10,
        chunk_days=5,
        max_reject_rate=1.0,
        fetch=fetch,
    )


def test_a_dateless_reject_holds_the_watermark_at_its_window_not_the_whole_venue(con):
    """It used to freeze the venue: the watermark never moved again, the range it
    re-asked grew every night until the lookback cap gave the days up, and the
    run said `ok` throughout."""
    summary = run(con, GarbageInLastWindow())

    last_window_start = END - timedelta(days=4)
    assert ingest.get_watermark(con, "milford-sound") == last_window_start - timedelta(days=1)
    assert summary.status == "degraded", "a dateless reject is unresolved too"
    assert "milford-sound" in summary.note


def test_accepting_a_dateless_reject_lets_the_venue_move(con, tmp_path):
    run(con, GarbageInLastWindow())
    assert ingest.resolve(con, "milford-sound", None, accept=True) == 1

    summary = run(con, GarbageInLastWindow())

    assert ingest.get_watermark(con, "milford-sound") == END
    assert summary.status == "ok", summary.note


def test_the_cli_accepts_dateless_rejects_with_null(tmp_path, capsys):
    db = tmp_path / "w.duckdb"
    con = ingest.connect(db)
    run(con, GarbageInLastWindow())
    con.close()

    assert main(["resolve", "milford-sound", "null", "--accept", "--db", str(db)]) == 0
    assert "1 rejection(s) accepted" in capsys.readouterr().out
