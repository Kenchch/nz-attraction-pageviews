"""How long a standing venue failure costs the healthy venues.

The per-venue hold is tested elsewhere for a single night. This asks the
question a night-by-night test answers and a one-shot test cannot: with a
persistent upstream problem at some venues, what happens to the *other* venues
over a month, and what does the run log say each night?
"""

from datetime import date, timedelta

import pytest

from nz_attraction_pageviews import ingest, quality


@pytest.fixture
def con(tmp_path):
    connection = ingest.connect(tmp_path / "gate-duration.duckdb")
    yield connection
    connection.close()


TODAY = date(2026, 3, 1)
EIGHT = [ingest.Venue(f"v{i}", f"Venue {i}", "R", f"Article_{i}") for i in range(8)]
DRIFTED = {f"v{i}" for i in range(5)}  # a majority


def _daily(article, start, end, views=10):
    return [
        {
            "project": "en.wikipedia",
            "article": article,
            "granularity": "daily",
            "timestamp": f"{d:%Y%m%d}00",
            "access": "all-access",
            "agent": "user",
            "views": views,
        }
        for d in (start + timedelta(days=n) for n in range((end - start).days + 1))
    ]


def _feed(state):
    """Five of eight venues answer with a rejectable value, permanently."""

    def fetch(article, start, end):
        venue_id = f"v{article.rsplit('_', 1)[1]}"
        drifted = state["drifted"] and venue_id in DRIFTED
        return _daily(article, start, end, views=-1 if drifted else 10)

    return fetch


def _nightly(con, nights=25):
    """Run one ingest per night and record what each night did."""
    state = {"drifted": False}
    ingest.run(con, EIGHT, today=TODAY, backfill_days=10, chunk_days=30, fetch=_feed(state))

    state["drifted"] = True
    log = []
    for night in range(1, nights + 1):
        healthy_before = con.execute(
            "SELECT count(*) FROM pageviews WHERE venue_id NOT IN ('v0','v1','v2','v3','v4')"
        ).fetchone()[0]
        try:
            summary = ingest.run(
                con,
                EIGHT,
                today=TODAY + timedelta(days=night),
                backfill_days=10,
                chunk_days=30,
                fetch=_feed(state),
            )
            status = summary.status
        except quality.QualityGateFailed:
            status = "raised"
        healthy_after = con.execute(
            "SELECT count(*) FROM pageviews WHERE venue_id NOT IN ('v0','v1','v2','v3','v4')"
        ).fetchone()[0]
        log.append((night, status, healthy_after - healthy_before))
    return log


def test_healthy_venues_keep_loading_while_a_majority_is_held(con):
    """The point of holding a venue rather than failing the run.

    Five of eight venues drift permanently. The other three are answering
    correctly every night, and their rows must reach the warehouse every night:
    a run that refuses the whole extract spends the healthy venues to report the
    broken ones, and the days it skips are days it will not come back for once
    they fall past max_lookback_days.
    """
    log = _nightly(con)

    silent_nights = [n for n, _, loaded in log if loaded == 0]
    assert not silent_nights, (
        f"healthy venues loaded nothing on nights {silent_nights}; log={log[:6]}"
    )


def test_a_standing_failure_is_reported_every_night_not_just_the_first(con):
    """A persistent problem must not read as "ok" once it stops being novel.

    The gate fires on newly rejected days, which is what stops one bad venue
    holding the pipeline for ever. The cost of that is that a venue still broken
    on night 2 has nothing *new* wrong with it, so the run can go green while
    five venues are stuck. The status has to carry it.
    """
    log = _nightly(con)

    reported = [status for _, status, _ in log]
    assert set(reported) <= {"degraded", "raised"}, (
        f"a night reported a status that hides five held venues: {reported[:8]}"
    )
    assert "ok" not in reported, f"a run went green with five venues held: {reported[:8]}"


def test_the_held_venues_are_named_in_the_run_log(con):
    """ "Something was held" is not actionable; which venue is."""
    state = {"drifted": False}
    ingest.run(con, EIGHT, today=TODAY, backfill_days=10, chunk_days=30, fetch=_feed(state))
    state["drifted"] = True

    try:
        summary = ingest.run(
            con,
            EIGHT,
            today=TODAY + timedelta(days=1),
            backfill_days=10,
            chunk_days=30,
            fetch=_feed(state),
        )
        note = summary.note
    except quality.QualityGateFailed as exc:
        note = str(exc)

    named = [v for v in sorted(DRIFTED) if v in note]
    assert len(named) == len(DRIFTED), f"only {named} named in: {note!r}"


def test_a_held_venue_loads_nothing_of_its_own(con):
    """Holding has to mean holding: no partial rows from a venue over the ceiling."""
    log = _nightly(con, nights=3)
    assert log, "no nights ran"

    drifted_rows = con.execute(
        "SELECT count(*) FROM pageviews WHERE venue_id IN ('v0','v1','v2','v3','v4') "
        "AND view_date > ?",
        [TODAY],
    ).fetchone()[0]
    assert drifted_rows == 0, "a held venue wrote rows for the days it was held"
