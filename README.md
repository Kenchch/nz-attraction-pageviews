# NZ Attraction Pageviews

[![CI](https://github.com/Kenchch/nz-attraction-pageviews/actions/workflows/ci.yml/badge.svg)](https://github.com/Kenchch/nz-attraction-pageviews/actions/workflows/ci.yml)

Incremental daily Wikipedia pageviews for eight New Zealand attractions, stored
in DuckDB. For readers studying reliable API ingestion and recovery from missing data.

## What I built

- Windowed requests with retry/backoff and verification of ambiguous empty responses.
- Row-level acceptance rules with raw JSON preserved in quarantine.
- Watermarks bounded by publication evidence and unresolved days.
- Per-venue HTTP failure isolation; failed venues retain their watermarks.
- Transactional loading and a run log, exercised by deterministic offline tests.

## Decisions and evidence

| Decision | Behaviour |
|---|---|
| Empty responses | Widen and subdivide before accepting absence |
| HTTP errors | Retry transient failures; isolate failed venues; fail if all requested venues fail |
| Schema drift | Abort the entire run when the response contract changes |
| Watermarks | Hold at unresolved rejected or unpublished days |
| Quality gate | Refuse new failures; standing problems remain in the run log and hold the watermark |
| Idempotence | Re-requested days are overwritten; days behind settled watermarks are not re-fetched |

The offline demo loads 720 rows, then 24 new rows on a run three days later:
744 stored rows and zero duplicate `(venue_id, view_date)` pairs.

## Run it

```bash
pip install -e .
pip install pytest
python demo.py
python -m nz_attraction_pageviews --venues venues.csv --db warehouse.duckdb --backfill-days 90
pytest -q
```

Use `--chunk-days`, `--max-reject-rate` and `--today YYYY-MM-DD` to configure
the request windows, quality threshold and run date. Live runs contact Wikimedia.
The CLI prints operational notes as well as row counts.

## Limits

- Pageviews measure online attention, not visitor attendance.
- Use canonical article titles: redirect traffic can be plausible but incomplete.
- Publication lag and genuinely quiet days remain ambiguous; the trust horizon is an assumption.
- A settled watermark prevents historical restatements from being fetched automatically.
- Requests are sequential; this is an eight-venue pipeline with no orchestrator.
- Resolution annotations do not accept rows or advance watermarks; no automatic historical backfill sweep is implemented.

## Data

Wikimedia Analytics daily `per-article`, `all-access`, `user` pageviews (CC0).
No API key is required. Set a contactable User-Agent in `client.py` before running.

[Design notes, table schemas and detailed recovery evidence](docs/DESIGN.md) ·
[HTTP isolation regression tests](tests/test_venue_failures.py)

Repeated rejects are deduplicated by venue, date and rule; `rows_quarantined`
in the run log counts rejected observations in that run, not new unique issues.
Annotate a reviewed issue with:

```bash
python -m nz_attraction_pageviews resolve milford-sound 2026-02-01 --db warehouse.duckdb --resolution "Source reviewed"
```

A recurrence reopens the issue. This command never moves the watermark or
inserts a pageview. Existing historical duplicate records are preserved.

A [live seven-day run](fixtures/live/run-log.json) on 2026-09-06 loaded 56 rows
across all eight venues with zero rejects. Two captured API responses accompany
that log. After installing the package, run `python scripts/capture_live.py` to
capture fresh evidence. The network smoke test is opt-in:
`RUN_LIVE=1 pytest -m live` (PowerShell: set `$env:RUN_LIVE='1'` first).
