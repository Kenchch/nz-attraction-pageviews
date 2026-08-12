# NZ Attraction Pageviews

Incremental ingest of daily Wikipedia pageviews for eight New Zealand visitor
attractions, into DuckDB.

Pageviews are a weak proxy for visitor interest, not a measure of attendance.
The point of the project is not the metric. It is the ingest: what happens when
a third-party API rate limits you, answers 404 for a quiet week, restates a
figure it already gave you, or quietly changes its response shape.

```
venues.csv  ->  windowed API calls  ->  acceptance criteria  ->  gate  ->  DuckDB
                (retry / backoff)       (quarantine, not drop)   (abort)
```

## Run it

```bash
pip install -r requirements.txt

python demo.py        # offline, synthetic API, no network needed
python -m src         # live, hits the Wikimedia API
pytest -q             # 39 tests, all offline
```

`demo.py` output:

```
run 1 (first sight)        ok     requests=24  fetched=720  loaded=720  quarantined=0
run 2 (three days later)   ok     requests=8   fetched=24   loaded=24   quarantined=0

744 rows, 0 duplicate (venue, date) pairs
```

Run 2 asked for 8 requests instead of 24 because seven of the eight venues were
already current up to their watermark. That is the whole point of the watermark.

## Six decisions worth arguing about

**1. A 404 is data, not an error — but it is not evidence either.**
The Wikimedia API returns 404 when an article had no traffic in the window, not
just when the article does not exist. Treating that as a failure would abort a
run because a small museum had a quiet week.

The trap is believing it. The live API will answer 404 for one window and 200,
for those same days, to a request that starts earlier:

```
Hobbiton_Movie_Set, ending 2026-08-10:  1d 200  2d 200  3d 200  5d 404  7d 404  10d 200  14d 200
```

Not a flake — the same URL 404s eight times out of eight, while the 14 day
window reports 98, 90, 94, 91, 96, 106 and 124 views on the days the 7 day
window calls empty. Believing it loses those days permanently, because the
watermark advances past them and nothing asks again. The run still reports `ok`
with a 0% reject rate, since the rows were never rejected — they never arrived.

So `fetch_window` re-asks an empty window with an earlier start and trims the
answer back to the range requested. Only when every widening also 404s is the
window accepted as quiet. An article that genuinely does not exist 404s at every
width, which is what stops this from inventing data.

**2. Retry 429 and 5xx. Never retry 400.**
A malformed request will be malformed on the retry too, so retrying it only
burns someone else's rate limit. Backoff honours `Retry-After` when the server
sends one, otherwise `2^attempt` plus jitter. The jitter matters because eight
venues are fetched in a loop: without it, every retry lands on the same second
and recreates the burst that caused the throttle.

**3. A missing field is schema drift, and it stops the run.**
`EXPECTED_FIELDS` is the contract. If the API drops a field, the run fails with
the field name in the message. The alternative is that the field silently
becomes NULL and surfaces three tables downstream as a chart with a gap in it.

**4. Bad rows are quarantined, not dropped.**
Every rejected row lands in `quarantine` with the rule it broke and its raw
payload. "Why is Tuesday missing" is then a SQL query, not an archaeology dig
through rotated logs. Rules are checked in order, so the rule you see is the
root cause and not a downstream symptom.

**5. The gate runs before the load, not after.**
If more than 5% of a run is rejected, nothing is written and last night's data
stays intact. A partial load is worse than no load, because a partial load still
renders on a dashboard and looks fine.

**6. The load is one transaction, and re-running is free.**
`pageviews` is keyed on `(venue_id, view_date)` and loaded with
`INSERT OR REPLACE`, so a restated figure overwrites rather than duplicates, and
running the job twice in a morning is harmless. The whole run commits or none of
it does, so a crash on venue five cannot leave venues one to four a day ahead of
the rest.

## Tables

| Table | What it holds |
|---|---|
| `pageviews` | Clean daily views, keyed `(venue_id, view_date)` |
| `quarantine` | Rejected rows with the rule they broke and the raw payload |
| `watermark` | Last date covered per venue, so the next run resumes there |
| `run_log` | One row per run: counts, reject rate, status, failure note |

A failed run still writes to `run_log`. A job that fails silently is worse than
one that fails loudly.

```sql
-- did anything go wrong lately, and how much
SELECT started_at, status, rows_loaded, rows_quarantined, reject_rate, note
FROM run_log ORDER BY started_at DESC LIMIT 10;

-- what got rejected and why
SELECT rule, count(*) FROM quarantine GROUP BY 1 ORDER BY 2 DESC;
```

## Acceptance criteria

Applied per row, in this order:

| Rule | Rejects |
|---|---|
| `article_matches_request` | A response for an article we did not ask for |
| `timestamp_parses` | Anything that is not `YYYYMMDD00` |
| `date_in_requested_window` | A date outside the window we requested |
| `date_not_in_future` | A date after today |
| `views_is_integer` | Strings, floats, and `True` (which would otherwise load as 1) |
| `views_non_negative` | Negative counts |
| `one_row_per_date` | A second row for a date already seen in the window |

## Testing

39 tests, no network. The HTTP call is injected into `fetch_window` and the
fetcher is injected into `ingest.run`, so the suite drives real code paths with
stubbed transport rather than mocking out the logic being tested.

Covered: URL quoting for titles like `Sky_Tower_(Auckland)`, 404 verified before
it is believed and accepted as empty only when every widening agrees, recovered
rows trimmed to the requested window, unparseable timestamps left for quarantine
rather than dropped in the trim, `Retry-After` honoured, backoff growth, give-up
after max attempts, 400 not retried, both schema drift cases, every acceptance
rule, window tiling with no gap or overlap, watermark resume, idempotent re-run,
restatement overwrite, and a gate failure leaving the warehouse untouched.

CI runs lint and tests on Python 3.10 through 3.13.

## Limits

- Pageviews measure interest, not attendance. Do not read them as visitor numbers.
- Wikimedia publishes with a lag, so the job stops two days short of today.
- Runs are sequential. Eight venues is small enough that concurrency would add
  more failure modes than it removes wall clock.
- The watermark advances to the end of the requested range, including windows
  that came back empty. Otherwise a genuinely quiet venue would be re-requested
  forever. Decision 1 is what makes that safe: an empty window is verified
  against a wider request before the watermark moves past it. A Wikimedia outage
  returning 404 instead of 503 for every width would still be recorded as "no
  traffic". Catching that needs a backfill sweep, which is not built here.
- No orchestrator. In production this would be an Airflow or cron task; the
  entry point is deliberately a single idempotent command so that wiring it up
  is one line.

## Data

Wikimedia Analytics pageviews API, `per-article` daily, `all-access`, `user`
agent (bots excluded). No API key. Wikimedia asks for a contactable User-Agent,
set in `src/client.py` — change it to your own before running the live path.

Titles in `venues.csv` must be canonical, not redirects. The pageviews API is
title-exact and does not follow redirects, so a redirect title returns only the
traffic that arrived through that redirect — which looks like a plausible number
rather than an obviously wrong one. `Waitomo_Glowworm_Caves` reports around 175
views a month; the article it redirects to, `Waitomo_Glowworm_Cave`, reports
about 2400. Nothing in the pipeline can catch this, because a small number is
not an invalid one.

Data is licensed CC0 by the Wikimedia Foundation.
