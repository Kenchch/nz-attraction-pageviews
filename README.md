# NZ Attraction Pageviews

[![CI](https://github.com/Kenchch/nz-attraction-pageviews/actions/workflows/ci.yml/badge.svg)](https://github.com/Kenchch/nz-attraction-pageviews/actions/workflows/ci.yml)

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

python demo.py                     # offline, synthetic API, no network needed
python -m nz_attraction_pageviews  # live, hits the Wikimedia API
pytest -q                          # 167 tests, all offline
```

`demo.py` output:

```
run 1 (first sight)        ok     requests=24  fetched=720  loaded=720  quarantined=0
run 2 (three days later)   ok     requests=8   fetched=24   loaded=24   quarantined=0

744 rows, 0 duplicate (venue, date) pairs
```

All eight venues ask in both runs. Run 1 backfills 90 days, which is three
30 day windows each, so 24 requests. Run 2 happens three days later and each
venue needs only the three days since its watermark — one window each, so eight
requests and 24 rows instead of 720. That is the whole point of the watermark:
not that venues drop out, but that the range each one asks for collapses to what
it has not already seen.

## Six decisions worth arguing about

**1. A 404 is data, not an error — but it is not evidence either.**
The Wikimedia API returns 404 when an article had no traffic in the window, not
just when the article does not exist. Treating that as a failure would abort a
run because a small museum had a quiet week.

The trap is believing it. Whether the API answers 404 depends on the *shape* of
the request, not only on whether the days have traffic:

```
Hobbiton_Movie_Set, ending 2026-08-10:  1d 200  2d 200  3d 200  5d 404  7d 404  10d 200  14d 200
```

Not a flake — the same URL 404s eight times out of eight, while the 14 day
window reports 98, 90, 94, 91, 96, 106 and 124 views on the days the 7 day
window calls empty. Believing it loses those days permanently, because the
watermark advances past them and nothing asks again. The run still reports `ok`
with a 0% reject rate, since the rows were never rejected — they never arrived.

Widening the window catches many of these, and it is one cheap request, so it is
tried first. It is not enough on its own. A live 30 day window,
`Hobbiton_Movie_Set` over 2026-07-13..2026-08-11, answered 404 at its own width
*and* at both widenings — while every 7 day slice of it answered 200. That run
loaded 690 rows instead of 720 and called itself `ok`.

So an empty answer is checked twice over: widen first, and if that still comes
back empty, halve the window and ask about the pieces, recursing only into the
pieces that also 404. Against `WidthSensitiveApi` in the test suite, which
refuses any span over 7 days, a 30 day window gives up all 30 days for 13
requests. An article that genuinely does not exist answers 404 at every width
and every slice, down to single days, which is what stops this from inventing
data.

The live count is deliberately not quoted, because it does not hold still: the
same 2026-07-13..2026-08-11 window that 404'd eight times out of eight later
answered 200 on the first request. That is the argument for verifying rather
than measuring once and hard-coding what you saw.

"Empty" means empty, not 404. A 200 carrying `items: []` says exactly what the
404 says — no days — and gets exactly the same scrutiny. Verifying only the 404
left the identical hole open to any upstream that reports nothing with a
success code instead of an error one, and it costs one request to close.

A 200 gets no more benefit of the doubt than the 404 did. Widening is treated as
a cheap way to *get* rows, never as evidence that there are none, so the
subdivision runs regardless of what the widening produced and the two results are
merged by day. A widened response that answers with only some of the requested
days is the same failure as the 404 wearing a success code, and returning early
on it would leave the omitted days for the watermark to notice rather than fixing
them here. That costs two extra requests when the halves answer — the whole
subdivision only recurses into pieces that 404.

The merge is between the two sources only. A single response that names the same
day twice keeps both rows, for `one_row_per_date` to judge in the next stage.
Collapsing them here would be the verification layer quietly deciding a quality
question, and the drift would never be recorded — which is the failure this
module exists to prevent, arriving by the back door.

**2. Retry 429 and 5xx. Never retry 400.**
A malformed request will be malformed on the retry too, so retrying it only
burns someone else's rate limit. Backoff honours `Retry-After` when the server
sends one, otherwise `2^attempt` plus jitter. The jitter matters because eight
venues are fetched in a loop: without it, every retry lands on the same second
and recreates the burst that caused the throttle.

`Retry-After` is treated as a request, not an instruction, and capped at two
minutes. It is a number chosen by someone else's infrastructure: a misconfigured
proxy answering `86400` would park a nightly job for a day, and `float` accepts
`inf` and `nan` as readily as `30`. Capped, the whole retry sequence is bounded
at a few minutes, after which the window fails loudly and the watermark stays
put — so the days are asked for again tomorrow rather than lost.

**3. A missing field is schema drift, and so is a wrong value.**
`EXPECTED_FIELDS` is the contract. If the API drops a field, the run fails with
the field name in the message. The alternative is that the field silently
becomes NULL and surfaces three tables downstream as a chart with a gap in it.
An item that is not an object at all — a bare number where a row should be —
is the same failure and gets the same named error, rather than a `TypeError`
thrown from inside a set operation with nothing in it to say which article.

Presence was never the whole risk. `project`, `granularity`, `access` and
`agent` say *what was counted*, and nothing downstream reads them: every
acceptance rule below judges the date and the count. So a reply describing
`de.wikipedia`, `monthly`, `desktop`, `spider` passed straight through as a
clean row — a month of German bot traffic stored under one New Zealand day,
loaded, with the watermark advanced past it. Measured before the check
existed: 1 clean row, 0 quarantined. These four are now compared against what
the URL asked for, and a mismatch stops the run rather than quarantining one
row, because the request and the response disagree about the question and
every other row in the payload is equally suspect.

**4. Bad rows are quarantined, not dropped — and the watermark respects that.**
Every rejected row lands in `quarantine` with the rule it broke and its raw
payload. "Why is Tuesday missing" is then a SQL query, not an archaeology dig
through rotated logs. Rules are checked in order, so the rule you see is the
root cause and not a downstream symptom.

Quarantining is only half of it. The gate below is a rate across the whole run,
so one venue's bad patch can sit under the threshold and pass: 10 bad days out
of 240 rows is 4.2%, the run reports `ok`, and that venue loads 20 days instead
of 30. If its watermark then advanced to the end of the range anyway, those 10
days would never be requested again and the quarantine would be a record of data
permanently lost — quarantine with the outcome of a drop.

The same hole opens without any rejected row at all. If the API answers 200 and
just does not mention the last three days, nothing is quarantined — the
acceptance criteria only judge rows that turned up — and the watermark would
still march to the end of the range. That is what a publication lag longer than
`PUBLICATION_LAG_DAYS` looks like, and two days is an assumption about Wikimedia,
not a promise from it.

An absent day is genuinely ambiguous, and no amount of asking resolves it. The
API omits days with no traffic rather than sending a zero, so absent means either
"nobody looked" or "not published yet" — and an unpublished day 404s at every
width and every slice exactly like a quiet one, so decision 1 cannot tell them
apart either. Picking one meaning is wrong in one direction or the other:
believing "quiet" loses days whenever the lag runs long, and believing
"unpublished" strands any venue quiet enough to have gaps. `Te_Rerenga_Wairua`
has 74 absent days out of 90.

Two things do resolve it. Publication runs in date order, so *anything before a
day that did arrive is settled* — a later day arriving proves the earlier one was
published, and its absence can only mean quiet. Past that, only age: an absent
day older than `TRUST_LAG_DAYS` is taken as quiet, a more recent one is left
alone and asked for again next run.

Age alone is a bet, though, and the stake is permanent. `TRUST_LAG_DAYS` minus
`PUBLICATION_LAG_DAYS` is five days of margin, and a publication stall longer
than that walked the watermark over days nothing had ever published, one per
night, with every run still reporting `ok`; a lag that settled above seven days
overtook the frontier for good and the venue never loaded another row. So the
trust line is also bounded by the newest day *any* venue has produced a row for,
this run or in any run before it — publication is a property of the API, not of
one article. During a stall that frontier stops, the trust line stops with it,
and the days cost a re-request instead of vanishing. With no evidence at all — an empty
warehouse and a run that fetched nothing — the calendar line stands, so a first
run against quiet venues still makes progress. Once the warehouse holds a single
row that escape hatch closes, which is the point: from then on the frontier is
the honest answer to "has anything been published this recently?". A venue that
stops advancing is named in `run_log.note` as *N days behind*, so holding is
visible rather than something you have to notice.

So the watermark advances to the last day actually loaded, holes behind it
included, or to the trust line, whichever is
further — and never past a quarantined day *inside the range it asked for*,
whatever its age. A rejected row dated outside that range is still recorded in
`quarantine`, but it does not hold the watermark: the watermark only promises
about days this run requested, and it passed that one long ago. Stopping for it
would neither recover the day nor stop happening. The cost of all this is that
every venue re-asks for its last few days each night.

**5. The gate runs before the load, not after.**
If more than 5% of a run is rejected, nothing is written and last night's data
stays intact. A partial load is worse than no load, because a partial load still
renders on a dashboard and looks fine. The gate is deliberately a whole-run rate
rather than a per-venue one: at eight venues on a daily schedule a per-venue rate
is one or two rows, where a single bad row reads as 100% and would abort the run
for everyone. Decision 4 is what makes that safe — the run continues, but no
venue advances past a day it did not load.

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
| `quarantine` | Rejected rows: the day, the rule they broke, and the raw payload |
| `watermark` | Last date covered per venue, so the next run resumes there |
| `run_log` | One row per run: counts, reject rate, status, failure note |

A failed run still writes to `run_log`, with the reject rate that failed it and
whatever else it had to say — a run that abandoned days *and then* failed keeps
both facts in `note`, since the half that is not in the traceback is the half
you would never otherwise learn. A job that fails silently is worse than one that
fails loudly.

`quarantine.view_date` is nullable, and the null case is the honest one: a row
rejected by `timestamp_parses` has no day to record, because the day is what was
wrong with it. Every other rule fills it in.

```sql
-- did anything go wrong lately, and how much
SELECT started_at, status, rows_loaded, rows_quarantined, reject_rate, note
FROM run_log ORDER BY started_at DESC LIMIT 10;

-- what got rejected and why. Count days, not rows: a venue stuck on a bad day
-- re-quarantines it every run, so count(*) measures how long it has been stuck
-- rather than how much is actually wrong.
SELECT rule,
       count(DISTINCT (venue_id, view_date)) AS bad_days,
       count(*)                              AS rows_seen
FROM quarantine GROUP BY 1 ORDER BY 2 DESC;

-- which venues are stuck, and how far behind
SELECT venue_id, last_date FROM watermark ORDER BY last_date;
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
| `views_within_bigint` | Counts too large for the column, which would fail the load itself |
| `one_row_per_date` | A second row for a date already seen in the window |

## Testing

167 tests, no network. The HTTP call is injected into `fetch_window` and the
fetcher is injected into `ingest.run`, so the suite drives real code paths with
stubbed transport rather than mocking out the logic being tested.

Covered: URL quoting for titles like `Sky_Tower_(Auckland)`, 404 verified before
it is believed and accepted as empty only when every widening and every slice
agrees, a first widening that answers 200 with pad-region rows only still falling
through to the second widening rather than spending its silence as evidence,
a widened 200 that mentions none of the requested days subdivided rather
than believed, a window that answers only in slices stitched back together in
order, subdivision recursing only into the pieces that failed, recovered rows
trimmed to the requested window on both paths, an item that is not an object
raising schema drift by name, unparseable timestamps left for quarantine rather
than dropped in the trim, `Retry-After` honoured, backoff growth, give-up
after max attempts, 400 not retried, both schema drift cases, every acceptance
rule, window tiling with no gap or overlap, watermark resume, idempotent re-run,
restatement overwrite, a gate failure leaving the warehouse untouched and still
logging the rate that failed it, the watermark refusing to step over a
quarantined day, over a day missing from the tail of a 200, or over a hole in the
middle of one — while a later run recovers all three — quarantined rows carrying
the day they belong to (and a null day when that is the defect), an absent day
trusted once something later arrives or once it is old enough but not before, a
sparse venue still making progress, the watermark never moving backwards, the
lookback cap bounding a stuck venue while recording what it gave up on even when
the run then fails, `Retry-After` capped rather than obeyed when a server asks
for a day or sends something that is not a finite number, and `venues.csv`
rejected with a line number for a blank field, a duplicate `venue_id` or a
missing column while tolerating a BOM and an extra column.

CI runs lint, format check, and tests on Python 3.10 through 3.13.

## Limits

- Pageviews measure interest, not attendance. Do not read them as visitor numbers.
- Wikimedia publishes with a lag, so the job stops two days short of today. Two
  is a guess at a normal day, not a guarantee; during an incident the lag can be
  longer. The watermark absorbs that rather than depending on the guess being
  right — it stops at the last day the API actually returned, so a longer lag
  costs a re-request next run instead of a permanent hole.
- Runs are sequential. Eight venues is small enough that concurrency would add
  more failure modes than it removes wall clock.
- Verifying a 404 is not free, and the bill scales with how wrong the API is
  being. Measured on a 30 day window against the stubs in the test suite: 4
  requests when only that exact window is refused, 13 when the widenings fail too
  and the answer is only reachable in 7 day slices, and 61 when the article does
  not exist at all — because that 404s the whole way down to single days. So a
  mis-typed title costs ~60 requests per window per run instead of 1. At eight
  venues that is affordable, and the alternative is losing days silently. At
  several hundred it would need a memo of which windows have already been
  verified empty, so a backfill does not re-derive the same nothing every night.
- Verification is not free in the ordinary case either, not just the pathological
  one. A venue whose recent days are genuinely absent re-asks for them every
  night, and each of those windows now 404s or answers empty and is verified in
  full — roughly 13 requests where a venue with traffic costs 1. An upstream that
  reports "no data" as `200 {"items": []}` rather than 404 is now verified the
  same way, so a first run against one costs about 1,400 requests for eight
  venues rather than 24. That is the price of not believing it; a request budget
  that stops a run rather than a window is the thing this does not have. At eight venues
  that is affordable; at several hundred it would need a memo of which windows
  have already been verified empty.
- A typo in `venues.csv` looks exactly like a quiet venue: the article does not
  exist, every width and slice 404s, and the window verifies as genuinely empty.
  A venue that has never produced a single row is therefore named in
  `run_log.note`, since nothing else in the summary tells the two apart.
- The trust line is bounded by the newest day any venue has ever returned. If the
  upstream genuinely publishes nothing at all for a stretch — every venue, every
  day — no watermark advances during it. That is deliberate: the alternative is
  the silent loss it replaced. It does mean a warehouse with a single, very quiet
  venue leans on the calendar line more than a warehouse with eight.
- `run_log.requests` counts windows asked for, not HTTP calls. Verification can
  turn one window into several calls, so the two diverge exactly when the API is
  misbehaving.
- A venue whose bad day never becomes good stops advancing, and then re-asks for
  everything from that day to today — not just the bad day — so both the range and
  the rewrite grow every run. `max_lookback_days` caps that at 180 days. Hitting
  the cap is not free: the span below it is abandoned, which is the same silent
  skip the watermark logic exists to prevent, so it is written to `run_log.note`
  (`v0: gave up on 12 days`) rather than happening quietly. A stuck venue is a
  query — `SELECT venue_id, last_date FROM watermark ORDER BY last_date` — not a
  surprise six months later.
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
set in `nz_attraction_pageviews/client.py` — change it to your own before running
the live path.

Titles are normalised to NFC on read. A macron is one codepoint in NFC and two
in NFD; both render as `ū`, macOS text entry and some spreadsheets produce NFD,
and the API answers in NFC — so an un-normalised `Tūrangi` would quarantine
every row it ever fetched, under a `detail` reading `got 'Tūrangi', asked for
'Tūrangi'`.

Titles in `venues.csv` must be canonical, not redirects. The pageviews API is
title-exact and does not follow redirects, so a redirect title returns only the
traffic that arrived through that redirect — which looks like a plausible number
rather than an obviously wrong one. `Waitomo_Glowworm_Caves` reports around 175
views a month; the article it redirects to, `Waitomo_Glowworm_Cave`, reports
about 2400. Nothing in the pipeline can catch this, because a small number is
not an invalid one.

Data is licensed CC0 by the Wikimedia Foundation.
