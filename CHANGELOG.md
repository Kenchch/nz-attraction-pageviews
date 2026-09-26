# Changelog

## 0.2.0 — 2026-09-26

The run now reports `ok` only when the warehouse is complete for every venue.
Several fixes change what an existing deployment sees; read **Upgrading** first.

### Upgrading from 0.1.0

- **A live run needs a contact.** Pass `--contact you@example.org` or set
  `NZAP_CONTACT` (an email address or URL). Without one the run refuses to
  start, rather than sending Wikimedia requests under the original author's
  address.
- **Expect `degraded` at first.** Failed venues, venues that have never produced
  a row, days given up to the lookback cap, and rejections from earlier nights
  used to report `ok`. The note ends `unresolved: <venues>`. A rejection whose
  day has since loaded cleanly is marked `superseded` on the first run and stops
  counting; the rest need `resolve … --accept` or a look upstream.
- **The warehouse is migrated when opened.** New columns: `quarantine.window_start`,
  `window_end`, `note`; `watermark.article`; `run_log.http_requests`. Notes that
  earlier versions stored in `quarantine.resolution` move to `note`, and those
  rows reopen: a note never released anything.
- **`--resolution` is now `--note`.** The old name still works.
- **`--backfill-days` above `--max-lookback-days` (180) is refused** instead of
  being silently cut to it.
- **`requirements.txt` is gone.** Use `uv sync`, or `pip install -e . --group dev`
  (pip 25.1+). The package installs a `nz-attraction-pageviews` command.
- **Exit codes are distinct:** 0 ok or degraded, 1 nothing matched / bad title,
  2 bad arguments, 3 configuration, 4 every venue failed, 5 schema drift,
  6 warehouse unavailable.

### Fixed

- A night with a first-time rejection, a failed venue, days given up or a venue
  that has never produced a row reported `ok`.
- A one-night glitch kept every later run `degraded` after the day had loaded.
- Days left out of the middle of a 200 were trusted as quiet and never asked
  for again; they are now re-asked in narrow slices.
- A row with an unparseable timestamp froze its venue's watermark permanently,
  and `resolve … null --accept` did nothing.
- Annotating then accepting matched nothing; accepting discarded the reason;
  a recurring rejection erased the operator's note.
- A 200 that was not JSON (an HTML error page, a huge integer, deep nesting)
  aborted every venue; it is retried and then fails only its venue.
- Transport failures all read `last status 599`; certificate failures were
  retried; offline, each venue spent two minutes retrying.
- A slow server could hold one request open indefinitely.
- A date named twice in one response loaded the first figure.
- A title written with spaces or a lower-case first letter, and a blank line at
  the end of venues.csv, broke the run.
- The warehouse was locked for the whole network phase.

### Added

- `resolve` takes a `FIRST..LAST` range or `all`, and says when nothing matched.
- `check-venues` asks MediaWiki whether each title is canonical.
- A changed title re-fetches the venue's backfill window under the new one.
- `run_log.http_requests` and a per-run budget (`--max-http-requests`, 2,000).
- `-v` logging; retries are logged by default.
- CI installs from `uv.lock`, enforces 90% coverage and builds the package; a
  weekly workflow runs the live smoke test. One CI run per ref, and a
  pull-request template (#8).

### Changed

- A venue over the reject ceiling is held on its own instead of the whole run
  being refused (#7): the whole-run gate had kept healthy venues from loading
  for nights on end.
- `ingest.py` is split: `store.py` holds the schema and every SQL statement,
  `watermark.py` the pure rules for where a venue starts and how far it may
  advance. `ingest` still exports the same public names.
- The unreachable `date_not_in_future` rule and the whole-run gate's leftovers
  (`QualityGateFailed`, the abort path) are removed.

## 0.1.0 — 2026-09-07

Tagged at `583a249`: windowed ingest with verified empties, quarantine, a
whole-run quality gate, per-venue isolation of HTTP failures, and a
transactional load.
