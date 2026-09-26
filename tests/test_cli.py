"""The command line, end to end, with urlopen stubbed so nothing leaves the machine."""

from __future__ import annotations

import io
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from nz_attraction_pageviews import __main__ as cli
from nz_attraction_pageviews import client, ingest, quality

TODAY = "2026-03-01"
CSV = (
    "venue_id,venue_name,region,wiki_article\n"
    "milford-sound,Milford Sound,Fiordland,Milford_Sound\n"
    "te-papa,Te Papa,Wellington,Te_Papa\n"
    "sky-tower,Sky Tower,Auckland,Sky Tower (Auckland)\n"
    ",,,\n"
)


class Response(io.BytesIO):
    def __init__(self, status, payload: bytes):
        super().__init__(payload)
        self.status, self.headers = status, {}


class FakeWikimedia:
    """Answers pageview URLs per article: 'ok', 'drift', 'http400' or 'down'."""

    def __init__(self, **behaviour):
        self.behaviour = behaviour
        self.urls = []

    def __call__(self, request, timeout=None):
        url = request.full_url
        self.urls.append(url)
        assert "tests@example.invalid" in request.get_header("User-agent")
        parts = url.rsplit("/", 5)
        article = urllib.parse.unquote(parts[-4])
        start, end = (datetime.strptime(p, "%Y%m%d").date() for p in parts[-2:])
        mode = self.behaviour.get(article, "ok")
        if mode == "down":
            raise urllib.error.URLError(OSError("getaddrinfo failed"))
        if mode == "http400":
            raise urllib.error.HTTPError(url, 400, "Bad Request", {}, io.BytesIO(b""))
        items = [
            {
                "project": "de.wikipedia" if mode == "drift" else "en.wikipedia",
                "article": article,
                "granularity": "daily",
                "timestamp": f"{start + timedelta(days=n):%Y%m%d}00",
                "access": "all-access",
                "agent": "user",
                "views": 10,
            }
            for n in range((end - start).days + 1)
        ]
        return Response(200, json.dumps({"items": items}).encode())


@pytest.fixture
def files(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    venues = tmp_path / "venues.csv"
    venues.write_text(CSV, encoding="utf-8")
    return venues, tmp_path / "warehouse.duckdb"


def ingest_args(files, *extra):
    venues, db = files
    return ["--venues", str(venues), "--db", str(db), "--today", TODAY, *extra]


def last_run(db: Path):
    con = duckdb.connect(str(db))
    try:
        return con.execute(
            "SELECT status, note, http_requests FROM run_log ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    finally:
        con.close()


def test_a_clean_night_prints_ok_and_exits_zero(files, monkeypatch, capsys):
    api = FakeWikimedia()
    monkeypatch.setattr(urllib.request, "urlopen", api)

    assert cli.main(ingest_args(files, "--backfill-days", "5")) == cli.EXIT_OK

    out = capsys.readouterr().out
    assert " ok: 3 requests (3 HTTP calls), 15 fetched, 15 loaded" in out
    assert any("Sky_Tower_%28Auckland%29" in u for u in api.urls), "title normalised"
    assert last_run(files[1]) == ("ok", "", 3)


def test_a_failed_venue_is_degraded_and_still_exits_zero(files, monkeypatch, capsys):
    monkeypatch.setattr(urllib.request, "urlopen", FakeWikimedia(Te_Papa="http400"))

    assert cli.main(ingest_args(files, "--backfill-days", "5")) == cli.EXIT_OK

    out = capsys.readouterr().out
    assert " degraded: " in out
    assert "te-papa: Te_Papa: HTTP 400" in out
    assert "unresolved: te-papa" in out


def test_schema_drift_is_one_line_and_its_own_exit_code(files, monkeypatch, capsys):
    monkeypatch.setattr(urllib.request, "urlopen", FakeWikimedia(Te_Papa="drift"))

    assert cli.main(ingest_args(files, "--backfill-days", "5")) == cli.EXIT_DRIFT

    err = capsys.readouterr().err
    assert err.startswith("error: the API's response no longer matches the contract")
    assert "Traceback" not in err
    assert last_run(files[1])[0] == "failed"


def test_no_network_fails_fast_and_says_why(files, monkeypatch, capsys):
    """Offline, the CLI spent about two minutes retrying each venue and then
    printed a traceback ending in "last status 599"."""
    api = FakeWikimedia(Milford_Sound="down", Te_Papa="down", **{"Sky_Tower_(Auckland)": "down"})
    monkeypatch.setattr(urllib.request, "urlopen", api)

    assert cli.main(ingest_args(files, "--backfill-days", "5")) == cli.EXIT_UPSTREAM

    err = capsys.readouterr().err
    assert "error: all 3 requested venues failed" in err
    assert "getaddrinfo failed" in err, "the cause is logged, not just a status code"
    assert len(api.urls) == 2 * 4, "two venues tried, four attempts each; the third is not"
    status, note, http_requests = last_run(files[1])
    assert status == "failed"
    assert "sky-tower: not attempted, the network looks unreachable" in note
    assert http_requests == 8


def test_bad_arguments_are_refused_before_a_warehouse_is_created(files, capsys):
    venues, db = files
    for flags in (
        ["--max-reject-rate", "1.5"],
        ["--max-reject-rate", "nan"],
        ["--backfill-days", "10000000"],
        ["--chunk-days", "0"],
    ):
        with pytest.raises(SystemExit) as exc:
            cli.main(ingest_args(files, *flags))
        assert exc.value.code == cli.EXIT_USAGE, flags
    assert "Traceback" not in capsys.readouterr().err

    assert cli.main(ingest_args(files, "--backfill-days", "365")) == cli.EXIT_CONFIG
    assert "max_lookback_days" in capsys.readouterr().err
    assert not db.exists(), "a refused run left an empty warehouse behind"


def test_a_missing_venues_file_is_one_line(files, capsys):
    _, db = files
    code = cli.main(["--venues", "nope.csv", "--db", str(db), "--today", TODAY])
    assert code == cli.EXIT_CONFIG
    assert capsys.readouterr().err.startswith("error: ")


def test_a_live_run_without_a_contact_is_refused(files, monkeypatch, capsys):
    monkeypatch.delenv(client.CONTACT_ENV)
    assert cli.main(ingest_args(files)) == cli.EXIT_CONFIG
    assert "--contact" in capsys.readouterr().err
    assert not files[1].exists()


def test_the_warehouse_is_not_held_open_while_fetching(files, monkeypatch):
    """A read-write DuckDB connection locks the file against every other process,
    readers included, so holding it through the fetch locked out `resolve` and
    BI tools for as long as the network took. It is now opened only to plan and
    to load."""
    opened = []
    real_connect = ingest.connect

    def tracking_connect(path):
        con = real_connect(path)
        opened.append(con)
        return con

    def is_open(con):
        try:
            con.execute("SELECT 1")
            return True
        except duckdb.ConnectionException:
            return False

    fetching_with_open = []
    api = FakeWikimedia()

    def urlopen(request, timeout=None):
        fetching_with_open.append(any(is_open(c) for c in opened))
        return api(request, timeout)

    monkeypatch.setattr(ingest, "connect", tracking_connect)
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)

    assert cli.main(ingest_args(files, "--backfill-days", "5")) == cli.EXIT_OK
    assert fetching_with_open and not any(fetching_with_open)
    assert len(opened) == 2, "once to plan, once to load"


def test_check_venues_names_a_redirect(files, monkeypatch, capsys):
    def api(request, timeout=None):
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
        titles = query["titles"][0].split("|")
        payload = {
            "query": {
                "normalized": [{"from": t, "to": t.replace("_", " ")} for t in titles],
                "redirects": [{"from": "Te Papa", "to": "Te Papa Tongarewa"}],
                "pages": [{"title": t.replace("_", " ")} for t in titles if t != "Te_Papa"]
                + [{"title": "Te Papa Tongarewa"}],
            }
        }
        return Response(200, json.dumps(payload).encode())

    monkeypatch.setattr(urllib.request, "urlopen", api)
    venues, _ = files

    assert cli.main(["check-venues", "--venues", str(venues)]) == cli.EXIT_NOTHING_MATCHED
    out = capsys.readouterr().out
    assert "REDIRECT: use 'Te_Papa_Tongarewa'" in out
    assert out.count(" ok") == 2


@pytest.mark.parametrize("name", ["Milford_Sound", "Te_Papa"])
def test_the_captured_live_responses_still_meet_the_contract(name):
    """The fixtures are real Wikimedia answers from 2026-09-06. If _parse or the
    acceptance rules drift away from what the API actually sends, this fails
    without needing the network."""
    path = Path(__file__).parent.parent / "fixtures" / "live" / f"{name}.json"
    items = client._parse(path.read_bytes(), name)
    days = sorted(quality.parse_timestamp(i["timestamp"]) for i in items)

    clean, bad = quality.check_window(
        items, venue_id="v", article=name, start=days[0], end=days[-1]
    )

    assert bad == []
    assert len(clean) == len(items) == 7
    assert days[-1] - days[0] == timedelta(days=6)
    assert all(isinstance(r.views, int) and r.views >= 0 for r in clean)
    assert days[-1] <= date(2026, 9, 6)
