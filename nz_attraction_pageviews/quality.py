"""Acceptance criteria for a fetched window.

Two decisions worth defending in a review:

- A row that breaks a rule is quarantined, not dropped. The row keeps the name
  of the rule it broke, so "why is Tuesday missing" is answerable from a table
  rather than from a log file that has rotated away.
- Nothing here decides whether a venue loads. That is `ingest._apply_gate`,
  which holds a venue whose newly rejected share is over the ceiling, and
  `ingest.run`, which reports any venue with an unresolved rejection as
  `degraded`.
"""

from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime

# `pageviews.views` is a BIGINT. A larger int would pass every other rule and
# then fail inside the driver, aborting the load for every venue. A row the
# warehouse cannot hold is a bad row, so it is rejected here like any other.
VIEWS_MAX = 2**63 - 1

# Characters MediaWiki never allows in a title. One of these in venues.csv is a
# typo, not an article, and `|` would also split a batched title lookup.
FORBIDDEN_TITLE_CHARS = frozenset("#<>[]{}|")


@dataclass(frozen=True)
class CleanRow:
    venue_id: str
    article: str
    view_date: date
    views: int


@dataclass(frozen=True)
class BadRow:
    venue_id: str
    article: str
    view_date: date | None
    rule: str
    detail: str
    raw: str
    # The window the row arrived in. For a row with no parseable day this is
    # the only thing that says which days it might have belonged to, so the
    # watermark stops before `window_start` rather than freezing the venue.
    window_start: date | None = None
    window_end: date | None = None


def parse_timestamp(value) -> date:
    """Wikimedia sends daily timestamps as YYYYMMDD00.

    The trailing 00 is the "daily": an hourly stamp such as 2026031012 would
    otherwise load as that day's total when it is one hour of it.
    """
    text = str(value)
    if not re.fullmatch(r"\d{8}00", text):
        raise ValueError(f"expected Wikimedia daily timestamp YYYYMMDD00, got {value!r}")
    return datetime.strptime(text[:8], "%Y%m%d").date()


def _date_or_none(item: dict) -> date | None:
    """The row's day, or None when that is exactly what is wrong with it."""
    try:
        return parse_timestamp(item["timestamp"])
    except (ValueError, KeyError, TypeError):
        return None


def check_window(
    items: list[dict],
    *,
    venue_id: str,
    article: str,
    start: date,
    end: date,
) -> tuple[list[CleanRow], list[BadRow]]:
    """Apply every rule to every row. Returns (clean, quarantined).

    Two passes, because `one_row_per_date` is a property of the response rather
    than of a row. When one response names a day twice with different figures,
    nothing says which is right, so neither is loaded: taking the first would
    put a possibly wrong number in `pageviews` and quarantine only the other.
    """
    bad: list[BadRow] = []
    candidates: list[tuple[dict, date]] = []

    def reject(item: dict, rule: str, detail: str) -> None:
        bad.append(
            BadRow(
                venue_id,
                article,
                _date_or_none(item),
                rule,
                detail,
                json.dumps(item, ensure_ascii=False),
                start,
                end,
            )
        )

    for item in items:
        broken = _first_broken_rule(item, article=article, start=start, end=end)
        if broken is not None:
            reject(item, *broken)
        else:
            candidates.append((item, parse_timestamp(item["timestamp"])))

    per_day = Counter(view_date for _, view_date in candidates)
    clean: list[CleanRow] = []
    for item, view_date in candidates:
        if per_day[view_date] > 1:
            figures = [i["views"] for i, d in candidates if d == view_date]
            reject(
                item,
                "one_row_per_date",
                f"{view_date} appears {per_day[view_date]} times with views {figures}; "
                f"none of them was loaded",
            )
        else:
            clean.append(CleanRow(venue_id, article, view_date, item["views"]))

    return clean, bad


def normalise_title(title: str) -> str:
    """One spelling of a title, so two spellings of the same one compare equal.

    - NFC: a macron can be one codepoint or `u` plus a combining macron. Both
      render as `ū`; the API answers in NFC, and macOS text entry and some
      spreadsheets produce NFD.
    - Underscores for spaces, and a capital first letter: MediaWiki's own
      canonical form, which the title-exact pageviews API expects. Without it
      `Sky Tower (Auckland)` 404s every night, and `milford_Sound` fetches rows
      that all fail `article_matches_request`.
    """
    text = unicodedata.normalize("NFC", title).strip().replace(" ", "_")
    return text[:1].upper() + text[1:]


def invalid_title_chars(title: str) -> list[str]:
    """Characters MediaWiki does not allow in a title, in the order they appear."""
    return sorted(set(title) & FORBIDDEN_TITLE_CHARS, key=title.index)


def _same_title(got, asked: str) -> bool:
    """Compare titles, tolerating an `article` that is not a string at all.

    `_parse` checks only that the field is present, so a drifted null or number
    can arrive here. That is a row that does not match, not a reason to raise.
    """
    if not isinstance(got, str) or not isinstance(asked, str):
        return got == asked
    return normalise_title(got) == normalise_title(asked)


def _title_mismatch_detail(got, asked: str) -> str:
    """Name the difference, adding the escaped form when it may not be visible.

    Codepoints can differ while rendering identically, and `got 'Tūrangi',
    asked for 'Tūrangi'` is true and useless.
    """
    plain = f"got {got!r}, asked for {asked!r}"
    if not isinstance(got, str) or not isinstance(asked, str):
        return plain
    if got.isascii() and asked.isascii():
        return plain
    escaped_got = got.encode("unicode_escape").decode()
    escaped_asked = asked.encode("unicode_escape").decode()
    return f"{plain} ({escaped_got} vs {escaped_asked})"


def _first_broken_rule(
    item: dict,
    *,
    article: str,
    start: date,
    end: date,
) -> tuple[str, str] | None:
    """Return (rule, detail) for the first rule this row breaks, or None if it is clean.

    Rules are ordered cheapest and most fundamental first, so the reported rule is
    the root cause rather than a downstream symptom. `one_row_per_date` is not
    here: it needs the whole response, so `check_window` applies it last.

    There is no "date in the future" rule. The window never ends later than
    `today - PUBLICATION_LAG_DAYS`, so a future date always fails
    `date_in_requested_window` first.
    """
    if not _same_title(item["article"], article):
        return "article_matches_request", _title_mismatch_detail(item["article"], article)

    try:
        view_date = parse_timestamp(item["timestamp"])
    except ValueError as exc:
        return "timestamp_parses", str(exc)

    if not (start <= view_date <= end):
        return "date_in_requested_window", f"{view_date} outside {start}..{end}"

    views = item["views"]
    if isinstance(views, bool) or not isinstance(views, int):
        return "views_is_integer", f"got {type(views).__name__} {views!r}"

    if views < 0:
        return "views_non_negative", f"got {views}"

    if views > VIEWS_MAX:
        return "views_within_bigint", f"got {views}, above the {VIEWS_MAX} the column holds"

    return None


def reject_rate(fetched: int, quarantined: int) -> float:
    """Share of fetched rows that were rejected; 0.0 when nothing was fetched."""
    if fetched == 0:
        return 0.0
    return quarantined / fetched
