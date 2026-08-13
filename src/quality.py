"""Acceptance criteria for a fetched window.

Two decisions worth defending in a review:

- A row that breaks a rule is quarantined, not dropped. The row keeps the name
  of the rule it broke, so "why is Tuesday missing" is answerable from a table
  rather than from a log file that has rotated away.
- The gate is checked before anything is written. If today's extract is bad,
  last night's data stays intact. A partial load is worse than no load, because
  it looks fine on a dashboard.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


class QualityGateFailed(RuntimeError):
    """Reject rate above the configured threshold. Nothing was loaded."""


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


def parse_timestamp(value) -> date:
    """Wikimedia sends daily timestamps as YYYYMMDD00."""
    text = str(value)
    if len(text) != 10 or not text.isdigit():
        raise ValueError(f"expected 10-digit YYYYMMDD00, got {value!r}")
    return datetime.strptime(text[:8], "%Y%m%d").date()


def _date_or_none(item: dict) -> date | None:
    """The row's day, or None when that is exactly what is wrong with it.

    A row rejected by `timestamp_parses` has no day to record, which is why
    `quarantine.view_date` is nullable.
    """
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
    today: date,
) -> tuple[list[CleanRow], list[BadRow]]:
    """Apply every rule to every row. Returns (clean, quarantined)."""
    clean: list[CleanRow] = []
    bad: list[BadRow] = []
    seen: dict[date, int] = {}

    for item in items:
        broken = _first_broken_rule(
            item, article=article, start=start, end=end, today=today, seen=seen
        )
        if broken is not None:
            rule, detail = broken
            bad.append(BadRow(venue_id, article, _date_or_none(item), rule, detail, repr(item)))
            continue

        view_date = parse_timestamp(item["timestamp"])
        seen[view_date] = item["views"]
        clean.append(CleanRow(venue_id, article, view_date, item["views"]))

    return clean, bad


def _first_broken_rule(
    item: dict,
    *,
    article: str,
    start: date,
    end: date,
    today: date,
    seen: dict[date, int],
) -> tuple[str, str] | None:
    """Return (rule, detail) for the first rule this row breaks, or None if it is clean.

    Rules are ordered cheapest and most fundamental first, so the reported rule is
    the root cause rather than a downstream symptom.
    """
    if item["article"] != article:
        return "article_matches_request", f"got {item['article']!r}, asked for {article!r}"

    try:
        view_date = parse_timestamp(item["timestamp"])
    except ValueError as exc:
        return "timestamp_parses", str(exc)

    if not (start <= view_date <= end):
        return "date_in_requested_window", f"{view_date} outside {start}..{end}"

    if view_date > today:
        return "date_not_in_future", f"{view_date} is after {today}"

    views = item["views"]
    if isinstance(views, bool) or not isinstance(views, int):
        return "views_is_integer", f"got {type(views).__name__} {views!r}"

    if views < 0:
        return "views_non_negative", f"got {views}"

    if view_date in seen:
        return "one_row_per_date", f"{view_date} already seen with {seen[view_date]} views"

    return None


def reject_rate(fetched: int, quarantined: int) -> float:
    """The run's reject rate. Separate from the gate so it can be recorded first.

    `run_log.reject_rate` is the column you reach for when a run has gone wrong,
    so it must be written before the gate is allowed to raise. Deriving it from
    the gate's return value meant a failed run logged 0.0 - zero in the one case
    the number was worth having.
    """
    if fetched == 0:
        return 0.0
    return quarantined / fetched


def enforce_gate(fetched: int, quarantined: int, max_reject_rate: float) -> float:
    """Raise if too much of the run was rejected. Returns the rate when it passes."""
    rate = reject_rate(fetched, quarantined)
    if rate > max_reject_rate:
        raise QualityGateFailed(
            f"rejected {quarantined}/{fetched} rows ({rate:.2%}), "
            f"above the {max_reject_rate:.2%} threshold. Nothing was loaded."
        )
    return rate
