"""Where each venue's range starts, and how far its watermark may move.

Pure functions of dates and rows, with no warehouse in sight, so the rules the
whole pipeline rests on can be tested one case at a time.
"""

from __future__ import annotations

from datetime import date, timedelta

from . import quality


def plan_windows(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    """Split an inclusive range into windows of at most chunk_days."""
    if chunk_days < 1:
        raise ValueError("chunk_days must be at least 1")
    windows = []
    cursor = start
    while cursor <= end:
        stop = min(cursor + timedelta(days=chunk_days - 1), end)
        windows.append((cursor, stop))
        cursor = stop + timedelta(days=1)
    return windows


def start_date(
    watermark: date | None,
    watermark_article: str | None,
    has_rows: bool,
    article: str,
    end: date,
    backfill_days: int,
) -> date:
    """Resume the day after the watermark, or backfill on first sight of a venue.

    Two watermarks are not a record of coverage for this venue, and the whole
    backfill window is asked for again:

    - One for a venue that has never produced a row. The frontier is a property
      of the API, so healthy siblings walk a dead venue's watermark along the
      trust line; a typo corrected in venues.csv would otherwise recover only
      the days since.
    - One recorded for a different title. Coverage of a redirect says nothing
      about the canonical article that replaced it.
    """
    backfill = end - timedelta(days=backfill_days - 1)
    if watermark is None:
        return backfill
    retitled = watermark_article is not None and watermark_article != article
    if retitled or not has_rows:
        return min(watermark + timedelta(days=1), backfill)
    return watermark + timedelta(days=1)


def venue_frontier(
    start: date,
    end: date,
    venue_clean: list[quality.CleanRow],
    venue_bad: list[quality.BadRow],
    trusted_end: date | None,
    accepted: set[date] | None = None,
    accepted_null: set[tuple[str, date | None]] | None = None,
) -> date | None:
    """How far this venue may advance. None means leave the watermark alone.

    The watermark promises that every day up to it has been dealt with, so it may
    not step over a day we failed to load.

    - A rejected day inside the requested range is a hard stop, whatever its
      age, unless an operator has accepted it. A rejected day outside the range
      is recorded but does not stop anything: the watermark passed it runs ago.
    - A rejected row with no parseable day stops the watermark before the
      window it arrived in, since any day of that window could be the one it
      was for - unless an operator has accepted that rule for that window.
    - An absent day before one that arrived is settled: publication runs in date
      order. (`client.fetch_window` has already re-asked such holes narrowly.)
    - An absent day after the last one that arrived is settled only once it is
      older than `trusted_end`. With no evidence anywhere that the upstream
      has published (`trusted_end` is None), nothing is settled: in that case
      no venue loaded anything either, so the watermark stays where it is.
    """
    accepted = accepted or set()
    accepted_null = accepted_null or set()
    stops = []
    for row in venue_bad:
        if row.view_date is None:
            if (row.rule, row.window_start) not in accepted_null:
                stops.append(row.window_start or start)
        elif start <= row.view_date <= end and row.view_date not in accepted:
            stops.append(row.view_date)
    ceiling = min(stops) - timedelta(days=1) if stops else end

    if trusted_end is None:
        return None
    loaded = [row.view_date for row in venue_clean]
    trusted = min(end, trusted_end)
    frontier = max(max(loaded), trusted) if loaded else trusted
    frontier = min(frontier, ceiling)
    return frontier if frontier >= start else None


def is_accepted(row: quality.BadRow, days: set[date], null_windows: set) -> bool:
    if row.view_date is None:
        return (row.rule, row.window_start) in null_windows
    return row.view_date in days
