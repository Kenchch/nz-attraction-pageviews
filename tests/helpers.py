"""Stubs shared by the ingest tests. Fetching is stubbed, so they run offline."""

from __future__ import annotations

from datetime import date, timedelta

from nz_attraction_pageviews import ingest

TODAY = date(2026, 3, 1)
VENUES = [
    ingest.Venue("milford-sound", "Milford Sound", "Fiordland", "Milford_Sound"),
    # The canonical title. `Museum_of_New_Zealand_Te_Papa_Tongarewa` is a
    # redirect to it, and redirects are what venues.csv must not use.
    ingest.Venue("te-papa", "Te Papa", "Wellington", "Te_Papa"),
]


def row(article, day, views=50):
    return {
        "project": "en.wikipedia",
        "article": article,
        "granularity": "daily",
        "timestamp": f"{day:%Y%m%d}00",
        "access": "all-access",
        "agent": "user",
        "views": views,
    }


class Recorder:
    """Stub fetcher. Returns one row per day and remembers what it was asked for."""

    def __init__(self, views=50):
        self.views = views
        self.calls = []

    def __call__(self, article, start, end):
        self.calls.append((article, start, end))
        rows, cursor = [], start
        while cursor <= end:
            rows.append(row(article, cursor, self.views))
            cursor += timedelta(days=1)
        return rows

    @property
    def days_requested(self):
        return sum((end - start).days + 1 for _, start, end in self.calls)
