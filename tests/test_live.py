"""Network smoke test, enabled explicitly with RUN_LIVE=1 pytest -m live."""

import os
from datetime import date, timedelta

import pytest

from nz_attraction_pageviews import client


@pytest.mark.live
@pytest.mark.skipif(os.getenv("RUN_LIVE") != "1", reason="Set RUN_LIVE=1 to contact Wikimedia")
def test_live_pageviews():
    end = date.today() - timedelta(days=3)
    result = client.fetch_window("Milford_Sound", end - timedelta(days=6), end)
    assert result
    assert all("views" in row for row in result)
