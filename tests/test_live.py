"""Network smoke test, enabled explicitly with RUN_LIVE=1 pytest -m live.

Needs a contact for the User-Agent: set NZAP_CONTACT to an email address or URL.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

from nz_attraction_pageviews import client


@pytest.fixture
def contact():
    """Overrides conftest's placeholder contact: a live request carries a real one."""
    if not os.environ.get(client.CONTACT_ENV):
        pytest.skip(f"set {client.CONTACT_ENV} so Wikimedia can reach whoever runs this")


@pytest.mark.live
@pytest.mark.skipif(os.getenv("RUN_LIVE") != "1", reason="Set RUN_LIVE=1 to contact Wikimedia")
def test_live_pageviews(contact):
    # UTC, like the pipeline: a local date can be a day ahead of the API's.
    end = datetime.now(timezone.utc).date() - timedelta(days=3)
    result = client.fetch_window("Milford_Sound", end - timedelta(days=6), end)
    assert result
    assert all("views" in row for row in result)
