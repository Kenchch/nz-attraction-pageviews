import pytest

from nz_attraction_pageviews import client


@pytest.fixture(autouse=True)
def contact(monkeypatch):
    """A contact for the User-Agent, which live requests refuse to go without.

    Nothing here reaches the network - every test stubs urlopen or the opener -
    but http_get builds the header before it gets that far.
    """
    monkeypatch.setenv(client.CONTACT_ENV, "tests@example.invalid")
