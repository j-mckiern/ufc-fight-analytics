"""Odds-feed parsing tests — a mocked GraphQL session, no live network."""

import json
from pathlib import Path

from ufc.scraping.odds import fetch_event_odds, fetch_upcoming_odds_events

FIXTURES = Path(__file__).parent / "fixtures"


class _FakeResponse:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return {"data": self._data}


class _FakeSession:
    """Returns a canned GraphQL payload regardless of the query sent."""

    def __init__(self, fixture: str):
        self._data = json.loads((FIXTURES / fixture).read_text())

    def post(self, url, json=None, headers=None, timeout=None):
        return _FakeResponse(self._data)


def test_fetch_upcoming_odds_events_filters_to_ufc():
    events = fetch_upcoming_odds_events(_FakeSession("odds_events.json"), "2026-07-08")
    # The non-UFC (FNG) event is filtered out by default.
    assert [e["pk"] for e in events] == [8996, 9103]
    assert all(set(e) == {"pk", "name", "date", "slug"} for e in events)


def test_fetch_upcoming_odds_events_promotion_none_keeps_all():
    events = fetch_upcoming_odds_events(
        _FakeSession("odds_events.json"), "2026-07-08", promotion=None
    )
    assert len(events) == 3


def test_fetch_event_odds_shape():
    odds = fetch_event_odds(_FakeSession("odds_event.json"), 8996)

    assert [b["slug"] for b in odds["sportsbooks"]] == [
        "draftkings", "fanduel", "pinnacle"
    ]
    assert len(odds["fights"]) == 3

    mcg = odds["fights"][0]
    assert mcg["fighter1_name"] == "Conor McGregor"
    assert mcg["fighter2_name"] == "Max Holloway"
    assert mcg["best_odds1"] == 195
    assert mcg["is_cancelled"] is False
    assert mcg["offers"]["draftkings"] == {"odds1": 180, "odds2": -220}
    # A null outcome is preserved as None (Pinnacle has no price for side 2).
    assert mcg["offers"]["pinnacle"] == {"odds1": 188, "odds2": None}


def test_fetch_event_odds_cancelled_and_missing_book():
    odds = fetch_event_odds(_FakeSession("odds_event.json"), 8996)
    cancelled = odds["fights"][2]
    assert cancelled["is_cancelled"] is True
    assert cancelled["offers"] == {}

    # The second fight only has a DraftKings line — FanDuel is absent.
    saint_denis = odds["fights"][1]
    assert set(saint_denis["offers"]) == {"draftkings"}
