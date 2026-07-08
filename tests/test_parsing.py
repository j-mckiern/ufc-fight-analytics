import datetime
import re
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from ufc.scraping.events import parse_event_date, parse_event_fights, parse_events_list
from ufc.scraping.fight_details import (
    parse_control_time,
    parse_fight_detail,
    parse_x_of_y,
)
from ufc.scraping.fighters import clean_fighter_stats, parse_fighter_profile
from ufc.scraping.upcoming import parse_upcoming_card

FIXTURES = Path(__file__).parent / "fixtures"


def _soup(name: str) -> BeautifulSoup:
    return BeautifulSoup((FIXTURES / name).read_text(), "html.parser")


def test_parse_x_of_y():
    assert parse_x_of_y("22 of 55") == (22, 55)
    assert parse_x_of_y("  3 of 7 ") == (3, 7)
    assert parse_x_of_y("4") == (4, 0)
    assert parse_x_of_y("--") == (0, 0)
    assert parse_x_of_y("") == (0, 0)


def test_parse_control_time():
    assert parse_control_time("4:32") == 272
    assert parse_control_time("0:00") == 0
    assert parse_control_time("--") == 0
    assert parse_control_time("") == 0
    assert parse_control_time("garbage") == 0


def test_parse_event_date():
    assert parse_event_date("February 21, 2026") == "2026-02-21"
    assert parse_event_date("July 11, 2026 ") == "2026-07-11"
    assert parse_event_date("not a date") == "not a date"


def test_parse_events_list_completed():
    events = parse_events_list(_soup("events_completed.html"))
    assert len(events) >= 3
    for e in events:
        assert re.fullmatch(r"[0-9a-f]{16}", e["event_id"])
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", e["date"])
        assert e["name"]
    # newest first
    assert events[0]["date"] >= events[-1]["date"]


def test_parse_events_list_upcoming():
    events = parse_events_list(_soup("events_upcoming.html"))
    assert len(events) >= 1
    assert any("McGregor" in e["name"] for e in events)


def test_parse_event_fights_completed():
    fights = parse_event_fights(_soup("event_completed.html"), "2026-06-20")
    assert len(fights) >= 10
    for f in fights:
        assert re.fullmatch(r"[0-9a-f]{16}", f["fight_id"])
        assert f["event_date"] == "2026-06-20"
        assert f["method"]  # completed fights always have a result method
        assert f["weight_class"]
        assert f["fight_url"].startswith("http")


def test_parse_event_fights_skips_unfought():
    """An upcoming event page yields no fight rows (empty methods)."""
    assert parse_event_fights(_soup("event_upcoming.html"), "2026-07-11") == []


def test_parse_upcoming_card():
    bouts = parse_upcoming_card(_soup("event_upcoming.html"))
    assert len(bouts) >= 5
    for b in bouts:
        assert re.fullmatch(r"[0-9a-f]{16}", b["fight_id"])
        assert re.fullmatch(r"[0-9a-f]{16}", b["fighter_a_id"])
        assert re.fullmatch(r"[0-9a-f]{16}", b["fighter_b_id"])
        assert b["fighter_a_name"] and b["fighter_b_name"]
        assert b["weight_class"]
    names = {b["fighter_a_name"] for b in bouts} | {b["fighter_b_name"] for b in bouts}
    assert "Conor McGregor" in names and "Max Holloway" in names


def test_parse_fight_detail():
    rows = parse_fight_detail(_soup("fight_detail.html"), "abc123")
    assert len(rows) == 2
    for r in rows:
        assert r["fight_id"] == "abc123"
        assert re.fullmatch(r"[0-9a-f]{16}", r["fighter_id"])
        assert r["result"] in {"W", "L", "D", "NC"}
        for col in ["sig_strikes", "sig_attempted", "td", "td_attempted",
                    "sub_attempts", "control_time"]:
            assert isinstance(r[col], int)
    assert {r["result"] for r in rows} == {"W", "L"}


def test_parse_fight_detail_unfought_returns_empty():
    """A fight page with no totals yet must not produce stat rows."""
    assert parse_fight_detail(_soup("fight_detail_upcoming.html"), "abc123") == []


def test_parse_and_clean_fighter_profile():
    raw = parse_fighter_profile(_soup("fighter_profile.html"), "f4c49976c75c5ab2")
    assert raw["name"] == "Conor McGregor"
    cleaned = clean_fighter_stats(raw, today=datetime.date(2026, 7, 8))
    assert cleaned["fighter_id"] == "f4c49976c75c5ab2"
    assert cleaned["dob"] == "1988-07-14"
    assert cleaned["age"] == 37
    assert cleaned["height_in"] == 69
    assert cleaned["weight_lb"] == 155
    assert cleaned["reach_in"] == 74
    assert cleaned["stance"] == "Southpaw"
    assert cleaned["wins"] is not None and cleaned["wins"] >= 22
    assert 0 <= cleaned["str_acc_dec"] <= 1


def test_clean_fighter_stats_handles_missing():
    cleaned = clean_fighter_stats(
        {"fighter_id": "x", "name": "No Data", "record": "--",
         "Height": "--", "Weight": "--", "Reach": "--", "DOB": "--"},
    )
    assert cleaned["dob"] is None
    assert cleaned["age"] is None
    assert cleaned["height_in"] is None
    assert cleaned["stance"] is None
