"""Upcoming events and their fight cards (no results yet, just matchups)."""

import requests
from bs4 import BeautifulSoup

from ufc.scraping.events import parse_events_list
from ufc.scraping.http import BASE_URL, get_soup

EVENTS_UPCOMING_URL = f"{BASE_URL}/statistics/events/upcoming?page=all"


def scrape_upcoming_events(session: requests.Session) -> list[dict]:
    """Upcoming events, soonest first: {event_id, url, name, date}."""
    return parse_events_list(get_soup(EVENTS_UPCOMING_URL, session))


def parse_upcoming_card(soup: BeautifulSoup) -> list[dict]:
    """Parse an upcoming event page into bout dicts.

    Each bout: fight_id, fighter_a_id/name, fighter_b_id/name, weight_class.
    Fighter order matches the page (first listed is A).
    """
    table = soup.find("table", class_="b-fight-details__table")
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []

    bouts: list[dict] = []
    for row in tbody.find_all("tr"):
        fight_url = row.get("data-link", "").strip()
        if not fight_url:
            continue
        cells = row.find_all("td")
        if len(cells) < 7:
            continue

        fighter_links = cells[1].find_all("a", href=True)
        if len(fighter_links) != 2:
            continue

        def _fighter(link) -> tuple[str, str]:
            return (
                link["href"].strip().rstrip("/").split("/")[-1],
                link.get_text(strip=True),
            )

        a_id, a_name = _fighter(fighter_links[0])
        b_id, b_name = _fighter(fighter_links[1])
        weight_ps = cells[6].find_all("p")
        weight_class = (
            weight_ps[0].get_text(strip=True) if weight_ps else
            cells[6].get_text(strip=True)
        )

        bouts.append({
            "fight_id": fight_url.rstrip("/").split("/")[-1],
            "fighter_a_id": a_id,
            "fighter_a_name": a_name,
            "fighter_b_id": b_id,
            "fighter_b_name": b_name,
            "weight_class": weight_class,
        })
    return bouts


def scrape_upcoming_card(event_url: str, session: requests.Session) -> list[dict]:
    return parse_upcoming_card(get_soup(event_url, session))
