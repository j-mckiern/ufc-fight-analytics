"""Events list and completed-event pages (fights with method/round results)."""

from datetime import datetime

import requests
from bs4 import BeautifulSoup

from ufc.scraping.http import BASE_URL, get_soup

EVENTS_COMPLETED_URL = f"{BASE_URL}/statistics/events/completed?page=all"


def parse_event_date(raw: str) -> str:
    """Convert 'February 21, 2026' -> '2026-02-21'."""
    try:
        return datetime.strptime(raw.strip(), "%B %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
        return raw.strip()


def cell_texts(cell) -> list[str]:
    """Stripped text values from <p> tags inside a <td>."""
    return [p.get_text(strip=True) for p in cell.find_all("p")]


def _id_from_url(url: str) -> str:
    return url.rstrip("/").split("/")[-1]


def parse_events_list(soup: BeautifulSoup) -> list[dict]:
    """Parse an events-list page (completed or upcoming) into event dicts.

    Note: the completed-events page lists the next upcoming event at the top,
    so callers must filter by date before treating entries as completed.
    """
    table = soup.find("table", class_="b-statistics__table-events")
    events = []
    for row in table.find_all("tr", class_="b-statistics__table-row"):
        link = row.find("a", class_="b-link")
        date_span = row.find("span", class_="b-statistics__date")
        if link and date_span:
            url = link["href"].strip()
            events.append({
                "event_id": _id_from_url(url),
                "url": url,
                "name": link.get_text(strip=True),
                "date": parse_event_date(date_span.get_text(strip=True)),
            })
    return events


def scrape_events_list(session: requests.Session) -> list[dict]:
    return parse_events_list(get_soup(EVENTS_COMPLETED_URL, session))


def parse_event_fights(soup: BeautifulSoup, event_date: str) -> list[dict]:
    """Parse a completed event page into fight rows.

    Rows without a result method are skipped (they appear when an event
    hasn't happened yet).
    """
    table = soup.find("table", class_="b-fight-details__table")
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []

    fights: list[dict] = []
    for row in tbody.find_all("tr"):
        fight_url = row.get("data-link", "").strip()
        if not fight_url:
            continue
        cells = row.find_all("td")
        if len(cells) < 10:
            continue

        method_ps = cell_texts(cells[7])
        method = method_ps[0] if method_ps else cells[7].get_text(strip=True)
        if not method:
            continue

        fights.append({
            "fight_id": _id_from_url(fight_url),
            "fight_url": fight_url,
            "event_date": event_date,
            "weight_class": cells[6].get_text(strip=True),
            "method": method,
            "round": cells[8].get_text(strip=True),
        })
    return fights


def scrape_event_fights(
    event_url: str, event_date: str, session: requests.Session
) -> list[dict]:
    return parse_event_fights(get_soup(event_url, session), event_date)
