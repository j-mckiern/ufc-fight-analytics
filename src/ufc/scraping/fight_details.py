"""Fight-detail pages: per-fighter totals (strikes, takedowns, control time)."""

import re

import requests
from bs4 import BeautifulSoup

from ufc.scraping.http import get_soup


def parse_control_time(time_str: str) -> int:
    """Convert a control-time string like '4:32' to total seconds (272)."""
    time_str = time_str.strip()
    if not time_str or time_str in ("--", "---"):
        return 0
    parts = time_str.split(":")
    if len(parts) == 2:
        try:
            return int(parts[0]) * 60 + int(parts[1])
        except ValueError:
            return 0
    return 0


def parse_x_of_y(text: str) -> tuple[int, int]:
    """Parse 'X of Y' -> (landed, attempted). Falls back to (0, 0)."""
    text = text.strip()
    m = re.match(r"(\d+)\s+of\s+(\d+)", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    if text.isdigit():
        return int(text), 0
    return 0, 0


def _cell_texts(cell) -> list[str]:
    return [p.get_text(strip=True) for p in cell.find_all("p")]


def parse_fight_detail(soup: BeautifulSoup, fight_id: str) -> list[dict]:
    """Parse a fight-detail page into two per-fighter stat rows."""
    persons = soup.find_all("div", class_="b-fight-details__person")
    fighters: list[dict] = []
    for p in persons:
        link = p.find("a", class_="b-fight-details__person-link") or p.find(
            "a", class_="b-link"
        )
        status_el = p.find("i", class_="b-fight-details__person-status")
        if link:
            fighter_id = link["href"].strip().rstrip("/").split("/")[-1]
            result = status_el.get_text(strip=True) if status_el else ""
            fighters.append({"fighter_id": fighter_id, "result": result})

    if len(fighters) != 2:
        return []

    tables = soup.find_all("table")
    if not tables:
        return []
    tbody = tables[0].find("tbody")
    if not tbody:
        return []
    row = tbody.find("tr")
    if not row:
        return []
    cells = row.find_all("td")
    if len(cells) < 10:
        return []

    stats: list[dict] = []
    for idx in range(2):
        sig_landed, sig_attempted = parse_x_of_y(
            _cell_texts(cells[2])[idx] if len(_cell_texts(cells[2])) > idx else ""
        )
        td_landed, td_attempted = parse_x_of_y(
            _cell_texts(cells[5])[idx] if len(_cell_texts(cells[5])) > idx else ""
        )
        sub_text = _cell_texts(cells[7])[idx] if len(_cell_texts(cells[7])) > idx else "0"
        ctrl_text = (
            _cell_texts(cells[9])[idx] if len(_cell_texts(cells[9])) > idx else "0:00"
        )
        stats.append({
            "fight_id": fight_id,
            "fighter_id": fighters[idx]["fighter_id"],
            "result": fighters[idx]["result"],
            "sig_strikes": sig_landed,
            "sig_attempted": sig_attempted,
            "td": td_landed,
            "td_attempted": td_attempted,
            "sub_attempts": int(sub_text) if sub_text.isdigit() else 0,
            "control_time": parse_control_time(ctrl_text),
        })
    return stats


def scrape_fight_detail(fight_url: str, session: requests.Session) -> list[dict]:
    fight_id = fight_url.rstrip("/").split("/")[-1]
    return parse_fight_detail(get_soup(fight_url, session), fight_id)
