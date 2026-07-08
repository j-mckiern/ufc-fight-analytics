"""Fighter profile pages: scraping, cleaning (dob retained), and upsert persistence."""

import datetime
import logging
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from ufc.scraping.http import BASE_URL, get_soup

logger = logging.getLogger(__name__)

FIGHTER_FIELDS = [
    "fighter_id", "name", "nickname", "wins", "losses", "ties",
    "height_in", "weight_lb", "reach_in", "stance", "dob", "age",
    "slpm", "str_acc_dec", "sapm", "str_def_dec",
    "td_avg", "td_acc_dec", "td_def_dec", "sub_avg",
]


def parse_fighter_profile(soup: BeautifulSoup, fighter_id: str) -> dict:
    """Extract the raw label/value stats from a fighter profile page."""
    name_elem = soup.find("span", class_="b-content__title-highlight")
    record_elem = soup.find("span", class_="b-content__title-record")
    nickname_elem = soup.find("p", class_="b-content__Nickname")

    if not name_elem or not record_elem:
        return {}

    stats = {
        "fighter_id": fighter_id,
        "name": name_elem.get_text(strip=True),
        "record": record_elem.get_text(strip=True).replace("Record: ", ""),
        "nickname": nickname_elem.get_text(strip=True) if nickname_elem else "",
    }
    for item in soup.find_all("li", class_="b-list__box-list-item"):
        label = item.find("i")
        if not label:
            continue
        key = label.get_text(strip=True).replace(":", "").strip()
        value = item.get_text(strip=True).replace(label.get_text(strip=True), "").strip()
        if key:
            stats[key] = value
    return stats


def _parse_percentage(value):
    if value is None or value == "--":
        return None
    try:
        return float(value.replace("%", "")) / 100
    except (ValueError, AttributeError):
        return None


def _parse_float(value):
    if value is None or value == "--":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_int(value):
    if value is None or value == "--":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_height(value):
    if value is None or value == "--":
        return None
    try:
        value = value.strip().replace('"', "")
        parts = value.split("'")
        feet = int(parts[0])
        inches = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else 0
        return feet * 12 + inches
    except (ValueError, IndexError, AttributeError):
        return None


def _parse_weight(value):
    if value is None or value == "--":
        return None
    try:
        return int(float(value.replace("lbs.", "").strip()))
    except (ValueError, AttributeError):
        return None


def _parse_reach(value):
    if value is None or value == "--":
        return None
    try:
        return int(float(value.replace('"', "").strip()))
    except (ValueError, AttributeError):
        return None


def clean_fighter_stats(raw: dict, today: datetime.date | None = None) -> dict:
    """Normalize a raw profile dict into the fighters.csv schema.

    Keeps dob (ISO date) so features can compute age at event date; age is
    still stored as a fallback for display and for rows scraped before dob
    existed.
    """
    today = today or datetime.date.today()

    record = raw.get("record", "--").replace("Record: ", "")
    wins, losses, ties = (record.split("-") + ["0", "0", "0"])[:3]

    dob = None
    age = None
    dob_str = raw.get("DOB", "")
    if dob_str and dob_str != "--":
        try:
            parsed = datetime.datetime.strptime(dob_str, "%b %d, %Y").date()
            dob = parsed.isoformat()
            age = (
                today.year
                - parsed.year
                - ((today.month, today.day) < (parsed.month, parsed.day))
            )
        except (ValueError, TypeError):
            pass

    return {
        "fighter_id": raw.get("fighter_id"),
        "name": raw.get("name", "").strip() if raw.get("name") else None,
        "nickname": raw.get("nickname") or None,
        "wins": _parse_int(wins),
        "losses": _parse_int(losses),
        "ties": _parse_int(ties),
        "height_in": _parse_height(raw.get("Height")),
        "weight_lb": _parse_weight(raw.get("Weight")),
        "reach_in": _parse_reach(raw.get("Reach")),
        "stance": raw.get("STANCE") or None,
        "dob": dob,
        "age": age,
        "slpm": _parse_float(raw.get("SLpM")),
        "str_acc_dec": _parse_percentage(raw.get("Str. Acc.")),
        "sapm": _parse_float(raw.get("SApM")),
        "str_def_dec": _parse_percentage(raw.get("Str. Def")),
        "td_avg": _parse_float(raw.get("TD Avg.")),
        "td_acc_dec": _parse_percentage(raw.get("TD Acc.")),
        "td_def_dec": _parse_percentage(raw.get("TD Def.")),
        "sub_avg": _parse_float(raw.get("Sub. Avg.")),
    }


def scrape_fighter(fighter_id: str, session: requests.Session) -> dict:
    """Scrape and clean one fighter profile. Returns {} on failure."""
    url = f"{BASE_URL}/fighter-details/{fighter_id}"
    try:
        raw = parse_fighter_profile(get_soup(url, session), fighter_id)
    except requests.RequestException as exc:
        logger.warning("Fighter %s: %s", fighter_id, exc)
        return {}
    if not raw:
        logger.warning("Fighter %s: profile missing required elements", fighter_id)
        return {}
    return clean_fighter_stats(raw)


def upsert_fighters(records: list[dict], fighters_path: Path) -> pd.DataFrame:
    """Insert-or-update fighter rows by fighter_id, rewriting fighters.csv.

    Unlike the old append-only save, this refreshes records/career stats for
    fighters who fought again, and adds the dob column to legacy files.
    """
    new = pd.DataFrame(records, columns=FIGHTER_FIELDS)
    if fighters_path.exists():
        existing = pd.read_csv(fighters_path)
        for col in FIGHTER_FIELDS:
            if col not in existing.columns:
                existing[col] = None
        existing = existing[FIGHTER_FIELDS]
        keep = existing[~existing["fighter_id"].isin(set(new["fighter_id"]))]
        combined = pd.concat([keep, new], ignore_index=True)
    else:
        combined = new

    tmp_path = fighters_path.with_suffix(".csv.tmp")
    combined.to_csv(tmp_path, index=False)
    tmp_path.replace(fighters_path)
    return combined
