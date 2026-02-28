#!/usr/bin/env python3
"""
UFC fight data scraper.

Scrapes fight data from ufcstats.com and produces:
  - data/fights.csv       (fight_id, event_date, weight_class, method, round)
  - data/fight_stats.csv  (fight_id, fighter_id, result, sig_strikes, sig_attempted,
                            td, td_attempted, sub_attempts, control_time)

"""

import csv
import re
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://ufcstats.com"
EVENTS_URL = f"{BASE_URL}/statistics/events/completed?page=all"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / datetime.now().strftime("%Y-%m-%d")
MAX_WORKERS = 10
MAX_RETRIES = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_soup(url: str, session: requests.Session) -> BeautifulSoup:
    """Fetch a URL and return a parsed BeautifulSoup object.

    Retries with exponential backoff on 429 (Too Many Requests).
    """
    for attempt in range(MAX_RETRIES):
        resp = session.get(url, timeout=30)
        if resp.status_code == 429:
            wait = 2 ** attempt  # 1, 2, 4, 8, 16 seconds
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    # Final attempt — let it raise if still 429
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


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
    """Parse 'X of Y' → (landed, attempted).  Falls back to (0, 0)."""
    text = text.strip()
    m = re.match(r"(\d+)\s+of\s+(\d+)", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    if text.isdigit():
        return int(text), 0
    return 0, 0


def parse_event_date(raw: str) -> str:
    """Convert 'February 21, 2026' → '2026-02-21'."""
    try:
        return datetime.strptime(raw.strip(), "%B %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
        return raw.strip()


def cell_texts(cell) -> list[str]:
    """Return a list of stripped text values from <p> tags inside a <td>."""
    return [p.get_text(strip=True) for p in cell.find_all("p")]


# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------

def scrape_events_list(session: requests.Session) -> list[dict]:
    """Scrape the completed-events page.

    Returns a list of ``{"url": str, "date": str}`` dicts (newest first).
    """
    soup = get_soup(EVENTS_URL, session)
    table = soup.find("table", class_="b-statistics__table-events")
    rows = table.find_all("tr", class_="b-statistics__table-row")

    events = []
    for row in rows:
        link = row.find("a", class_="b-link")
        date_span = row.find("span", class_="b-statistics__date")
        if link and date_span:
            events.append({
                "url": link["href"].strip(),
                "date": parse_event_date(date_span.get_text(strip=True)),
            })
    return events


def scrape_event_page(
    event_url: str,
    event_date: str,
    session: requests.Session,
) -> list[dict]:
    """Scrape a single event page.

    Returns a list of fight dicts with keys:
        fight_id, fight_url, event_date, weight_class, method, round
    """
    soup = get_soup(event_url, session)
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

        fight_id = fight_url.rstrip("/").split("/")[-1]
        cells = row.find_all("td")
        if len(cells) < 10:
            continue

        # Weight class — cell 6
        weight_class = cells[6].get_text(strip=True)

        # Method — cell 7 has two <p>: method and detail; we keep only method
        method_ps = cell_texts(cells[7])
        method = method_ps[0] if method_ps else cells[7].get_text(strip=True)

        # Round — cell 8
        fight_round = cells[8].get_text(strip=True)

        fights.append({
            "fight_id": fight_id,
            "fight_url": fight_url,
            "event_date": event_date,
            "weight_class": weight_class,
            "method": method,
            "round": fight_round,
        })

    return fights


def scrape_fight_detail(
    fight_url: str,
    session: requests.Session,
) -> list[dict]:
    """Scrape a single fight-detail page.

    Returns two dicts (one per fighter) with keys:
        fight_id, fighter_id, result, sig_strikes, sig_attempted,
        td, td_attempted, sub_attempts, control_time
    """
    soup = get_soup(fight_url, session)
    fight_id = fight_url.rstrip("/").split("/")[-1]

    # --- Fighter identities & results (W / L / D / NC) ---
    persons = soup.find_all("div", class_="b-fight-details__person")
    fighters: list[dict] = []
    for p in persons:
        link = (
            p.find("a", class_="b-fight-details__person-link")
            or p.find("a", class_="b-link")
        )
        status_el = p.find("i", class_="b-fight-details__person-status")
        if link:
            fighter_id = link["href"].strip().rstrip("/").split("/")[-1]
            result = status_el.get_text(strip=True) if status_el else ""
            fighters.append({"fighter_id": fighter_id, "result": result})

    if len(fighters) != 2:
        return []

    # --- Totals table (first <table> on the page, a.k.a. Table 0) ---
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
        # Sig. strikes (cell 2): "X of Y"
        sig_landed, sig_attempted = parse_x_of_y(
            cell_texts(cells[2])[idx] if len(cell_texts(cells[2])) > idx else ""
        )

        # Takedowns (cell 5): "X of Y"
        td_landed, td_attempted = parse_x_of_y(
            cell_texts(cells[5])[idx] if len(cell_texts(cells[5])) > idx else ""
        )

        # Sub attempts (cell 7): plain number
        sub_text = (
            cell_texts(cells[7])[idx] if len(cell_texts(cells[7])) > idx else "0"
        )
        sub_attempts = int(sub_text) if sub_text.isdigit() else 0

        # Control time (cell 9): "M:SS"
        ctrl_text = (
            cell_texts(cells[9])[idx] if len(cell_texts(cells[9])) > idx else "0:00"
        )
        control_time = parse_control_time(ctrl_text)

        stats.append({
            "fight_id": fight_id,
            "fighter_id": fighters[idx]["fighter_id"],
            "result": fighters[idx]["result"],
            "sig_strikes": sig_landed,
            "sig_attempted": sig_attempted,
            "td": td_landed,
            "td_attempted": td_attempted,
            "sub_attempts": sub_attempts,
            "control_time": control_time,
        })

    return stats


# ---------------------------------------------------------------------------
# Persistence helpers (support resuming interrupted runs)
# ---------------------------------------------------------------------------

FIGHTS_FIELDS = ["fight_id", "event_date", "weight_class", "method", "round"]
STATS_FIELDS = [
    "fight_id", "fighter_id", "result", "sig_strikes", "sig_attempted",
    "td", "td_attempted", "sub_attempts", "control_time",
]


def load_existing_ids(filepath: Path, id_column: str) -> set[str]:
    """Return the set of IDs already written to *filepath*."""
    ids: set[str] = set()
    if filepath.exists():
        with open(filepath, newline="") as f:
            for row in csv.DictReader(f):
                ids.add(row[id_column])
    return ids


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fights_path = DATA_DIR / "fights.csv"
    stats_path = DATA_DIR / "fight_stats.csv"

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; UFC-Stats-Scraper/1.0)",
    })

    # ── Phase 1: events → fights.csv ──────────────────────────────────────
    print("Fetching events list...")
    events = scrape_events_list(session)
    print(f"Found {len(events)} completed events.")

    existing_fight_ids = load_existing_ids(fights_path, "fight_id")
    fights_existed = fights_path.exists() and fights_path.stat().st_size > 0

    all_fight_rows: list[dict] = []  # full fight dicts for CSV

    def _scrape_event(event: dict) -> list[dict]:
        try:
            return scrape_event_page(event["url"], event["date"], session)
        except Exception as exc:
            print(f"\n    [!] Error on event {event['url']}: {exc}")
            return []

    print(f"  Scraping event pages ({MAX_WORKERS} workers)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_scrape_event, event): event
            for event in events
        }
        done = 0
        for future in as_completed(futures):
            done += 1
            print(f"\r  Event {done}/{len(events)}", end="", flush=True)
            all_fight_rows.extend(future.result())

    # Deduplicate and write new fights
    new_rows = [f for f in all_fight_rows if f["fight_id"] not in existing_fight_ids]
    with open(fights_path, "a" if fights_existed else "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIGHTS_FIELDS)
        if not fights_existed:
            writer.writeheader()
        for fight in new_rows:
            writer.writerow({k: fight[k] for k in FIGHTS_FIELDS})
            existing_fight_ids.add(fight["fight_id"])

    print(f"\n  Done. fights.csv - {len(existing_fight_ids)} fights total")

    # ── Phase 2: fight details → fight_stats.csv ─────────────────────────
    existing_stat_ids = load_existing_ids(stats_path, "fight_id")
    stats_existed = stats_path.exists() and stats_path.stat().st_size > 0

    to_scrape = [
        f for f in all_fight_rows
        if f["fight_id"] not in existing_stat_ids
    ]
    print(f"\nScraping {len(to_scrape)} fight-detail pages "
          f"({len(existing_stat_ids)} already done)...")

    all_stat_rows: list[dict] = []

    def _scrape_fight(fight: dict) -> list[dict]:
        try:
            return scrape_fight_detail(fight["fight_url"], session)
        except Exception as exc:
            print(f"\n    [!] Error on fight {fight['fight_id']}: {exc}")
            return []

    print(f"  Scraping fight pages ({MAX_WORKERS} workers)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_scrape_fight, fight): fight
            for fight in to_scrape
        }
        done = 0
        for future in as_completed(futures):
            done += 1
            print(f"\r  Fight {done}/{len(to_scrape)}", end="", flush=True)
            all_stat_rows.extend(future.result())

    # Write all new stats at once
    with open(stats_path, "a" if stats_existed else "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STATS_FIELDS)
        if not stats_existed:
            writer.writeheader()
        writer.writerows(all_stat_rows)

    total_stats = load_existing_ids(stats_path, "fight_id")
    print(f"\n  Done. fight_stats.csv - {len(total_stats)} fights total")
    print(f"\nDone. Files saved to {DATA_DIR}/")


if __name__ == "__main__":
    main()
