#!/usr/bin/env python3
"""
UFC fighter stats scraper.

Scrapes fighter data from ufcstats.com and produces:
  - data/{date}/raw/fighters.csv  (fighter_id, name, nickname, wins, losses, ties,
                                    height_in, weight_lb, reach_in, stance, age,
                                    slpm, str_acc_dec, sapm, str_def_dec,
                                    td_avg, td_acc_dec, td_def_dec, sub_avg)
  - data/{date}/scraper_errors.log  (detailed error logging)
  - data/{date}/failed_fighters.csv (fighters that failed to scrape)
"""

import argparse
import csv
import datetime
import logging
import string
import time
from pathlib import Path
from typing import Optional
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://ufcstats.com"
ALPHABET = string.ascii_lowercase  # "abcdefghijklmnopqrstuvwxyz"
MAX_RETRIES = 3
RETRY_DELAY = 2  # base seconds for exponential backoff
REQUEST_DELAY = 0.25  # seconds between requests per worker

# Global logger
logger: Optional[logging.Logger] = None


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Path) -> logging.Logger:
    """Configure logging to both console and file."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "scraper_errors.log"
    
    log = logging.getLogger("scrape_ufc_stats")
    log.setLevel(logging.DEBUG)
    
    # File handler (detailed)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    ))
    
    # Console handler (warnings and errors only)
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    
    log.addHandler(fh)
    log.addHandler(ch)
    
    return log


# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------

def get_fighter_ids_for_letter(letter: str) -> set[str]:
    """Fetch fighter IDs for a single letter."""
    fighter_ids = set()
    url = f"{BASE_URL}/statistics/fighters?char={letter}&page=all"
    
    try:
        page = requests.get(url, timeout=30)
        if page.status_code != 200:
            logger.warning(f"Letter {letter}: HTTP {page.status_code}")
            return fighter_ids
        
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find("table")
        tbody = table.find("tbody") if table else None
        
        if not tbody:
            logger.debug(f"Letter {letter}: No fighter table found")
            return fighter_ids
        
        for row in tbody.find_all("tr"):
            link = row.find("a", href=True)
            if not link:
                continue
            
            href = link["href"]
            fighter_id = href.rstrip("/").split("/")[-1]
            fighter_ids.add(fighter_id)
    except Exception as e:
        logger.error(f"Error fetching fighters for letter {letter}: {e}")
    
    return fighter_ids


def get_fighter_ids() -> set[str]:
    """Discover fighter IDs via A-Z search (legacy mode)."""
    fighter_ids = set()
    
    print("Fetching fighter IDs (10 workers)...")
    logger.info("Starting A-Z fighter discovery")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_fighter_ids_for_letter, letter): letter for letter in ALPHABET}
        done = 0
        for future in as_completed(futures):
            done += 1
            print(f"\r  Letter {done}/{len(ALPHABET)}", end="", flush=True)
            fighter_ids.update(future.result())
    
    print(f"\n  Done. Found {len(fighter_ids)} unique fighter IDs.")
    logger.info(f"A-Z discovery complete: {len(fighter_ids)} fighters")
    return fighter_ids


def load_fighter_ids_from_file(filepath: Path) -> set[str]:
    """Load fighter IDs from a CSV file (targeted scraping mode)."""
    fighter_ids = set()
    try:
        with open(filepath, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'fighter_id' in row:
                    fighter_ids.add(row['fighter_id'])
        logger.info(f"Loaded {len(fighter_ids)} fighter IDs from {filepath}")
    except Exception as e:
        logger.error(f"Error loading fighter IDs from {filepath}: {e}")
    return fighter_ids


def get_fighter_stats(fighter_id: str, attempt: int = 1) -> dict:
    """Scrape fighter stats with retry logic."""
    url = f"{BASE_URL}/fighter-details/{fighter_id}"

    try:
        time.sleep(REQUEST_DELAY)
        page = requests.get(url, timeout=30)

        if page.status_code == 404:
            logger.debug(f"Fighter {fighter_id}: Profile not found (HTTP 404)")
            return {}
        if page.status_code == 429:
            if attempt < MAX_RETRIES:
                delay = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Fighter {fighter_id}: HTTP 429, retrying in {delay}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(delay)
                return get_fighter_stats(fighter_id, attempt + 1)
            logger.warning(f"Fighter {fighter_id}: HTTP 429 after {MAX_RETRIES} retries")
            return {}
        if page.status_code != 200:
            logger.warning(f"Fighter {fighter_id}: HTTP {page.status_code}")
            return {}

        soup = BeautifulSoup(page.content, 'html.parser')

        # Name and record
        name_elem = soup.find("span", class_="b-content__title-highlight")
        record_elem = soup.find("span", class_="b-content__title-record")
        nickname_elem = soup.find("p", class_="b-content__Nickname")
        
        if not name_elem or not record_elem:
            logger.debug(f"Fighter {fighter_id}: Missing required elements")
            return {}
        
        name = name_elem.get_text(strip=True)
        record = record_elem.get_text(strip=True).replace("Record: ", "")
        nickname = nickname_elem.get_text(strip=True) if nickname_elem else ""

        # All list items share the same pattern: <i> label </i> value
        stats = {"fighter_id": fighter_id, "name": name, "record": record, "nickname": nickname}

        for item in soup.find_all("li", class_="b-list__box-list-item"):
            label = item.find("i")
            if not label:
                continue
            key = label.get_text(strip=True).replace(":", "").strip()
            value = item.get_text(strip=True).replace(label.get_text(strip=True), "").strip()
            if key:
                stats[key] = value

        logger.debug(f"Fighter {fighter_id}: Successfully scraped")
        return stats

    except requests.Timeout:
        if attempt < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** attempt)
            logger.warning(f"Fighter {fighter_id}: Request timeout, retrying in {delay}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(delay)
            return get_fighter_stats(fighter_id, attempt + 1)
        logger.warning(f"Fighter {fighter_id}: Request timeout after {MAX_RETRIES} retries")
        return {}
    except Exception as e:
        logger.error(f"Fighter {fighter_id}: {type(e).__name__}: {e}")
        return {}



# ---------------------------------------------------------------------------
# Data cleaning
# ---------------------------------------------------------------------------

def clean_fighter_stats(raw: dict) -> dict:
    # Helper function to convert percentages "50%" → 0.50
    def parse_percentage(value):
        if value is None or value == "--":
            return None
        try:
            return float(value.replace("%", "")) / 100
        except (ValueError, AttributeError):
            return None

    # Helper function to convert floats
    def parse_float(value):
        if value is None or value == "--":
            return None
        try:
            return float(value)
        except (ValueError, AttributeError):
            return None

    # Helper function to convert integers
    def parse_int(value):
        if value is None or value == "--":
            return None
        try:
            return int(value)
        except (ValueError, AttributeError):
            return None

    # Helper function to convert height "5' 7\"" to inches
    def parse_height(value):
        if value is None or value == "--":
            return None
        try:
            value = value.strip().replace('"', '')
            parts = value.split("'")
            feet = int(parts[0])
            inches = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else 0
            return feet * 12 + inches
        except (ValueError, IndexError, AttributeError):
            return None

    # Helper function to convert weight "155 lbs." to pounds
    def parse_weight(value):
        if value is None or value == "--":
            return None
        try:
            return int(float(value.replace("lbs.", "").strip()))
        except (ValueError, AttributeError):
            return None

    # Helper function to convert reach "72\"" to inches
    def parse_reach(value):
        if value is None or value == "--":
            return None
        try:
            return int(float(value.replace('"', '').strip()))
        except (ValueError, AttributeError):
            return None

    # Split record "5-3-0" into wins, losses, ties
    record = raw.get("record", "--").replace("Record: ", "")
    wins, losses, ties = (record.split("-") + ["0", "0", "0"])[:3]
    wins = parse_int(wins)
    losses = parse_int(losses)
    ties = parse_int(ties)

    # Calculate age from DOB
    dob_str = raw.get("DOB", "")
    age = None
    if dob_str and dob_str != "--":
        try:
            dob = datetime.datetime.strptime(dob_str, "%b %d, %Y").date()
            today = datetime.date.today()
            age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        except (ValueError, TypeError):
            age = None

    return {
        "fighter_id": raw.get("fighter_id"),
        "name": raw.get("name", "").strip() if raw.get("name") else None,
        "nickname": raw.get("nickname") or None,
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "height_in": parse_height(raw.get("Height")),
        "weight_lb": parse_weight(raw.get("Weight")),
        "reach_in": parse_reach(raw.get("Reach")),
        "stance": raw.get("STANCE") or None,
        "age": age,
        "slpm": parse_float(raw.get("SLpM")),
        "str_acc_dec": parse_percentage(raw.get("Str. Acc.")),
        "sapm": parse_float(raw.get("SApM")),
        "str_def_dec": parse_percentage(raw.get("Str. Def")),
        "td_avg": parse_float(raw.get("TD Avg.")),
        "td_acc_dec": parse_percentage(raw.get("TD Acc.")),
        "td_def_dec": parse_percentage(raw.get("TD Def.")),
        "sub_avg": parse_float(raw.get("Sub. Avg.")),
    }


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load_existing_fighters(filepath: Path) -> set[str]:
    """Load already-scraped fighter IDs from fighters.csv."""
    existing = set()
    if filepath.exists():
        try:
            with open(filepath, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing.add(row['fighter_id'])
            logger.debug(f"Loaded {len(existing)} existing fighters from {filepath}")
        except Exception as e:
            logger.error(f"Error loading existing fighters: {e}")
    return existing


def save_to_csv(fighters: list[dict], out_dir: Path):
    """Append fighters to fighters.csv (supports resuming mid-scrape)."""
    if not fighters:
        return

    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    filename = raw_dir / "fighters.csv"
    file_exists = filename.exists()

    try:
        with open(filename, "a" if file_exists else "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fighters[0].keys())
            if not file_exists:
                writer.writeheader()
                logger.debug(f"Created {filename}")
            writer.writerows(fighters)
        logger.info(f"Saved {len(fighters)} fighters to {filename}")
    except Exception as e:
        logger.error(f"Error saving fighters to CSV: {e}")


def save_failed_fighters(failed: dict[str, tuple[str, int]], out_dir: Path):
    """Save failed scrape attempts for manual inspection and retry."""
    if not failed:
        return
    
    filename = out_dir / "failed_fighters.csv"
    try:
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["fighter_id", "error_message", "attempt_count"])
            writer.writeheader()
            for fighter_id, (error_msg, attempts) in sorted(failed.items()):
                writer.writerow({
                    "fighter_id": fighter_id,
                    "error_message": error_msg,
                    "attempt_count": attempts,
                })
        logger.info(f"Saved {len(failed)} failed fighters to {filename}")
    except Exception as e:
        logger.error(f"Error saving failed fighters: {e}")



# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global logger
    
    # Parse CLI arguments
    parser = argparse.ArgumentParser(
        description="Scrape UFC fighter stats from ufcstats.com"
    )
    parser.add_argument(
        "--fighter-ids-file",
        type=Path,
        help="CSV file with fighter_id column to scrape (targeted mode). If not provided, uses A-Z discovery."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.date.today().isoformat(),
        help="Date for output directory (default: today)"
    )
    args = parser.parse_args()
    
    # Setup output directory and logging
    out_dir = Path(__file__).resolve().parent.parent / "data" / args.date
    out_dir.mkdir(parents=True, exist_ok=True)
    logger = setup_logging(out_dir)
    
    logger.info("=" * 60)
    logger.info(f"Starting UFC fighter stats scraper")
    logger.info(f"Output directory: {out_dir}")
    
    # Determine scraping mode
    if args.fighter_ids_file:
        logger.info(f"Targeted scraping mode: {args.fighter_ids_file}")
        fighter_ids = load_fighter_ids_from_file(args.fighter_ids_file)
        mode = "targeted"
    else:
        logger.info("Discovery mode: A-Z search")
        fighter_ids = get_fighter_ids()
        mode = "discovery"
    
    if not fighter_ids:
        logger.warning("No fighter IDs found. Exiting.")
        print("\n[!] No fighter IDs to scrape. Exiting.")
        return
    
    # Load existing fighters to avoid duplicates
    fighters_file = out_dir / "raw" / "fighters.csv"
    existing = load_existing_fighters(fighters_file)
    
    to_scrape = fighter_ids - existing
    logger.info(f"Fighter IDs to scrape: {len(to_scrape)} (existing: {len(existing)})")
    print(f"\nScraping stats for {len(to_scrape)} fighters "
          f"({len(existing)} already done, {mode} mode)...\n")
    
    if not to_scrape:
        logger.info("All fighters already scraped. Nothing to do.")
        print("All fighters already scraped.")
        return
    
    fighters = []
    failed_fighters = {}
    
    def _scrape_fighter(fighter_id: str) -> tuple[Optional[dict], Optional[str]]:
        """Scrape a single fighter, return (stats_dict, error_msg)."""
        raw = get_fighter_stats(fighter_id)
        if raw:
            return (clean_fighter_stats(raw), None)
        else:
            return (None, "Failed to fetch or parse fighter page")
    
    print(f"  Scraping fighter pages (5 workers)...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_scrape_fighter, fid): fid for fid in to_scrape}
        done = 0
        for future in as_completed(futures):
            done += 1
            fighter_id = futures[future]
            if done % 10 == 0:
                print(f"\r  Fighter {done}/{len(to_scrape)}", end="", flush=True)
            try:
                stats, error = future.result()
                if stats:
                    fighters.append(stats)
                else:
                    failed_fighters[fighter_id] = (error or "Unknown error", 1)
            except Exception as exc:
                logger.error(f"Future exception for {fighter_id}: {exc}")
                failed_fighters[fighter_id] = (str(exc), 1)
    
    print(f"\r  Fighter {done}/{len(to_scrape)}")
    
    logger.info(f"Scraping complete: {len(fighters)} successful, {len(failed_fighters)} failed")
    print(f"  Done. Collected stats for {len(fighters)} fighters ({len(failed_fighters)} failed).\n")
    
    # Save results
    save_to_csv(fighters, out_dir)
    if failed_fighters:
        save_failed_fighters(failed_fighters, out_dir)
    
    # Coverage report
    total = len(existing) + len(fighters)
    print(f"Coverage: {total} fighters in {out_dir}/raw/fighters.csv")
    if failed_fighters:
        print(f"  ({len(failed_fighters)} failed - see {out_dir}/failed_fighters.csv)")
    logger.info(f"Scraper finished. Files saved to {out_dir}/")


if __name__ == "__main__":
    main()