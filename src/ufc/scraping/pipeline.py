"""Incremental scraping into the single evolving data/raw directory.

scrape_new() only fetches event pages it hasn't seen (tracked in events.csv),
only fetches fight details missing from fight_stats.csv, and refreshes the
profile of every fighter who appears in newly scraped fights (upsert), so
records and career stats stay current. Safe to re-run any time; failures
leave the event unmarked so the next run retries it.
"""

import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

from ufc import config
from ufc.scraping.events import scrape_event_fights, scrape_events_list
from ufc.scraping.fight_details import scrape_fight_detail
from ufc.scraping.fighters import scrape_fighter, upsert_fighters
from ufc.scraping.http import make_session

logger = logging.getLogger(__name__)

MAX_WORKERS = 8

EVENTS_FIELDS = ["event_id", "name", "date"]
FIGHTS_FIELDS = ["fight_id", "event_date", "weight_class", "method", "round"]
STATS_FIELDS = [
    "fight_id", "fighter_id", "result", "sig_strikes", "sig_attempted",
    "td", "td_attempted", "sub_attempts", "control_time",
]


def load_existing_ids(filepath: Path, id_column: str) -> set[str]:
    ids: set[str] = set()
    if filepath.exists():
        with open(filepath, newline="") as f:
            for row in csv.DictReader(f):
                ids.add(row[id_column])
    return ids


def _append_rows(filepath: Path, fieldnames: list[str], rows: list[dict]) -> None:
    exists = filepath.exists() and filepath.stat().st_size > 0
    with open(filepath, "a" if exists else "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def _max_fight_date(fights_path: Path) -> str:
    latest = ""
    if fights_path.exists():
        with open(fights_path, newline="") as f:
            for row in csv.DictReader(f):
                latest = max(latest, row["event_date"])
    return latest


def _backfill_events(events_path: Path, events: list[dict], fights_path: Path) -> None:
    """First run against a legacy data dir: mark already-scraped events done.

    Every event strictly older than the newest fight date is assumed present
    in fights.csv; events on the boundary date get (re-)scraped, which is
    harmless thanks to fight_id dedupe.
    """
    latest = _max_fight_date(fights_path)
    if not latest:
        return
    done = [e for e in events if e["date"] < latest]
    _append_rows(events_path, EVENTS_FIELDS, done)
    logger.info("Backfilled events.csv with %d pre-%s events", len(done), latest)


def missing_fighter_ids(raw_dir: Path) -> set[str]:
    """Fighters appearing in fight_stats.csv without a profile in fighters.csv."""
    return load_existing_ids(raw_dir / "fight_stats.csv", "fighter_id") - (
        load_existing_ids(raw_dir / "fighters.csv", "fighter_id")
    )


def scrape_new(raw_dir: Path | None = None, progress=print) -> dict:
    """Incrementally scrape new events, fights, stats, and fighter profiles."""
    raw_dir = raw_dir if raw_dir is not None else config.RAW_DIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    events_path = raw_dir / "events.csv"
    fights_path = raw_dir / "fights.csv"
    stats_path = raw_dir / "fight_stats.csv"
    fighters_path = raw_dir / "fighters.csv"

    session = make_session()

    progress("Fetching completed events list...")
    all_events = scrape_events_list(session)
    today = date.today().isoformat()
    completed = [e for e in all_events if e["date"] < today]

    if not events_path.exists():
        _backfill_events(events_path, completed, fights_path)
    done_ids = load_existing_ids(events_path, "event_id")
    new_events = [e for e in completed if e["event_id"] not in done_ids]
    progress(f"{len(new_events)} new events to scrape "
             f"({len(completed)} completed total).")

    # -- event pages -> fight rows ------------------------------------------
    event_fights: dict[str, list[dict]] = {}
    failed_events: set[str] = set()
    if new_events:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(scrape_event_fights, e["url"], e["date"], session): e
                for e in new_events
            }
            for i, future in enumerate(as_completed(futures), 1):
                event = futures[future]
                progress(f"\r  event page {i}/{len(new_events)}", end="", flush=True)
                try:
                    event_fights[event["event_id"]] = future.result()
                except Exception as exc:
                    logger.error("Event %s failed: %s", event["url"], exc)
                    failed_events.add(event["event_id"])
        progress("")

    all_new_fights = [f for fights in event_fights.values() for f in fights]
    existing_fight_ids = load_existing_ids(fights_path, "fight_id")
    fight_rows = [f for f in all_new_fights if f["fight_id"] not in existing_fight_ids]
    _append_rows(fights_path, FIGHTS_FIELDS, fight_rows)

    # -- fight detail pages -> stat rows ------------------------------------
    existing_stat_ids = load_existing_ids(stats_path, "fight_id")
    to_detail = [f for f in all_new_fights if f["fight_id"] not in existing_stat_ids]
    stat_rows: list[dict] = []
    failed_fights: set[str] = set()
    if to_detail:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(scrape_fight_detail, f["fight_url"], session): f
                for f in to_detail
            }
            for i, future in enumerate(as_completed(futures), 1):
                fight = futures[future]
                progress(f"\r  fight page {i}/{len(to_detail)}", end="", flush=True)
                try:
                    rows = future.result()
                except Exception as exc:
                    logger.error("Fight %s failed: %s", fight["fight_id"], exc)
                    rows = []
                if rows:
                    stat_rows.extend(rows)
                else:
                    failed_fights.add(fight["fight_id"])
        progress("")
    _append_rows(stats_path, STATS_FIELDS, stat_rows)

    # -- mark fully scraped events done --------------------------------------
    done_events = []
    for event in new_events:
        if event["event_id"] in failed_events:
            continue
        fights = event_fights.get(event["event_id"], [])
        if any(f["fight_id"] in failed_fights for f in fights):
            continue
        done_events.append(event)
    _append_rows(events_path, EVENTS_FIELDS, done_events)

    # -- refresh profiles for fighters in new fights + any missing ----------
    to_refresh = {row["fighter_id"] for row in stat_rows} | missing_fighter_ids(raw_dir)
    fighter_records: list[dict] = []
    failed_fighters: set[str] = set()
    if to_refresh:
        progress(f"Refreshing {len(to_refresh)} fighter profiles...")
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {
                pool.submit(scrape_fighter, fid, session): fid for fid in to_refresh
            }
            for i, future in enumerate(as_completed(futures), 1):
                fid = futures[future]
                progress(f"\r  fighter {i}/{len(to_refresh)}", end="", flush=True)
                record = future.result()
                if record:
                    fighter_records.append(record)
                else:
                    failed_fighters.add(fid)
        progress("")
        upsert_fighters(fighter_records, fighters_path)

    still_missing = missing_fighter_ids(raw_dir)
    summary = {
        "new_events": len(done_events),
        "failed_events": len(failed_events),
        "new_fights": len(fight_rows),
        "new_stat_rows": len(stat_rows),
        "failed_fights": len(failed_fights),
        "fighters_refreshed": len(fighter_records),
        "failed_fighters": len(failed_fighters),
        "fighters_missing_profile": len(still_missing),
    }
    progress(f"Done: {summary}")
    return summary
