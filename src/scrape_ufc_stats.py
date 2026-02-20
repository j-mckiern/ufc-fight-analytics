"""Script to scrape UFCStats.com and build a csv"""

import csv
import datetime
import string
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://ufcstats.com"
ALPHABET = string.ascii_lowercase  # "abcdefghijklmnopqrstuvwxyz"


def get_fighter_ids_for_letter(letter: str) -> set[str]:
    """Fetch fighter IDs for a single letter."""
    fighter_ids = set()
    url = f"{BASE_URL}/statistics/fighters?char={letter}&page=all"
    
    try:
        page = requests.get(url)
        if page.status_code != 200:
            return fighter_ids
        
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find("table")
        tbody = table.find("tbody")
        
        if not tbody:
            return fighter_ids
        
        for row in tbody.find_all("tr"):
            link = row.find("a", href=True)
            if not link:
                continue
            
            href = link["href"]
            fighter_id = href.rstrip("/").split("/")[-1]
            fighter_ids.add(fighter_id)
    except Exception as e:
        print(f"Error fetching fighters for letter {letter}: {e}")
    
    return fighter_ids


def get_fighter_ids() -> set[str]:
    fighter_ids = set()
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_fighter_ids_for_letter, letter): letter for letter in ALPHABET}
        
        for future in as_completed(futures):
            fighter_ids.update(future.result())
    
    return fighter_ids

def get_fighter_stats(fighter_id: str) -> dict:
    url = f"{BASE_URL}/fighter-details/{fighter_id}"

    try:
        page = requests.get(url)
        if page.status_code != 200:
            return {}

        soup = BeautifulSoup(page.content, 'html.parser')

        # Name and record
        name = soup.find("span", class_="b-content__title-highlight").get_text(strip=True)
        record = soup.find("span", class_="b-content__title-record").get_text(strip=True).replace("Record: ", "")
        nickname = soup.find("p", class_="b-content__Nickname").get_text(strip=True)

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

        return stats

    except Exception as e:
        print(f"Error fetching stats for fighter_id {fighter_id}: {e}")
        return {}

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

def save_to_csv(fighters: list[dict]):
    if not fighters:
        return

    date_str = datetime.date.today().isoformat()
    filename = f"data/fighters_{date_str}.csv"
    
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fighters[0].keys())
        writer.writeheader()
        writer.writerows(fighters)


def main():
    
    fighter_ids = get_fighter_ids()

    fighters = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(get_fighter_stats, fid): fid for fid in fighter_ids}
        for future in as_completed(futures):
            raw = future.result()
            if raw:
                fighters.append(clean_fighter_stats(raw))   

    save_to_csv(fighters)

if __name__ == "__main__":
    main()