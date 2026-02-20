"""Script to scrape UFCStats.com and build a csv"""

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


def main():
    fighter_ids = get_fighter_ids()

    print(fighter_ids)

if __name__ == "__main__":
    main()