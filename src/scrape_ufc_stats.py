"""Script to scrape UFCStats.com and build a csv"""

import requests
from bs4 import BeautifulSoup

URL = "http://ufcstats.com/statistics/fighters" 
page = requests.get(URL)

soup = BeautifulSoup(page.content, 'html.parser')

rows = soup.find_all("tr", class_="b-statistics__table-row")

fighters = []

for row in rows:
    cols = row.find_all("td")


    if len(cols) < 3:
        continue  # skip header / malformed rows

    # name fields
    first_name = cols[0].find("a", class_="b-link b-link_style_black").get_text(strip=True)
    last_name = cols[1].find("a", class_="b-link b-link_style_black").get_text(strip=True)
    nickname = cols[2].find("a", class_="b-link b-link_style_black").get_text(strip=True)

    fighter = {
        "first_name": first_name,
        "last_name": last_name,
        "nickname": nickname
    }
    fighters.append(fighter)

# Print names, skipping missing parts
for f in fighters:
    parts = []
    if f["first_name"]:
        parts.append(f["first_name"])
    if f["nickname"]:
        parts.append(f'({f["nickname"]})')
    if f["last_name"]:
        parts.append(f["last_name"])
    
    print(" ".join(parts))