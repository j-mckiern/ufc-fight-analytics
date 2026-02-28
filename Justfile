default:
    @just --list

# Run both scrapers (fighters first, then fights)
scrape: scrape-stats scrape-fights

# Scrape fighter stats
scrape-stats:
    uv run src/scrape_ufc_stats.py

# Scrape fight data
scrape-fights:
    uv run src/scrape_ufc_fights.py
