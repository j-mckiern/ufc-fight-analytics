DATE := `date +%Y-%m-%d`

default:
    @just --list

# Run complete scraping pipeline (fights first, then stats for those fighters)
scrape: scrape-fights scrape-stats-targeted

# Scrape fight data (events, fights, fight stats)
scrape-fights:
    uv run src/scrape_ufc_fights.py

# Scrape fighter stats for all discover via A-Z (legacy mode)
scrape-stats-legacy:
    uv run src/scrape_ufc_stats.py

# Scrape fighter stats for pending fighters only (targeted mode, recommended)
scrape-stats-targeted:
    #!/usr/bin/env bash
    set -euo pipefail
    PENDING_FILE="data/{{DATE}}/pending_fighters.csv"
    if [ -f "$PENDING_FILE" ]; then
        echo "Scraping stats for fighters in $PENDING_FILE"
        uv run src/scrape_ufc_stats.py --fighter-ids-file "$PENDING_FILE"
    else
        echo "[!] No pending_fighters.csv found at $PENDING_FILE"
        echo "    Run: just scrape-fights"
        exit 1
    fi

# Validate fighter ID coverage across CSV files
validate:
    uv run src/validate_fighters.py

# Run all notebooks in order (prep → train → predict) and save outputs
run-notebooks:
    uv run jupyter nbconvert --to notebook --execute --inplace notebooks/fight_prediction_prep.ipynb
    uv run jupyter nbconvert --to notebook --execute --inplace notebooks/fight_prediction_model.ipynb
    uv run jupyter nbconvert --to notebook --execute --inplace notebooks/predict.ipynb

# Clean data directory (use with caution!)
clean:
    rm -rf data/
    @echo "Data directory cleaned"

