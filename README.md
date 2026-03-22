# UFC Fight Analytics

Predict UFC fight outcomes using historical fighter stats and machine learning.

**Status:** Currently in real-world testing against upcoming UFC events.

## Results

| Model | Accuracy | AUC |
|---|---|---|
| Logistic Regression | 69.9% | 0.773 |
| Random Forest | 70.5% | 0.774 |
| **Gradient Boosting** | **71.3%** | **0.777** |

## Project Structure

```
ufc-fight-analytics/
├── src/
│   ├── scrape_ufc_fights.py    # Scrape events, fights, and fight stats
│   ├── scrape_ufc_stats.py     # Scrape individual fighter profiles
│   └── validate_fighters.py    # Check fighter ID coverage
├── notebooks/
│   ├── fight_prediction_prep.ipynb   # Data cleaning and feature engineering
│   ├── fight_prediction_model.ipynb  # Model training and evaluation
│   └── predict.ipynb                 # Interactive + static predictions
├── data/
│   └── 2026-03-18/
│       ├── raw/                # Scraped CSVs (fights, fighters, fight_stats)
│       └── prepared/           # Engineered features, train/test splits
├── models/                     # Saved model artifacts (.joblib)
└── Justfile                    # Task runner
```

## How It Works

1. **Scrape** — Pull fight results and fighter stats from ufcstats.com
2. **Prepare** — Clean data, engineer features (career stats, win rates, physical diffs), and split train/test
3. **Train** — Compare Logistic Regression, Random Forest, and Gradient Boosting classifiers
4. **Predict** — Pick two fighters and get a win probability for each

## Quickstart

```bash
# Install dependencies
uv sync

# Scrape data (fights first, then fighter stats)
just scrape

# Run all notebooks (prep → train → predict) to regenerate outputs
just run-notebooks
```

Or open notebooks individually in order:
1. `notebooks/fight_prediction_prep.ipynb`
2. `notebooks/fight_prediction_model.ipynb`
3. `notebooks/predict.ipynb`

## Tech Stack

- **Python 3.14** with uv for dependency management
- **scikit-learn** for model training
- **pandas / numpy** for data processing
- **BeautifulSoup** + **requests** for web scraping
- **matplotlib / seaborn** for visualization
- **ipywidgets** for interactive predictions
- **just** for task running
