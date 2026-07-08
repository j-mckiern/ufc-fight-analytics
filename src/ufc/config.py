"""Central configuration: canonical paths and modeling constants.

All pipeline functions accept explicit path arguments and default to these
values, so tests can pass temporary directories without touching config.
"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = Path(os.environ.get("UFC_DATA_DIR", REPO_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
PREPARED_DIR = DATA_DIR / "prepared"
PREDICTIONS_CSV = DATA_DIR / "predictions.csv"

MODELS_DIR = Path(os.environ.get("UFC_MODELS_DIR", REPO_ROOT / "models"))
CURRENT_MODEL_DIR = MODELS_DIR / "current"
ARCHIVE_MODEL_DIR = MODELS_DIR / "archive"

SEED = 42

# Fights with these methods are excluded from training data.
DROP_METHODS = {"Overturned", "CNC", "DQ", "Other"}

# Weight classes with fewer training fights than this are collapsed to "Other".
RARE_WEIGHT_CLASS_MIN = 50

# The temporal train/test split holds out the most recent N months of fights,
# so the test window stays meaningful as new data accumulates.
TEST_WINDOW_MONTHS = 18
