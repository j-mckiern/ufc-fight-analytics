import json

import pandas as pd

from ufc.features.build import build_training_data, save_prepared


def test_end_to_end_build(raw_dir, tmp_path):
    data = build_training_data(raw_dir=raw_dir, cutoff="2024-01-01")

    # temporal split
    assert (data.train["event_date"] < pd.Timestamp("2024-01-01")).all()
    assert (data.test["event_date"] >= pd.Timestamp("2024-01-01")).all()
    assert len(data.train) + len(data.test) == len(data.dataset)

    # no nulls in features
    assert data.dataset[data.params.feature_cols].isnull().sum().sum() == 0

    # fighter_features covers every profiled fighter, including the debut one
    assert set(data.fighter_features["fighter_id"]) == {
        "f1", "f2", "f3", "f4", "f5", "f6", "f7"
    }
    debut = data.fighter_features[data.fighter_features["fighter_id"] == "f7"].iloc[0]
    assert debut["hist_fights"] == 0
    # rates stay NaN so the shared imputer handles them at prediction time
    assert pd.isna(debut["hist_win_rate"])

    prepared = save_prepared(data, tmp_path / "prepared")
    saved = {p.name for p in prepared.iterdir()}
    assert saved == {
        "train.csv", "test.csv", "matchups_full.csv",
        "fighter_features.csv", "feature_cols.csv", "prep_params.json",
    }
    with open(prepared / "prep_params.json") as f:
        params = json.load(f)
    assert params["feature_cols"] == data.params.feature_cols


def test_rolling_cutoff_default(raw_dir):
    data = build_training_data(raw_dir=raw_dir)
    # newest event 2025-01-01 minus 18 months = 2023-07-01
    assert data.params.cutoff == "2023-07-01"
    assert (data.test["event_date"] >= pd.Timestamp("2023-07-01")).all()
