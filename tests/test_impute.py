import numpy as np
import pandas as pd

from ufc.features.build import PrepParams, build_training_data
from ufc.features.cleaning import clean_fight_data
from ufc.features.history import compute_fighter_history
from ufc.features.impute import ImputationParams
from ufc.features.matchups import assign_ab, build_matchups, merge_matchup_features


def _unimputed_dataset(fights_df, fight_stats_df, fighters_df):
    fights_clean, stats_clean = clean_fight_data(fights_df, fight_stats_df)
    matchups = assign_ab(build_matchups(stats_clean, fights_clean), seed=0)
    hist = compute_fighter_history(stats_clean, fights_clean)
    return merge_matchup_features(matchups, fighters_df, hist)


def test_transform_fills_everything(fights_df, fight_stats_df, fighters_df):
    ds = _unimputed_dataset(fights_df, fight_stats_df, fighters_df)
    params = ImputationParams.fit(ds)
    out = params.transform(ds)

    feature_like = [
        c for c in out.columns
        if c not in {"fight_id", "fighter_a_id", "fighter_b_id", "event_date", "target"}
        and not c.startswith("stance")
        and c != "weight_class"
    ]
    assert out[feature_like].isnull().sum().sum() == 0
    assert (out[["stance_a", "stance_b"]].isnull().sum() == 0).all()


def test_single_row_transform_matches_batch(fights_df, fight_stats_df, fighters_df):
    """A prediction-time row must get the same imputed values as in training."""
    ds = _unimputed_dataset(fights_df, fight_stats_df, fighters_df)
    params = ImputationParams.fit(ds)
    batch = params.transform(ds)

    # ft2 involves f5 (missing height/reach) — imputation actually fires
    idx = ds.index[ds["fight_id"] == "ft2"][0]
    single = params.transform(ds.loc[[idx]])

    pd.testing.assert_frame_equal(
        single.reset_index(drop=True),
        batch.loc[[idx]].reset_index(drop=True),
    )


def test_unseen_weight_class_falls_back_to_global(fights_df, fight_stats_df, fighters_df):
    ds = _unimputed_dataset(fights_df, fight_stats_df, fighters_df)
    params = ImputationParams.fit(ds)

    row = ds[ds["fight_id"] == "ft2"].copy()
    row["weight_class"] = "Super Heavyweight"  # never seen during fit
    out = params.transform(row)
    assert out["height_in_a"].notna().all()
    assert out["reach_in_b"].notna().all()


def test_prep_params_json_roundtrip(raw_dir):
    data = build_training_data(raw_dir=raw_dir, cutoff="2024-01-01")
    params = PrepParams.from_dict(data.params.to_dict())
    assert params.feature_cols == data.params.feature_cols
    assert params.kept_weight_classes == data.params.kept_weight_classes
    assert params.impute.phys_wc_medians == data.params.impute.phys_wc_medians
    # NaN medians compare unequal by ==; check keys and non-NaN values
    for col, v in data.params.impute.hist_rate_medians.items():
        v2 = params.impute.hist_rate_medians[col]
        assert (np.isnan(v) and np.isnan(v2)) or v == v2
