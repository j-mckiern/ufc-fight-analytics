import numpy as np
import pandas as pd

from ufc.features.cleaning import clean_fight_data
from ufc.features.history import HIST_FEATURES, compute_fighter_history


def _history(fights_df, fight_stats_df):
    fights_clean, stats_clean = clean_fight_data(fights_df, fight_stats_df)
    return compute_fighter_history(stats_clean, fights_clean)


def test_debut_fight_has_zero_history(fights_df, fight_stats_df):
    hist = _history(fights_df, fight_stats_df)
    first = hist[(hist["fighter_id"] == "f1") & (hist["fight_id"] == "ft1")].iloc[0]
    assert first["hist_fights"] == 0
    assert first["hist_wins"] == 0
    assert first["hist_losses"] == 0
    assert np.isnan(first["hist_win_rate"])
    assert np.isnan(first["hist_strike_acc"])


def test_history_counts_prior_fights_only(fights_df, fight_stats_df):
    hist = _history(fights_df, fight_stats_df)
    # f1: ft1 (2020, W), ft3 (2021, W), ft8 (2025, W)
    ft8 = hist[(hist["fighter_id"] == "f1") & (hist["fight_id"] == "ft8")].iloc[0]
    assert ft8["hist_fights"] == 2
    assert ft8["hist_wins"] == 2
    assert ft8["hist_losses"] == 0
    assert ft8["hist_win_rate"] == 1.0
    # prior strike accuracy: mean of 50/100 and 40/80 = 0.5
    assert abs(ft8["hist_strike_acc"] - 0.5) < 1e-12
    # one of the two prior wins was a finish (ft3 SUB); ft1 was U-DEC
    assert abs(ft8["hist_finish_rate"] - 0.5) < 1e-12


def test_leak_freeness_prefix_property(fights_df, fight_stats_df):
    """Features for past fights must not change when future fights are added."""
    full = _history(fights_df, fight_stats_df)

    cutoff = pd.Timestamp("2024-01-01")
    truncated_fights = fights_df[fights_df["event_date"] < cutoff]
    truncated_stats = fight_stats_df[
        fight_stats_df["fight_id"].isin(truncated_fights["fight_id"])
    ]
    partial = _history(truncated_fights, truncated_stats)

    key = ["fighter_id", "fight_id"]
    merged = partial[key + HIST_FEATURES].merge(
        full[key + HIST_FEATURES], on=key, suffixes=("_partial", "_full")
    )
    assert len(merged) == len(partial)
    for col in HIST_FEATURES:
        pd.testing.assert_series_equal(
            merged[f"{col}_partial"],
            merged[f"{col}_full"],
            check_names=False,
        )
