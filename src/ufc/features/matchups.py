"""Matchup table construction: A/B assignment, profile + history merges, diffs."""

import numpy as np
import pandas as pd

from ufc.config import SEED
from ufc.features.history import HIST_COLS, HIST_FEATURES

PROFILE_COLS = [
    "fighter_id",
    "height_in",
    "weight_lb",
    "reach_in",
    "stance",
    "age",
    "slpm",
    "str_acc_dec",
    "sapm",
    "str_def_dec",
    "td_avg",
    "td_acc_dec",
    "td_def_dec",
    "sub_avg",
]

DIFF_PAIRS = [
    "height_in",
    "weight_lb",
    "reach_in",
    "age",
    "slpm",
    "str_acc_dec",
    "sapm",
    "str_def_dec",
    "td_avg",
    "td_acc_dec",
    "td_def_dec",
    "sub_avg",
    "hist_fights",
    "hist_wins",
    "hist_losses",
    "hist_win_rate",
    "hist_strike_acc",
    "hist_td_acc",
    "hist_sub_avg",
    "hist_ctrl_avg",
    "hist_finish_rate",
]


def build_matchups(
    fight_stats_clean: pd.DataFrame, fights_clean: pd.DataFrame
) -> pd.DataFrame:
    """One row per fight with winner_id/loser_id, event_date, and weight_class."""
    winners = fight_stats_clean[fight_stats_clean["result"] == "W"][
        ["fight_id", "fighter_id"]
    ].rename(columns={"fighter_id": "winner_id"})
    losers = fight_stats_clean[fight_stats_clean["result"] == "L"][
        ["fight_id", "fighter_id"]
    ].rename(columns={"fighter_id": "loser_id"})
    return winners.merge(losers, on="fight_id").merge(
        fights_clean[["fight_id", "event_date", "weight_class"]], on="fight_id"
    )


def assign_ab(matchups: pd.DataFrame, seed: int = SEED) -> pd.DataFrame:
    """Randomly assign winner/loser to A/B positions to avoid positional bias.

    target = 1 if fighter A won, 0 if fighter B won (~0.5 balance).
    """
    matchups = matchups.copy()
    rng = np.random.default_rng(seed)
    swap = rng.random(len(matchups)) > 0.5

    matchups["fighter_a_id"] = np.where(swap, matchups["winner_id"], matchups["loser_id"])
    matchups["fighter_b_id"] = np.where(swap, matchups["loser_id"], matchups["winner_id"])
    matchups["target"] = np.where(matchups["fighter_a_id"] == matchups["winner_id"], 1, 0)
    return matchups.drop(columns=["winner_id", "loser_id"])


def _apply_age_at_event(dataset: pd.DataFrame, suffix: str) -> None:
    """Replace scrape-time age with age at event_date where dob is known."""
    dob = pd.to_datetime(dataset[f"dob_{suffix}"], errors="coerce")
    age_at_event = np.floor((dataset["event_date"] - dob).dt.days / 365.2425)
    dataset[f"age_{suffix}"] = age_at_event.where(dob.notna(), dataset[f"age_{suffix}"])


def merge_matchup_features(
    matchups: pd.DataFrame,
    fighters: pd.DataFrame,
    fighter_hist: pd.DataFrame,
) -> pd.DataFrame:
    """Attach profile and historical features for both fighters, plus A-B diffs.

    If fighters has a `dob` column, age is computed at event_date (leak-free)
    instead of using the scrape-time age; rows without dob keep the stored age.
    """
    profile_cols = list(PROFILE_COLS)
    has_dob = "dob" in fighters.columns
    if has_dob:
        profile_cols = profile_cols + ["dob"]

    dataset = matchups.merge(
        fighters[profile_cols].rename(
            columns={c: f"{c}_a" for c in profile_cols if c != "fighter_id"}
        ),
        left_on="fighter_a_id",
        right_on="fighter_id",
        how="left",
    ).drop(columns="fighter_id")

    dataset = dataset.merge(
        fighters[profile_cols].rename(
            columns={c: f"{c}_b" for c in profile_cols if c != "fighter_id"}
        ),
        left_on="fighter_b_id",
        right_on="fighter_id",
        how="left",
    ).drop(columns="fighter_id")

    if has_dob:
        _apply_age_at_event(dataset, "a")
        _apply_age_at_event(dataset, "b")
        dataset = dataset.drop(columns=["dob_a", "dob_b"])

    hist_slim = fighter_hist[HIST_COLS]
    dataset = dataset.merge(
        hist_slim.rename(columns={c: f"{c}_a" for c in HIST_FEATURES}),
        left_on=["fight_id", "fighter_a_id"],
        right_on=["fight_id", "fighter_id"],
        how="left",
    ).drop(columns="fighter_id")

    dataset = dataset.merge(
        hist_slim.rename(columns={c: f"{c}_b" for c in HIST_FEATURES}),
        left_on=["fight_id", "fighter_b_id"],
        right_on=["fight_id", "fighter_id"],
        how="left",
    ).drop(columns="fighter_id")

    for col in DIFF_PAIRS:
        dataset[f"{col}_diff"] = dataset[f"{col}_a"] - dataset[f"{col}_b"]

    return dataset
