import pandas as pd

from ufc.features.cleaning import clean_fight_data
from ufc.features.history import compute_fighter_history
from ufc.features.matchups import (
    DIFF_PAIRS,
    assign_ab,
    build_matchups,
    merge_matchup_features,
)


def _pipeline(fights_df, fight_stats_df, fighters_df):
    fights_clean, stats_clean = clean_fight_data(fights_df, fight_stats_df)
    matchups = build_matchups(stats_clean, fights_clean)
    hist = compute_fighter_history(stats_clean, fights_clean)
    return matchups, hist, fighters_df


def test_target_marks_winner(fights_df, fight_stats_df):
    fights_clean, stats_clean = clean_fight_data(fights_df, fight_stats_df)
    matchups = build_matchups(stats_clean, fights_clean)
    winners = dict(zip(matchups["fight_id"], matchups["winner_id"]))

    assigned = assign_ab(matchups, seed=0)
    for _, row in assigned.iterrows():
        expected = 1 if row["fighter_a_id"] == winners[row["fight_id"]] else 0
        assert row["target"] == expected
        assert {row["fighter_a_id"], row["fighter_b_id"]} >= {winners[row["fight_id"]]}


def test_swapping_ab_negates_diffs(fights_df, fight_stats_df, fighters_df):
    matchups, hist, fighters = _pipeline(fights_df, fight_stats_df, fighters_df)
    assigned = assign_ab(matchups, seed=0)

    swapped = assigned.copy()
    swapped[["fighter_a_id", "fighter_b_id"]] = assigned[
        ["fighter_b_id", "fighter_a_id"]
    ].to_numpy()

    ds = merge_matchup_features(assigned, fighters, hist)
    ds_swap = merge_matchup_features(swapped, fighters, hist)

    for col in DIFF_PAIRS:
        pd.testing.assert_series_equal(
            ds[f"{col}_diff"], -ds_swap[f"{col}_diff"], check_names=False
        )


def test_age_computed_at_event_date_when_dob_present(
    fights_df, fight_stats_df, fighters_df
):
    fighters = fighters_df.copy()
    # f1 born 1995-06-15: age 24 at ft1 (2020-01-01), 29 at ft8 (2025-01-01)
    fighters["dob"] = pd.NaT
    fighters.loc[fighters["fighter_id"] == "f1", "dob"] = pd.Timestamp("1995-06-15")

    matchups, hist, _ = _pipeline(fights_df, fight_stats_df, fighters_df)
    assigned = assign_ab(matchups, seed=0)
    ds = merge_matchup_features(assigned, fighters, hist)

    for fight_id, expected_age in [("ft1", 24), ("ft8", 29)]:
        row = ds[ds["fight_id"] == fight_id].iloc[0]
        side = "a" if row["fighter_a_id"] == "f1" else "b"
        assert row[f"age_{side}"] == expected_age

    # fighters without dob keep their scrape-time age
    row = ds[ds["fight_id"] == "ft1"].iloc[0]
    other = "b" if row["fighter_a_id"] == "f1" else "a"
    assert row[f"age_{other}"] == 31  # f2's stored age
