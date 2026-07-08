from ufc.features.cleaning import clean_fight_data


def test_drops_draws_and_bad_methods(fights_df, fight_stats_df):
    fights_clean, stats_clean = clean_fight_data(fights_df, fight_stats_df)

    kept = set(fights_clean["fight_id"])
    assert "ft6" not in kept  # DQ
    assert "ft7" not in kept  # draw (no W/L pair)
    assert kept == {"ft1", "ft2", "ft3", "ft4", "ft5", "ft8", "ft9"}

    # fight_stats filtered to the same fights, one W and one L each
    per_fight = stats_clean.groupby("fight_id")["result"].apply(set)
    assert set(per_fight.index) == kept
    assert all(results == {"W", "L"} for results in per_fight)
