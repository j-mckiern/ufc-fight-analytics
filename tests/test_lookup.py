from ufc.features.build import build_fighter_features
from ufc.features.cleaning import clean_fight_data
from ufc.features.history import compute_fighter_history
from ufc.modeling.lookup import find_fighters


def _ff(fighters_df, fights_df, fight_stats_df):
    fights_clean, stats_clean = clean_fight_data(fights_df, fight_stats_df)
    hist = compute_fighter_history(stats_clean, fights_clean)
    return build_fighter_features(fighters_df, hist)


def test_exact_and_case_insensitive(fighters_df, fights_df, fight_stats_df):
    ff = _ff(fighters_df, fights_df, fight_stats_df)
    assert find_fighters(ff, "Alpha Ape")["fighter_id"].tolist() == ["f1"]
    assert find_fighters(ff, "alpha ape")["fighter_id"].tolist() == ["f1"]


def test_partial_match(fighters_df, fights_df, fight_stats_df):
    ff = _ff(fighters_df, fights_df, fight_stats_df)
    assert find_fighters(ff, "bravo")["fighter_id"].tolist() == ["f2"]


def test_duplicate_names_return_all_candidates(fighters_df, fights_df, fight_stats_df):
    ff = _ff(fighters_df, fights_df, fight_stats_df)
    assert sorted(find_fighters(ff, "Bruno Silva")["fighter_id"]) == ["f3", "f4"]
    assert sorted(find_fighters(ff, "bruno")["fighter_id"]) == ["f3", "f4"]


def test_no_match_and_empty_query_return_empty(fighters_df, fights_df, fight_stats_df):
    ff = _ff(fighters_df, fights_df, fight_stats_df)
    assert len(find_fighters(ff, "Zed Zebra")) == 0
    assert len(find_fighters(ff, "")) == 0
    assert len(find_fighters(ff, "  ")) == 0
