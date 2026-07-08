"""Filter raw fight data down to fights with a decisive W/L outcome."""

import pandas as pd

from ufc.config import DROP_METHODS


def clean_fight_data(
    fights: pd.DataFrame, fight_stats: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (fights_clean, fight_stats_clean).

    Keeps only fights with exactly one W and one L row (drops draws,
    no-contests, and malformed records) and drops methods in DROP_METHODS.
    """
    w_fights = fight_stats[fight_stats["result"] == "W"]["fight_id"]
    l_fights = fight_stats[fight_stats["result"] == "L"]["fight_id"]
    valid_fight_ids = set(w_fights) & set(l_fights)

    fight_stats_clean = fight_stats[fight_stats["fight_id"].isin(valid_fight_ids)].copy()
    fights_clean = fights[fights["fight_id"].isin(valid_fight_ids)].copy()

    fights_clean = fights_clean[~fights_clean["method"].isin(DROP_METHODS)]
    valid_fight_ids = set(fights_clean["fight_id"])
    fight_stats_clean = fight_stats_clean[
        fight_stats_clean["fight_id"].isin(valid_fight_ids)
    ].copy()

    return fights_clean, fight_stats_clean
