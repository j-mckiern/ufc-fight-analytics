"""Per-fighter historical features computed from prior fights only.

Every hist_* value on a row describes the fighter *before* that fight:
cumulative stats are shifted by one within each fighter's chronologically
sorted fight sequence, so a fighter's debut always has zero counts and NaN
rates. This is what keeps the features leak-free.
"""

import numpy as np
import pandas as pd

HIST_COLS = [
    "fight_id",
    "fighter_id",
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

HIST_FEATURES = [c for c in HIST_COLS if c not in ("fight_id", "fighter_id")]

FINISH_METHODS = ["KO/TKO", "SUB"]


def compute_fighter_history(
    fight_stats_clean: pd.DataFrame, fights_clean: pd.DataFrame
) -> pd.DataFrame:
    """Return one row per (fighter, fight) with pre-fight historical features."""
    fs = fight_stats_clean.merge(
        fights_clean[["fight_id", "event_date", "method"]], on="fight_id"
    )
    fs = fs.sort_values(["fighter_id", "event_date", "fight_id"]).reset_index(drop=True)

    grp = fs.groupby("fighter_id")

    # Fight count per fighter (0-indexed: 0 for first fight)
    fs["hist_fights"] = grp.cumcount()

    fs["_is_win"] = (fs["result"] == "W").astype(int)
    fs["_is_loss"] = (fs["result"] == "L").astype(int)
    fs["hist_wins"] = grp["_is_win"].transform(lambda x: x.cumsum().shift(1, fill_value=0))
    fs["hist_losses"] = grp["_is_loss"].transform(lambda x: x.cumsum().shift(1, fill_value=0))
    fs["hist_win_rate"] = fs["hist_wins"] / fs["hist_fights"].replace(0, np.nan)

    fs["_strike_acc"] = fs["sig_strikes"] / fs["sig_attempted"].replace(0, np.nan)
    fs["hist_strike_acc"] = grp["_strike_acc"].transform(
        lambda x: x.expanding().mean().shift(1)
    )

    fs["_td_acc"] = fs["td"] / fs["td_attempted"].replace(0, np.nan)
    fs["hist_td_acc"] = grp["_td_acc"].transform(lambda x: x.expanding().mean().shift(1))

    fs["hist_sub_avg"] = grp["sub_attempts"].transform(
        lambda x: x.expanding().mean().shift(1)
    )
    fs["hist_ctrl_avg"] = grp["control_time"].transform(
        lambda x: x.expanding().mean().shift(1)
    )

    # Finish rate: fraction of prior wins by KO/TKO or SUB
    fs["_is_finish_win"] = (
        (fs["result"] == "W") & (fs["method"].isin(FINISH_METHODS))
    ).astype(int)
    fs["_hist_finish_wins"] = grp["_is_finish_win"].transform(
        lambda x: x.cumsum().shift(1, fill_value=0)
    )
    fs["hist_finish_rate"] = fs["_hist_finish_wins"] / fs["hist_wins"].replace(0, np.nan)

    return fs.drop(columns=[c for c in fs.columns if c.startswith("_")])
