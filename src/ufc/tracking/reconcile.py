"""Match pending predictions to scraped results and summarize accuracy.

Matching is by unordered fighter-id pair within +/-3 days of the predicted
event date — robust to card reshuffles and date shifts, and immune to
duplicate fighter names since everything keys on ids.
"""

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from ufc import config

MATCH_WINDOW_DAYS = 3


def _load_results(raw_dir: Path) -> pd.DataFrame:
    """One row per decided fight: fighter pair, winner (NaN for draw/NC), method."""
    fights = pd.read_csv(raw_dir / "fights.csv", parse_dates=["event_date"])
    stats = pd.read_csv(raw_dir / "fight_stats.csv")

    pairs = (
        stats.groupby("fight_id")["fighter_id"].apply(sorted).reset_index()
    )
    pairs = pairs[pairs["fighter_id"].str.len() == 2]
    pairs["f1"] = pairs["fighter_id"].str[0]
    pairs["f2"] = pairs["fighter_id"].str[1]

    winners = stats[stats["result"] == "W"][["fight_id", "fighter_id"]].rename(
        columns={"fighter_id": "winner_id"}
    )
    results = (
        pairs[["fight_id", "f1", "f2"]]
        .merge(winners, on="fight_id", how="left")
        .merge(fights[["fight_id", "event_date", "method"]], on="fight_id")
    )
    return results


def reconcile(
    predictions: pd.DataFrame, raw_dir: Path | None = None
) -> tuple[pd.DataFrame, int]:
    """Fill in results for pending predictions. Returns (updated_df, n_matched)."""
    raw_dir = raw_dir if raw_dir is not None else config.RAW_DIR
    results = _load_results(raw_dir)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    predictions = predictions.copy()
    # loaded all-NaN columns come back float64; make them assignable
    for col in ("reconciled_at", "actual_method", "actual_winner_id", "status"):
        predictions[col] = predictions[col].astype("object")
    predictions["correct"] = predictions["correct"].astype("boolean")
    matched = 0
    for idx, row in predictions[predictions["status"] == "pending"].iterrows():
        pair = sorted([str(row["fighter_a_id"]), str(row["fighter_b_id"])])
        event_date = pd.Timestamp(row["event_date"])
        candidates = results[
            (results["f1"] == pair[0])
            & (results["f2"] == pair[1])
            & ((results["event_date"] - event_date).abs().dt.days <= MATCH_WINDOW_DAYS)
        ]
        if len(candidates) == 0:
            continue
        fight = candidates.iloc[0]
        matched += 1
        predictions.loc[idx, "reconciled_at"] = now
        predictions.loc[idx, "actual_method"] = fight["method"]
        if pd.isna(fight["winner_id"]):
            predictions.loc[idx, "status"] = "no_result"
        else:
            predictions.loc[idx, "status"] = "reconciled"
            predictions.loc[idx, "actual_winner_id"] = fight["winner_id"]
            predictions.loc[idx, "correct"] = (
                str(row["predicted_winner_id"]) == str(fight["winner_id"])
            )
    return predictions, matched


def accuracy_summary(predictions: pd.DataFrame) -> dict:
    """Hit rate and Brier score over reconciled predictions."""
    done = predictions[predictions["status"] == "reconciled"].copy()
    if len(done) == 0:
        return {"n": 0, "accuracy": None, "brier": None, "by_model": {}}

    done["correct"] = done["correct"].astype(bool)
    a_won = done["actual_winner_id"].astype(str) == done["fighter_a_id"].astype(str)
    brier = float(np.mean((done["prob_a"] - a_won.astype(float)) ** 2))

    by_model = {}
    for version, grp in done.groupby("model_version"):
        grp_a_won = grp["actual_winner_id"].astype(str) == grp["fighter_a_id"].astype(str)
        by_model[str(version)] = {
            "n": len(grp),
            "accuracy": float(grp["correct"].mean()),
            "brier": float(np.mean((grp["prob_a"] - grp_a_won.astype(float)) ** 2)),
        }

    return {
        "n": len(done),
        "accuracy": float(done["correct"].mean()),
        "brier": brier,
        "by_model": by_model,
    }
