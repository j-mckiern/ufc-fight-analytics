"""Matchup and card prediction against the current model bundle.

Feature rows are built from the fighter_features lookup table and passed
through the bundle's fitted ImputationParams, so a prediction row gets exactly
the same imputation the training matrix got. Fighters absent from the lookup
(debuts) are skipped with a warning entry — they never crash a card run.
"""

import numpy as np
import pandas as pd

from ufc.features.history import HIST_FEATURES
from ufc.features.matchups import PROFILE_COLS
from ufc.modeling.registry import ModelBundle

PROFILE_NUMERIC = [c for c in PROFILE_COLS if c not in ("fighter_id", "stance")]


class FighterNotFoundError(Exception):
    """The fighter has no row in fighter_features (typically a UFC debut)."""


def get_fighter_by_id(fighter_features: pd.DataFrame, fighter_id: str) -> pd.Series:
    rows = fighter_features[fighter_features["fighter_id"] == fighter_id]
    if len(rows) == 0:
        raise FighterNotFoundError(
            f"No data for fighter '{fighter_id}' — likely a UFC debut."
        )
    return rows.iloc[0]


def _age(fighter: pd.Series, event_date) -> float:
    dob = fighter.get("dob")
    if event_date is not None and pd.notna(dob):
        days = (pd.Timestamp(event_date) - pd.Timestamp(dob)).days
        return float(np.floor(days / 365.2425))
    return fighter.get("age", np.nan)


def build_feature_row(
    fighter_a: pd.Series,
    fighter_b: pd.Series,
    weight_class: str | None,
    bundle: ModelBundle,
    event_date=None,
) -> pd.DataFrame:
    """Assemble one model-ready row, imputed and encoded like training data."""
    params = bundle.params
    row: dict = {}
    for suffix, fighter in (("a", fighter_a), ("b", fighter_b)):
        for col in PROFILE_NUMERIC + HIST_FEATURES:
            row[f"{col}_{suffix}"] = fighter.get(col, np.nan)
        row[f"age_{suffix}"] = _age(fighter, event_date)
        row[f"stance_{suffix}"] = fighter.get("stance", np.nan)
    # The imputer looks up per-weight-class medians by the raw class name;
    # rare-class collapse only applies to the dummy encoding below.
    row["weight_class"] = weight_class if weight_class else "Other"

    X = params.impute.transform(pd.DataFrame([row]))

    if X.loc[0, "weight_class"] not in params.kept_weight_classes:
        X.loc[0, "weight_class"] = "Other"
    X = pd.get_dummies(X, columns=["stance_a", "stance_b", "weight_class"], dtype=int)
    return X.reindex(columns=params.feature_cols, fill_value=0)


def predict_matchup(
    bundle: ModelBundle,
    fighter_features: pd.DataFrame,
    fighter_a_id: str,
    fighter_b_id: str,
    weight_class: str | None = None,
    event_date=None,
) -> dict:
    """Predict a single matchup by fighter ids.

    Raises FighterNotFoundError if either fighter has no data (debut).
    """
    fighter_a = get_fighter_by_id(fighter_features, fighter_a_id)
    fighter_b = get_fighter_by_id(fighter_features, fighter_b_id)

    X = build_feature_row(fighter_a, fighter_b, weight_class, bundle, event_date)
    prob_a = float(bundle.pipeline.predict_proba(X)[0, 1])

    a_wins = prob_a > 0.5
    return {
        "fighter_a_id": fighter_a_id,
        "fighter_b_id": fighter_b_id,
        "fighter_a_name": fighter_a.get("name", fighter_a_id),
        "fighter_b_name": fighter_b.get("name", fighter_b_id),
        "weight_class": weight_class,
        "prob_a": prob_a,
        "prob_b": 1 - prob_a,
        "predicted_winner_id": fighter_a_id if a_wins else fighter_b_id,
        "predicted_winner_name": (
            fighter_a.get("name") if a_wins else fighter_b.get("name")
        ),
        "confidence": max(prob_a, 1 - prob_a),
        "model_version": bundle.model_version,
    }


def predict_card(
    bundle: ModelBundle,
    fighter_features: pd.DataFrame,
    bouts: list[dict],
    event_date=None,
) -> list[dict]:
    """Predict every bout on a card; debut fighters yield a skipped entry.

    Each bout needs fighter_a_id/fighter_b_id and may carry
    fighter_a_name/fighter_b_name/weight_class. Never raises for missing
    fighters — the rest of the card still gets predictions.
    """
    results = []
    for bout in bouts:
        base = {
            "fighter_a_id": bout.get("fighter_a_id"),
            "fighter_b_id": bout.get("fighter_b_id"),
            "fighter_a_name": bout.get("fighter_a_name", bout.get("fighter_a_id")),
            "fighter_b_name": bout.get("fighter_b_name", bout.get("fighter_b_id")),
            "weight_class": bout.get("weight_class"),
        }
        try:
            result = predict_matchup(
                bundle,
                fighter_features,
                bout["fighter_a_id"],
                bout["fighter_b_id"],
                weight_class=bout.get("weight_class"),
                event_date=event_date,
            )
        except FighterNotFoundError as exc:
            results.append({**base, "status": "skipped_debut", "reason": str(exc)})
        else:
            results.append({**base, **result, "status": "ok"})
    return results
