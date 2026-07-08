"""Categorical encoding: stance and weight-class one-hot with rare-class collapse."""

import pandas as pd

from ufc.config import RARE_WEIGHT_CLASS_MIN

EXCLUDE_COLS = {"fight_id", "fighter_a_id", "fighter_b_id", "event_date", "target"}


def fit_weight_class_categories(
    dataset: pd.DataFrame, rare_min: int = RARE_WEIGHT_CLASS_MIN
) -> list[str]:
    """Weight classes common enough to keep as their own dummy column."""
    counts = dataset["weight_class"].value_counts()
    return [str(wc) for wc in counts[counts >= rare_min].index]


def collapse_rare_weight_classes(
    dataset: pd.DataFrame, kept_classes: list[str]
) -> pd.DataFrame:
    dataset = dataset.copy()
    dataset["weight_class"] = dataset["weight_class"].where(
        dataset["weight_class"].isin(kept_classes), "Other"
    )
    return dataset


def encode_categoricals(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset = pd.get_dummies(
        dataset, columns=["stance_a", "stance_b"], drop_first=True, dtype=int
    )
    return pd.get_dummies(dataset, columns=["weight_class"], drop_first=True, dtype=int)


def derive_feature_cols(dataset: pd.DataFrame) -> list[str]:
    return [c for c in dataset.columns if c not in EXCLUDE_COLS]
