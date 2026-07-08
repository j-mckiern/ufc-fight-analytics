"""Missing-value imputation with fitted, serializable parameters.

The same ImputationParams object is fit during training-data preparation and
saved with the model bundle, then reused to transform single prediction rows.
This guarantees a fighter pair gets identical imputed values at training and
prediction time (the old notebook code used two different median sources).
"""

import math
from dataclasses import dataclass

import pandas as pd

from ufc.features.matchups import DIFF_PAIRS

PHYS_BASE = ["height_in", "weight_lb", "reach_in", "age"]
HIST_COUNT_BASE = ["hist_fights", "hist_wins", "hist_losses"]
HIST_RATE_BASE = [
    "hist_win_rate",
    "hist_strike_acc",
    "hist_td_acc",
    "hist_sub_avg",
    "hist_ctrl_avg",
    "hist_finish_rate",
]
CAREER_BASE = [
    "slpm",
    "str_acc_dec",
    "sapm",
    "str_def_dec",
    "td_avg",
    "td_acc_dec",
    "td_def_dec",
    "sub_avg",
]


def _suffixed(bases: list[str]) -> list[str]:
    return [f"{c}_{s}" for c in bases for s in ("a", "b")]


@dataclass
class ImputationParams:
    # suffixed physical column -> weight_class -> median within that class
    phys_wc_medians: dict[str, dict[str, float]]
    # suffixed physical column -> fallback median (computed after the
    # weight-class fill, matching the original preparation order)
    phys_global_medians: dict[str, float]
    # suffixed hist rate/avg column -> global median
    hist_rate_medians: dict[str, float]

    @classmethod
    def fit(cls, dataset: pd.DataFrame) -> "ImputationParams":
        phys_wc: dict[str, dict[str, float]] = {}
        phys_global: dict[str, float] = {}
        for col in _suffixed(PHYS_BASE):
            wc_medians = dataset.groupby("weight_class")[col].median()
            phys_wc[col] = {
                wc: float(m) for wc, m in wc_medians.items() if pd.notna(m)
            }
            filled = dataset[col].fillna(dataset["weight_class"].map(wc_medians))
            phys_global[col] = float(filled.median())

        hist_rate = {
            col: float(dataset[col].median()) for col in _suffixed(HIST_RATE_BASE)
        }
        return cls(phys_wc, phys_global, hist_rate)

    def transform(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """Return a copy with all feature columns imputed and diffs recomputed."""
        dataset = dataset.copy()

        for col, wc_map in self.phys_wc_medians.items():
            dataset[col] = dataset[col].fillna(dataset["weight_class"].map(wc_map))
            fallback = self.phys_global_medians[col]
            if not math.isnan(fallback):
                dataset[col] = dataset[col].fillna(fallback)

        for col in _suffixed(HIST_COUNT_BASE):
            dataset[col] = dataset[col].fillna(0)

        for col, median in self.hist_rate_medians.items():
            if not math.isnan(median):
                dataset[col] = dataset[col].fillna(median)

        for col in _suffixed(CAREER_BASE):
            dataset[col] = dataset[col].fillna(0)

        dataset["stance_a"] = dataset["stance_a"].fillna("Unknown")
        dataset["stance_b"] = dataset["stance_b"].fillna("Unknown")

        for col in DIFF_PAIRS:
            dataset[f"{col}_diff"] = dataset[f"{col}_a"] - dataset[f"{col}_b"]

        return dataset

    def to_dict(self) -> dict:
        return {
            "phys_wc_medians": self.phys_wc_medians,
            "phys_global_medians": self.phys_global_medians,
            "hist_rate_medians": self.hist_rate_medians,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ImputationParams":
        return cls(
            phys_wc_medians=d["phys_wc_medians"],
            phys_global_medians=d["phys_global_medians"],
            hist_rate_medians=d["hist_rate_medians"],
        )
