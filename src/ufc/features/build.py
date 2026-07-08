"""End-to-end training-data preparation from raw scraped CSVs."""

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ufc import config
from ufc.features.cleaning import clean_fight_data
from ufc.features.encode import (
    collapse_rare_weight_classes,
    derive_feature_cols,
    encode_categoricals,
    fit_weight_class_categories,
)
from ufc.features.history import HIST_FEATURES, compute_fighter_history
from ufc.features.impute import HIST_COUNT_BASE, HIST_RATE_BASE, ImputationParams
from ufc.features.matchups import assign_ab, build_matchups, merge_matchup_features


@dataclass
class PrepParams:
    """Everything prediction needs to reproduce the training feature space."""

    impute: ImputationParams
    kept_weight_classes: list[str]
    feature_cols: list[str]
    cutoff: str
    seed: int

    def to_dict(self) -> dict:
        return {
            "impute": self.impute.to_dict(),
            "kept_weight_classes": self.kept_weight_classes,
            "feature_cols": self.feature_cols,
            "cutoff": self.cutoff,
            "seed": self.seed,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PrepParams":
        return cls(
            impute=ImputationParams.from_dict(d["impute"]),
            kept_weight_classes=d["kept_weight_classes"],
            feature_cols=d["feature_cols"],
            cutoff=d["cutoff"],
            seed=d["seed"],
        )


@dataclass
class TrainingData:
    dataset: pd.DataFrame
    train: pd.DataFrame
    test: pd.DataFrame
    fighter_features: pd.DataFrame
    params: PrepParams


def load_raw(raw_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fighters = pd.read_csv(raw_dir / "fighters.csv")
    fights = pd.read_csv(raw_dir / "fights.csv", parse_dates=["event_date"])
    fight_stats = pd.read_csv(raw_dir / "fight_stats.csv")
    return fighters, fights, fight_stats


def build_fighter_features(
    fighters: pd.DataFrame,
    fighter_hist: pd.DataFrame,
    impute_rates: bool = False,
) -> pd.DataFrame:
    """Latest pre-fight historical snapshot per fighter, joined to profiles.

    Count features are filled with 0 (matches training imputation for debut
    rows). Rate features are left NaN by default so the shared
    ImputationParams fills them at prediction time exactly as in training;
    impute_rates=True reproduces the legacy notebook behavior instead.
    """
    latest_hist = (
        fighter_hist.sort_values("event_date").groupby("fighter_id").last().reset_index()
    )
    fighter_features = fighters.merge(
        latest_hist[["fighter_id"] + HIST_FEATURES], on="fighter_id", how="left"
    )
    for col in HIST_COUNT_BASE:
        fighter_features[col] = fighter_features[col].fillna(0)
    if impute_rates:
        for col in HIST_RATE_BASE:
            fighter_features[col] = fighter_features[col].fillna(
                fighter_features[col].median()
            )
    return fighter_features


def build_training_data(
    raw_dir: Path | None = None,
    seed: int = config.SEED,
    cutoff: str | pd.Timestamp | None = None,
    impute_fit: str = "train",
    legacy_parity: bool = False,
) -> TrainingData:
    """Build the full training dataset from raw CSVs.

    cutoff: temporal train/test split point; defaults to the newest event date
    minus config.TEST_WINDOW_MONTHS.
    impute_fit: "train" fits imputation/encoding parameters on pre-cutoff rows
    only (leak-free); "all" fits on the full dataset (legacy notebook behavior).
    legacy_parity: also reproduces the notebook's imputed fighter_features table.
    """
    raw_dir = Path(raw_dir) if raw_dir is not None else config.RAW_DIR
    fighters, fights, fight_stats = load_raw(raw_dir)

    fights_clean, fight_stats_clean = clean_fight_data(fights, fight_stats)
    matchups = assign_ab(build_matchups(fight_stats_clean, fights_clean), seed=seed)
    fighter_hist = compute_fighter_history(fight_stats_clean, fights_clean)
    dataset = merge_matchup_features(matchups, fighters, fighter_hist)

    if cutoff is None:
        cutoff = dataset["event_date"].max() - pd.DateOffset(
            months=config.TEST_WINDOW_MONTHS
        )
    cutoff = pd.Timestamp(cutoff)

    fit_df = dataset if impute_fit == "all" else dataset[dataset["event_date"] < cutoff]
    impute_params = ImputationParams.fit(fit_df)
    dataset = impute_params.transform(dataset)

    kept_classes = fit_weight_class_categories(fit_df)
    dataset = collapse_rare_weight_classes(dataset, kept_classes)
    dataset = encode_categoricals(dataset)

    feature_cols = derive_feature_cols(dataset)
    null_counts = dataset[feature_cols].isnull().sum()
    assert null_counts.sum() == 0, f"Nulls in features:\n{null_counts[null_counts > 0]}"

    train = dataset[dataset["event_date"] < cutoff].copy()
    test = dataset[dataset["event_date"] >= cutoff].copy()

    fighter_features = build_fighter_features(
        fighters, fighter_hist, impute_rates=legacy_parity
    )

    params = PrepParams(
        impute=impute_params,
        kept_weight_classes=kept_classes,
        feature_cols=feature_cols,
        cutoff=str(cutoff.date()),
        seed=seed,
    )
    return TrainingData(dataset, train, test, fighter_features, params)


def save_prepared(data: TrainingData, prepared_dir: Path | None = None) -> Path:
    prepared_dir = (
        Path(prepared_dir) if prepared_dir is not None else config.PREPARED_DIR
    )
    prepared_dir.mkdir(parents=True, exist_ok=True)

    feature_cols = data.params.feature_cols
    data.train[feature_cols + ["target"]].to_csv(prepared_dir / "train.csv", index=False)
    data.test[feature_cols + ["target"]].to_csv(prepared_dir / "test.csv", index=False)
    data.dataset.to_csv(prepared_dir / "matchups_full.csv", index=False)
    data.fighter_features.to_csv(prepared_dir / "fighter_features.csv", index=False)
    pd.Series(feature_cols).to_csv(
        prepared_dir / "feature_cols.csv", index=False, header=False
    )
    with open(prepared_dir / "prep_params.json", "w") as f:
        json.dump(data.params.to_dict(), f, indent=2)
    return prepared_dir
