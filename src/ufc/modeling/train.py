"""Model training: compare candidates, keep the best, archive the previous bundle.

The saved artifact is always a sklearn Pipeline (scaler included where needed),
so prediction never has to know which estimator won or how to scale inputs.
"""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import joblib
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from ufc import config
from ufc.features.build import TrainingData


def candidate_models(seed: int = config.SEED) -> dict[str, Pipeline]:
    return {
        "logistic_regression": Pipeline([
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(max_iter=1000, random_state=seed)),
        ]),
        "random_forest": Pipeline([
            ("model", RandomForestClassifier(
                n_estimators=300, max_depth=10, random_state=seed
            )),
        ]),
        "gradient_boosting": Pipeline([
            ("model", GradientBoostingClassifier(
                n_estimators=300, max_depth=4, learning_rate=0.05, random_state=seed
            )),
        ]),
    }


def archive_current(
    current_dir: Path | None = None, archive_dir: Path | None = None
) -> Path | None:
    """Move the current model bundle into the archive. Returns the new path."""
    current_dir = current_dir if current_dir is not None else config.CURRENT_MODEL_DIR
    archive_dir = archive_dir if archive_dir is not None else config.ARCHIVE_MODEL_DIR
    if not (current_dir / "model.joblib").exists():
        return None

    trained_at = "unknown"
    meta_path = current_dir / "metadata.json"
    if meta_path.exists():
        with open(meta_path) as f:
            trained_at = json.load(f).get("trained_at", "unknown")

    dest = archive_dir / trained_at.replace(":", "-")
    archive_dir.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir()
    for item in list(current_dir.iterdir()):
        shutil.move(str(item), dest / item.name)
    return dest


def train_and_save(
    data: TrainingData,
    model_dir: Path | None = None,
    archive_dir: Path | None = None,
    seed: int = config.SEED,
) -> dict:
    """Train all candidates, save the best (by test AUC) as the current bundle.

    Returns the metadata dict (includes per-model metrics).
    """
    model_dir = model_dir if model_dir is not None else config.CURRENT_MODEL_DIR

    feature_cols = data.params.feature_cols
    X_train, y_train = data.train[feature_cols], data.train["target"]
    X_test, y_test = data.test[feature_cols], data.test["target"]

    metrics: dict[str, dict] = {}
    fitted: dict[str, Pipeline] = {}
    for name, pipeline in candidate_models(seed).items():
        pipeline.fit(X_train, y_train)
        y_prob = pipeline.predict_proba(X_test)[:, 1]
        try:
            auc = float(roc_auc_score(y_test, y_prob))
        except ValueError:  # single-class test set (only tiny datasets)
            auc = float("nan")
        metrics[name] = {
            "accuracy": float(accuracy_score(y_test, y_prob > 0.5)),
            "auc": auc,
            "log_loss": float(log_loss(y_test, y_prob, labels=[0, 1])),
        }
        fitted[name] = pipeline

    def _selection_score(name: str) -> float:
        auc = metrics[name]["auc"]
        return auc if auc == auc else metrics[name]["accuracy"]

    best_name = max(metrics, key=_selection_score)
    trained_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")

    metadata = {
        "trained_at": trained_at,
        "model_name": best_name,
        "metrics": metrics,
        "train_rows": len(data.train),
        "test_rows": len(data.test),
        "cutoff": data.params.cutoff,
        "prep_params": data.params.to_dict(),
    }

    archive_current(model_dir, archive_dir)
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(fitted[best_name], model_dir / "model.joblib")
    with open(model_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    return metadata
