"""Loading the current model bundle (pipeline + prep params + metadata)."""

import json
from dataclasses import dataclass
from pathlib import Path

import joblib
from sklearn.pipeline import Pipeline

from ufc import config
from ufc.features.build import PrepParams


@dataclass
class ModelBundle:
    pipeline: Pipeline
    params: PrepParams
    metadata: dict

    @property
    def model_version(self) -> str:
        return self.metadata["trained_at"]

    @property
    def feature_cols(self) -> list[str]:
        return self.params.feature_cols


def load_model_bundle(model_dir: Path | None = None) -> ModelBundle:
    model_dir = model_dir if model_dir is not None else config.CURRENT_MODEL_DIR
    model_path = model_dir / "model.joblib"
    if not model_path.exists():
        raise FileNotFoundError(
            f"No model bundle at {model_dir}. Run `ufc train` first."
        )
    pipeline = joblib.load(model_path)
    with open(model_dir / "metadata.json") as f:
        metadata = json.load(f)
    params = PrepParams.from_dict(metadata["prep_params"])
    return ModelBundle(pipeline=pipeline, params=params, metadata=metadata)
