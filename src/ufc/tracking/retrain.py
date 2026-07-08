"""The approve step: fold newly reconciled data in and retrain the model."""

from pathlib import Path

from ufc import config
from ufc.features.build import build_training_data, save_prepared
from ufc.modeling.train import train_and_save


def approve_and_retrain(
    raw_dir: Path | None = None,
    model_dir: Path | None = None,
    archive_dir: Path | None = None,
) -> dict:
    """Rebuild training data from data/raw and retrain; archives the old bundle.

    Returns {"old": old_metadata_or_None, "new": new_metadata} so callers can
    show a before/after comparison.
    """
    old_metadata = None
    model_dir = model_dir if model_dir is not None else config.CURRENT_MODEL_DIR
    meta_path = model_dir / "metadata.json"
    if meta_path.exists():
        import json

        with open(meta_path) as f:
            old_metadata = json.load(f)

    data = build_training_data(raw_dir=raw_dir)
    if raw_dir is None:
        save_prepared(data)
    new_metadata = train_and_save(data, model_dir=model_dir, archive_dir=archive_dir)
    return {"old": old_metadata, "new": new_metadata}
