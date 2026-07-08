import json

import pytest

from ufc.features.build import build_training_data
from ufc.modeling.predict import (
    FighterNotFoundError,
    build_feature_row,
    get_fighter_by_id,
    predict_card,
    predict_matchup,
)
from ufc.modeling.registry import load_model_bundle
from ufc.modeling.train import train_and_save


from conftest import write_raw_dir


@pytest.fixture(scope="module")
def trained(tmp_path_factory):
    # module-scoped: build + train once for all prediction tests
    raw = write_raw_dir(tmp_path_factory.mktemp("data") / "raw")
    data = build_training_data(raw_dir=raw, cutoff="2024-01-01")
    model_dir = tmp_path_factory.mktemp("models") / "current"
    train_and_save(data, model_dir=model_dir, archive_dir=model_dir.parent / "archive")
    bundle = load_model_bundle(model_dir)
    return bundle, data.fighter_features, model_dir


def test_feature_row_matches_training_columns(trained):
    bundle, ff, _ = trained
    row = build_feature_row(
        get_fighter_by_id(ff, "f1"),
        get_fighter_by_id(ff, "f2"),
        "Lightweight",
        bundle,
    )
    assert list(row.columns) == bundle.params.feature_cols
    assert row.isnull().sum().sum() == 0


def test_predict_matchup_probabilities(trained):
    bundle, ff, _ = trained
    result = predict_matchup(bundle, ff, "f1", "f2", weight_class="Lightweight")
    assert 0.0 < result["prob_a"] < 1.0
    assert abs(result["prob_a"] + result["prob_b"] - 1.0) < 1e-12
    assert result["predicted_winner_id"] in {"f1", "f2"}
    assert result["model_version"] == bundle.model_version


def test_predict_matchup_debut_raises(trained):
    bundle, ff, _ = trained
    with pytest.raises(FighterNotFoundError):
        predict_matchup(bundle, ff, "f1", "f_debut", weight_class="Lightweight")


def test_predict_card_skips_debut_and_continues(trained):
    bundle, ff, _ = trained
    bouts = [
        {"fighter_a_id": "f1", "fighter_b_id": "f2", "weight_class": "Lightweight"},
        {"fighter_a_id": "f_debut", "fighter_b_id": "f3",
         "fighter_a_name": "New Guy", "weight_class": "Lightweight"},
        {"fighter_a_id": "f4", "fighter_b_id": "f6", "weight_class": "Middleweight"},
    ]
    results = predict_card(bundle, ff, bouts)
    assert [r["status"] for r in results] == ["ok", "skipped_debut", "ok"]
    skipped = results[1]
    assert "debut" in skipped["reason"]
    assert skipped["fighter_a_name"] == "New Guy"
    assert "prob_a" not in skipped


def test_archive_on_retrain(trained):
    bundle, ff, model_dir = trained
    raw = write_raw_dir(model_dir.parent / "raw2")
    data = build_training_data(raw_dir=raw, cutoff="2024-01-01")
    train_and_save(data, model_dir=model_dir, archive_dir=model_dir.parent / "archive")

    archived = list((model_dir.parent / "archive").iterdir())
    assert len(archived) == 1
    with open(archived[0] / "metadata.json") as f:
        assert json.load(f)["trained_at"] == bundle.model_version
    # new bundle still loads
    assert load_model_bundle(model_dir).pipeline is not None
