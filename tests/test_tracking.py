import pandas as pd
import pytest

from ufc.tracking.reconcile import accuracy_summary, reconcile
from ufc.tracking.store import append_predictions, load_predictions, save_predictions

EVENT = {"event_id": "ev2025", "name": "UFC Test Night", "date": "2025-01-02"}


def _card_results():
    return [
        {  # f1 beat f8 at ft8 (2025-01-01) — pair matches within +/-3 days
            "status": "ok", "model_version": "v1",
            "fighter_a_id": "f8", "fighter_a_name": "Orphan Onyx",
            "fighter_b_id": "f1", "fighter_b_name": "Alpha Ape",
            "weight_class": "Lightweight", "prob_a": 0.30,
            "predicted_winner_id": "f1",
        },
        {  # f6 beat f4 at ft9 — predicted wrong
            "status": "ok", "model_version": "v1",
            "fighter_a_id": "f4", "fighter_a_name": "Bruno Silva",
            "fighter_b_id": "f6", "fighter_b_name": "Delta Dog",
            "weight_class": "Middleweight", "prob_a": 0.80,
            "predicted_winner_id": "f4",
        },
        {  # debut fighter — recorded as skipped
            "status": "skipped_debut", "reason": "No data for fighter 'f_new'",
            "fighter_a_id": "f_new", "fighter_a_name": "New Guy",
            "fighter_b_id": "f2", "fighter_b_name": "Bravo Bear",
            "weight_class": "Lightweight",
        },
    ]


@pytest.fixture
def store_path(tmp_path):
    return tmp_path / "predictions.csv"


def test_append_and_load_roundtrip(store_path):
    added = append_predictions(_card_results(), EVENT, store_path)
    assert added == 3
    df = load_predictions(store_path)
    assert len(df) == 3
    assert set(df["status"]) == {"pending", "skipped_debut"}
    assert (df["event_id"] == "ev2025").all()
    assert df["prediction_id"].is_unique


def test_append_is_idempotent_per_pair(store_path):
    append_predictions(_card_results(), EVENT, store_path)
    added = append_predictions(_card_results(), EVENT, store_path)
    assert added == 0
    # same pair with sides swapped is still a duplicate
    swapped = _card_results()[0].copy()
    swapped["fighter_a_id"], swapped["fighter_b_id"] = (
        swapped["fighter_b_id"], swapped["fighter_a_id"],
    )
    assert append_predictions([swapped], EVENT, store_path) == 0


def test_reconcile_unordered_pair_and_accuracy(store_path, raw_dir):
    append_predictions(_card_results(), EVENT, store_path)
    df = load_predictions(store_path)

    df, matched = reconcile(df, raw_dir)
    assert matched == 2

    hit = df[df["fighter_a_id"] == "f8"].iloc[0]
    assert hit["status"] == "reconciled"
    assert hit["actual_winner_id"] == "f1"
    assert bool(hit["correct"]) is True
    assert hit["actual_method"] == "U-DEC"

    miss = df[df["fighter_a_id"] == "f4"].iloc[0]
    assert miss["status"] == "reconciled"
    assert bool(miss["correct"]) is False

    # skipped rows untouched
    assert (df[df["fighter_a_id"] == "f_new"]["status"] == "skipped_debut").all()

    stats = accuracy_summary(df)
    assert stats["n"] == 2
    assert stats["accuracy"] == 0.5
    # brier: mean((0.30-0)^2, (0.80-0)^2)  [f8 lost, f4 lost]
    assert abs(stats["brier"] - ((0.30**2 + 0.80**2) / 2)) < 1e-12
    assert stats["by_model"]["v1"]["n"] == 2


def test_reconcile_is_idempotent(store_path, raw_dir):
    append_predictions(_card_results(), EVENT, store_path)
    df = load_predictions(store_path)
    df, m1 = reconcile(df, raw_dir)
    save_predictions(df, store_path)
    df2, m2 = reconcile(load_predictions(store_path), raw_dir)
    assert m2 == 0
    assert (df2["status"] == df["status"]).all()


def test_reconcile_draw_is_no_result(store_path, raw_dir):
    # rebuild the raw fixture so the draw fight (ft7) is a decided pair;
    # instead predict the actual draw pair f1 vs f4 on 2023-03-01
    draw_event = {"event_id": "evdraw", "name": "Draw Night", "date": "2023-03-01"}
    bout = [{
        "status": "ok", "model_version": "v1",
        "fighter_a_id": "f1", "fighter_a_name": "Alpha Ape",
        "fighter_b_id": "f4", "fighter_b_name": "Bruno Silva",
        "weight_class": "Catch Weight", "prob_a": 0.6,
        "predicted_winner_id": "f1",
    }]
    append_predictions(bout, draw_event, store_path)
    df, matched = reconcile(load_predictions(store_path), raw_dir)
    assert matched == 1
    row = df.iloc[0]
    assert row["status"] == "no_result"
    assert pd.isna(row["actual_winner_id"])
    assert accuracy_summary(df)["n"] == 0


def test_no_match_outside_date_window(store_path, raw_dir):
    far_event = {"event_id": "evfar", "name": "Far Night", "date": "2025-02-01"}
    bout = [{
        "status": "ok", "model_version": "v1",
        "fighter_a_id": "f8", "fighter_a_name": "Orphan Onyx",
        "fighter_b_id": "f1", "fighter_b_name": "Alpha Ape",
        "weight_class": "Lightweight", "prob_a": 0.3,
        "predicted_winner_id": "f1",
    }]
    append_predictions(bout, far_event, store_path)
    df, matched = reconcile(load_predictions(store_path), raw_dir)
    assert matched == 0
    assert (df["status"] == "pending").all()
