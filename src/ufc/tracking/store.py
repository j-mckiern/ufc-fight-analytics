"""Durable predictions store: data/predictions.csv.

One row per predicted bout. status lifecycle:
  pending       -> prediction saved, result unknown yet
  reconciled    -> matched to a scraped result (correct filled in)
  no_result     -> matched to a draw/NC (excluded from accuracy)
  skipped_debut -> bout involved a fighter with no data; recorded for review
"""

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ufc import config

COLUMNS = [
    "prediction_id", "created_at", "model_version",
    "event_id", "event_name", "event_date",
    "fighter_a_id", "fighter_a_name", "fighter_b_id", "fighter_b_name",
    "weight_class", "prob_a", "predicted_winner_id",
    "status", "actual_winner_id", "actual_method", "correct", "reconciled_at",
]


def load_predictions(path: Path | None = None) -> pd.DataFrame:
    path = path if path is not None else config.PREDICTIONS_CSV
    if not path.exists():
        return pd.DataFrame(columns=COLUMNS)
    return pd.read_csv(path, dtype={"correct": "boolean"})


def save_predictions(df: pd.DataFrame, path: Path | None = None) -> None:
    path = path if path is not None else config.PREDICTIONS_CSV
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".csv.tmp")
    df[COLUMNS].to_csv(tmp, index=False)
    tmp.replace(path)


def _pair_key(a_id, b_id) -> tuple:
    return tuple(sorted([str(a_id), str(b_id)]))


def append_predictions(
    results: list[dict],
    event: dict,
    path: Path | None = None,
) -> int:
    """Record predict_card() output for an event. Returns rows added.

    Bouts already stored for this event (same unordered fighter pair, any
    status) are skipped, so re-saving a card is safe.
    """
    existing = load_predictions(path)
    event_rows = existing[existing["event_id"] == event["event_id"]]
    seen_pairs = {
        _pair_key(r["fighter_a_id"], r["fighter_b_id"])
        for _, r in event_rows.iterrows()
    }

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_rows = []
    for r in results:
        if _pair_key(r["fighter_a_id"], r["fighter_b_id"]) in seen_pairs:
            continue
        is_ok = r.get("status") == "ok"
        new_rows.append({
            "prediction_id": uuid.uuid4().hex[:12],
            "created_at": now,
            "model_version": r.get("model_version"),
            "event_id": event["event_id"],
            "event_name": event.get("name"),
            "event_date": event.get("date"),
            "fighter_a_id": r["fighter_a_id"],
            "fighter_a_name": r.get("fighter_a_name"),
            "fighter_b_id": r["fighter_b_id"],
            "fighter_b_name": r.get("fighter_b_name"),
            "weight_class": r.get("weight_class"),
            "prob_a": r.get("prob_a"),
            "predicted_winner_id": r.get("predicted_winner_id"),
            "status": "pending" if is_ok else "skipped_debut",
            "actual_winner_id": None,
            "actual_method": None,
            "correct": None,
            "reconciled_at": None,
        })

    if new_rows:
        combined = pd.concat(
            [existing, pd.DataFrame(new_rows, columns=COLUMNS)], ignore_index=True
        )
        save_predictions(combined, path)
    return len(new_rows)
