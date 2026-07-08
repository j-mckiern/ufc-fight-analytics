"""Review predictions vs actual results; approve retraining on the new data."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import clear_model_caches

from ufc.scraping.pipeline import scrape_new
from ufc.tracking.reconcile import accuracy_summary, reconcile
from ufc.tracking.retrain import approve_and_retrain
from ufc.tracking.store import load_predictions, save_predictions

st.title("✅ Review & Retrain")

predictions = load_predictions()
if len(predictions) == 0:
    st.info("No saved predictions yet. Save a card from the Fight Card page first.")
    st.stop()

pending = predictions[predictions["status"] == "pending"]
st.metric("Pending predictions", len(pending))

if st.button("🔄 Fetch results & reconcile", type="primary",
             help="Scrapes new events from ufcstats.com, then matches pending "
                  "predictions to actual results."):
    progress_box = st.status("Scraping new results…", expanded=True)

    def progress(msg="", end="\n", flush=False):
        text = str(msg).strip()
        if text and not text.startswith("\r"):
            progress_box.write(text)

    summary = scrape_new(progress=progress)
    progress_box.update(label="Reconciling…")
    updated, matched = reconcile(load_predictions())
    save_predictions(updated)
    progress_box.update(
        label=f"Scraped {summary['new_events']} new events; "
              f"matched {matched} predictions.",
        state="complete", expanded=False,
    )
    st.rerun()

# ---- results table -----------------------------------------------------------
reconciled = predictions[predictions["status"].isin(["reconciled", "no_result"])]
if len(reconciled):
    st.subheader("Predicted vs actual")
    for event_id, grp in reconciled.groupby("event_id", sort=False):
        first = grp.iloc[0]
        n_correct = int(grp["correct"].fillna(False).sum())
        n_scored = int((grp["status"] == "reconciled").sum())
        with st.expander(
            f"{first['event_name']} — {first['event_date']}  "
            f"({n_correct}/{n_scored} correct)",
            expanded=True,
        ):
            rows = []
            for _, r in grp.iterrows():
                if r["status"] == "no_result":
                    outcome, winner = "— excluded", r["actual_method"]
                else:
                    outcome = "✅ hit" if r["correct"] else "❌ miss"
                    winner = (
                        r["fighter_a_name"]
                        if str(r["actual_winner_id"]) == str(r["fighter_a_id"])
                        else r["fighter_b_name"]
                    )
                predicted = (
                    r["fighter_a_name"]
                    if str(r["predicted_winner_id"]) == str(r["fighter_a_id"])
                    else r["fighter_b_name"]
                )
                rows.append({
                    "Bout": f"{r['fighter_a_name']} vs {r['fighter_b_name']}",
                    "Predicted": f"{predicted} ({max(r['prob_a'], 1 - r['prob_a']):.0%})",
                    "Actual": f"{winner} ({r['actual_method']})",
                    "Outcome": outcome,
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

stats = accuracy_summary(predictions)
if stats["n"]:
    col1, col2, col3 = st.columns(3)
    col1.metric("Lifetime accuracy", f"{stats['accuracy']:.1%}")
    col2.metric("Brier score", f"{stats['brier']:.3f}")
    col3.metric("Scored predictions", stats["n"])

skipped = predictions[predictions["status"] == "skipped_debut"]
if len(skipped):
    st.caption(f"{len(skipped)} bouts skipped (debut fighters, no prediction made).")

# ---- retrain -----------------------------------------------------------------
st.divider()
st.subheader("Retrain")
st.write(
    "Retraining rebuilds the training set from everything in `data/raw` "
    "(including newly scraped fights) and archives the current model."
)
if st.button("🔁 Approve & retrain model"):
    with st.spinner("Rebuilding features and training candidates…"):
        outcome = approve_and_retrain()
    clear_model_caches()
    old, new = outcome["old"], outcome["new"]
    st.success(f"New model: {new['model_name']} (version {new['trained_at']})")

    def _metrics_row(meta):
        if not meta:
            return None
        m = meta["metrics"][meta["model_name"]]
        return {
            "model": meta["model_name"],
            "trained": meta["trained_at"],
            "test accuracy": f"{m['accuracy']:.3f}",
            "test AUC": f"{m['auc']:.3f}",
            "train rows": meta["train_rows"],
        }

    comparison = [r for r in (_metrics_row(old), _metrics_row(new)) if r]
    st.dataframe(
        pd.DataFrame(comparison, index=["before", "after"][-len(comparison):]),
        width="stretch",
    )
