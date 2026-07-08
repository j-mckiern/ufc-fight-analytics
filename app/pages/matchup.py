"""Single-matchup prediction page."""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (
    fighter_selectbox,
    get_bundle,
    get_fighter_features,
    probability_bar,
    weight_class_options,
)

from ufc.modeling.predict import FighterNotFoundError, predict_matchup

st.title("🥊 Matchup Predictor")

bundle = get_bundle()
ff = get_fighter_features()

col_a, col_vs, col_b = st.columns([5, 1, 5])
with col_a:
    fighter_a = fighter_selectbox("Fighter A", ff, key="matchup_a")
with col_vs:
    st.markdown("<div style='text-align:center; padding-top:2.2em'><b>vs</b></div>",
                unsafe_allow_html=True)
with col_b:
    fighter_b = fighter_selectbox("Fighter B", ff, key="matchup_b")

weight_class = st.selectbox(
    "Weight class", weight_class_options(bundle), index=None,
    placeholder="Select weight class (improves the prediction)",
)

if st.button("Predict", type="primary", disabled=not (fighter_a and fighter_b)):
    if fighter_a == fighter_b:
        st.warning("Pick two different fighters.")
    else:
        try:
            result = predict_matchup(bundle, ff, fighter_a, fighter_b, weight_class)
        except FighterNotFoundError as exc:
            st.warning(f"No prediction: {exc}")
        else:
            st.divider()
            probability_bar(
                result["fighter_a_name"], result["fighter_b_name"], result["prob_a"]
            )
            st.markdown(
                f"### Predicted winner: {result['predicted_winner_name']} "
                f"({result['confidence']:.1%} confidence)"
            )
            st.caption(f"Model version {result['model_version']}")
