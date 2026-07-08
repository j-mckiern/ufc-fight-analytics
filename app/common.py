"""Shared cached loaders and UI helpers for the Streamlit pages."""

import pandas as pd
import streamlit as st

from ufc.features.build import load_fighter_features
from ufc.modeling.registry import ModelBundle, load_model_bundle


@st.cache_resource
def get_bundle() -> ModelBundle:
    return load_model_bundle()


@st.cache_data
def get_fighter_features() -> pd.DataFrame:
    return load_fighter_features()


def clear_model_caches() -> None:
    """Call after retraining so pages pick up the new bundle/features."""
    get_bundle.clear()
    get_fighter_features.clear()


def fighter_label(row: pd.Series) -> str:
    """Disambiguating display label (duplicate names exist in the UFC)."""
    record = f"{row['wins']:.0f}-{row['losses']:.0f}" if pd.notna(row["wins"]) else "?"
    weight = f", {row['weight_lb']:.0f} lb" if pd.notna(row["weight_lb"]) else ""
    return f"{row['name']} ({record}{weight})"


def fighter_selectbox(label: str, fighter_features: pd.DataFrame, key: str):
    """Searchable fighter picker returning a fighter_id (or None)."""
    ff = fighter_features.sort_values("name")
    ids = ff["fighter_id"].tolist()
    labels = dict(zip(ids, ff.apply(fighter_label, axis=1)))
    return st.selectbox(
        label,
        options=ids,
        format_func=lambda fid: labels.get(fid, fid),
        index=None,
        placeholder="Search fighters…",
        key=key,
    )


def probability_bar(name_a: str, name_b: str, prob_a: float) -> None:
    pct_a = prob_a * 100
    winner_a = prob_a > 0.5
    col_a, col_bar, col_b = st.columns([3, 6, 3])
    col_a.markdown(f"**{name_a}**" if winner_a else name_a)
    col_bar.progress(prob_a, text=f"{pct_a:.1f}% — {100 - pct_a:.1f}%")
    col_b.markdown(f"**{name_b}**" if not winner_a else name_b)


def weight_class_options(bundle: ModelBundle) -> list[str]:
    return sorted(bundle.params.kept_weight_classes) + ["Other"]
