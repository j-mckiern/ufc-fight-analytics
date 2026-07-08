"""Current model metadata and candidate comparison."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_bundle

from ufc import config

st.title("📊 Model")

bundle = get_bundle()
meta = bundle.metadata

col1, col2, col3 = st.columns(3)
col1.metric("Model", meta["model_name"].replace("_", " "))
col2.metric("Version", meta["trained_at"])
col3.metric("Features", len(bundle.feature_cols))

st.caption(
    f"Trained on {meta['train_rows']} fights (before {meta['cutoff']}), "
    f"evaluated on {meta['test_rows']} later fights."
)

st.subheader("Candidate comparison (test set)")
rows = [
    {"model": name.replace("_", " "), **{k: round(v, 4) for k, v in m.items()}}
    for name, m in meta["metrics"].items()
]
st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

archive = config.ARCHIVE_MODEL_DIR
if archive.exists():
    versions = sorted((p.name for p in archive.iterdir() if p.is_dir()), reverse=True)
    if versions:
        st.subheader("Archived versions")
        for v in versions:
            st.text(v)
