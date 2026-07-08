"""UFC Fight Analytics — Streamlit UI entry point.

Run with: uv run streamlit run app/streamlit_app.py  (or `just app`)
"""

import streamlit as st

st.set_page_config(page_title="UFC Fight Analytics", page_icon="🥊", layout="wide")

pages = st.navigation([
    st.Page("pages/matchup.py", title="Matchup", icon="🥊", default=True),
    st.Page("pages/card.py", title="Fight Card", icon="🗓️"),
    st.Page("pages/review.py", title="Review & Retrain", icon="✅"),
    st.Page("pages/model_info.py", title="Model", icon="📊"),
])
pages.run()
