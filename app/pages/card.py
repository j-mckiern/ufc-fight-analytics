"""Upcoming fight-card prediction page: scrape, predict, save for tracking."""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_bundle, get_fighter_features, probability_bar

from ufc.modeling.predict import predict_card
from ufc.scraping.http import make_session
from ufc.scraping.upcoming import scrape_upcoming_card, scrape_upcoming_events
from ufc.tracking.store import append_predictions, load_predictions

st.title("🗓️ Upcoming Fight Card")


@st.cache_data(ttl=3600, show_spinner="Fetching upcoming events…")
def upcoming_events() -> list[dict]:
    return scrape_upcoming_events(make_session())


@st.cache_data(ttl=3600, show_spinner="Fetching fight card…")
def upcoming_card(event_url: str) -> list[dict]:
    return scrape_upcoming_card(event_url, make_session())


events = upcoming_events()
if not events:
    st.info("No upcoming events found on ufcstats.com.")
    st.stop()

event = st.selectbox(
    "Event",
    events,
    format_func=lambda e: f"{e['name']} — {e['date']}",
)

bouts = upcoming_card(event["url"])
if not bouts:
    st.info("No bouts listed for this event yet.")
    st.stop()

bundle = get_bundle()
ff = get_fighter_features()
results = predict_card(bundle, ff, bouts, event_date=event["date"])

st.subheader(f"{event['name']} — {event['date']}")
for r in results:
    if r["status"] == "ok":
        probability_bar(r["fighter_a_name"], r["fighter_b_name"], r["prob_a"])
        st.caption(r["weight_class"])
    else:
        st.warning(
            f"{r['fighter_a_name']} vs {r['fighter_b_name']}: no prediction — "
            f"debut fighter (no data yet)."
        )

st.divider()
existing = load_predictions()
already = int((existing["event_id"] == event["event_id"]).sum())
if already:
    st.caption(f"{already} predictions already saved for this event.")

if st.button("💾 Save predictions for tracking", type="primary"):
    added = append_predictions(results, event)
    if added:
        st.success(f"Saved {added} predictions. Review them after the event "
                   "on the Review & Retrain page.")
    else:
        st.info("These bouts are already saved.")
