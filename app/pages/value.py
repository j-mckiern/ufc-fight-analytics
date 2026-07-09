"""Value Bets: compare the model against a sportsbook's odds to find +EV bets.

Predictions come from the same pipeline as the Fight Card page (ufcstats card →
`predict_card`, giving real fighter ids and weight classes). Odds come live from
fightodds.io. For each fight we convert the selected book's American price to an
implied probability and flag a side when the model's probability beats it.
"""

import datetime as dt
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_bundle, get_fighter_features

from ufc.modeling.lookup import find_fighters
from ufc.modeling.predict import FighterNotFoundError, predict_card, predict_matchup
from ufc.modeling.value import match_odds_to_predictions, sort_key_by_value
from ufc.scraping.http import make_session
from ufc.scraping.odds import fetch_event_odds, fetch_upcoming_odds_events
from ufc.scraping.upcoming import scrape_upcoming_card, scrape_upcoming_events

st.title("💰 Value Bets")
st.caption(
    "Live odds from [fightodds.io](https://fightodds.io). A fight is flagged when "
    "the model's win probability beats the book's implied probability (the price, "
    "vig included). For entertainment/analysis — bet responsibly."
)


def _fmt_american(odds: float) -> str:
    return f"+{odds:.0f}" if odds > 0 else f"{odds:.0f}"


def _corner_cell(prob: float, ev: dict) -> str:
    """One fighter's model%, book odds, implied%, and edge for the table."""
    sign = "+" if ev["edge"] >= 0 else "−"
    return (f"{prob:.0%} · {_fmt_american(ev['odds'])} · "
            f"{ev['implied_prob']:.0%} · {sign}{abs(ev['edge']) * 100:.1f}")


@st.cache_data(ttl=1800, show_spinner="Fetching upcoming events with odds…")
def odds_events() -> list[dict]:
    today = dt.date.today().isoformat()
    return fetch_upcoming_odds_events(make_session(), today)


@st.cache_data(ttl=900, show_spinner="Fetching odds…")
def event_odds(event_pk: int) -> dict:
    return fetch_event_odds(make_session(), event_pk)


@st.cache_data(ttl=3600, show_spinner="Matching ufcstats card…")
def ufcstats_upcoming() -> list[dict]:
    return scrape_upcoming_events(make_session())


@st.cache_data(ttl=3600, show_spinner="Fetching fight card…")
def ufcstats_card(event_url: str) -> list[dict]:
    return scrape_upcoming_card(event_url, make_session())


def predictions_for_event(event: dict, odds_fights: list[dict]) -> list[dict]:
    """Predict a card, preferring the ufcstats source (ids + weight class).

    Matches the fightodds event to a ufcstats upcoming event by date; falls back
    to predicting from fightodds fighter names (weight class unknown) when
    ufcstats hasn't posted the card yet.
    """
    bundle, ff = get_bundle(), get_fighter_features()

    match = next(
        (e for e in ufcstats_upcoming() if e["date"] == event["date"]), None
    )
    if match is not None:
        bouts = ufcstats_card(match["url"])
        if bouts:
            return predict_card(bundle, ff, bouts, event_date=event["date"])

    # Fallback: resolve fighter ids from fightodds names (no weight class).
    results = []
    for fight in odds_fights:
        base = {
            "fighter_a_name": fight["fighter1_name"],
            "fighter_b_name": fight["fighter2_name"],
            "weight_class": None,
        }
        ids = []
        for name in (fight["fighter1_name"], fight["fighter2_name"]):
            cands = find_fighters(ff, name)
            ids.append(cands.iloc[0]["fighter_id"] if len(cands) == 1 else None)
        if None in ids:
            results.append({**base, "status": "skipped_debut",
                            "reason": "fighter not found / ambiguous"})
            continue
        try:
            results.append({
                **predict_matchup(bundle, ff, ids[0], ids[1],
                                  event_date=event["date"]),
                "status": "ok",
            })
        except FighterNotFoundError as exc:
            results.append({**base, "status": "skipped_debut", "reason": str(exc)})
    return results


# ---- event & sportsbook selection -------------------------------------------
events = odds_events()
if not events:
    st.info("No upcoming UFC events with odds found on fightodds.io.")
    st.stop()

event = st.selectbox(
    "Event", events, format_func=lambda e: f"{e['name']} — {e['date']}"
)

odds = event_odds(event["pk"])
if not odds["fights"]:
    st.info("No odds posted for this event yet.")
    st.stop()

books = odds["sportsbooks"]
# Prefer a mainstream US book as the default if present.
default_idx = next(
    (i for i, b in enumerate(books) if b["slug"] == "draftkings"), 0
)
col_book, col_edge = st.columns([2, 3])
book = col_book.selectbox(
    "Sportsbook", books, index=default_idx, format_func=lambda b: b["short_name"]
)
min_edge = col_edge.slider(
    "Minimum edge to flag a bet (percentage points)",
    min_value=0.0, max_value=15.0, value=0.0, step=0.5,
    help="Model probability must beat the book's implied probability by at least "
         "this margin before the fight is flagged as a value bet.",
) / 100.0

# ---- predict + match ---------------------------------------------------------
predictions = predictions_for_event(event, odds["fights"])
rows = match_odds_to_predictions(predictions, odds["fights"], book["slug"], min_edge)
rows.sort(key=sort_key_by_value)

value_rows = [r for r in rows if r.get("value_side")]

st.subheader(f"{event['name']} — vs {book['short_name']}")

# ---- flagged value bets ------------------------------------------------------
if value_rows:
    st.markdown(f"### 🎯 {len(value_rows)} value bet(s)")
    for r in value_rows:
        ev = r["eval_a"] if r["value_side"] == "a" else r["eval_b"]
        prob = r["prob_a"] if r["value_side"] == "a" else r["prob_b"]
        st.success(
            f"**Bet {r['value_fighter']}** at "
            f"{_fmt_american(ev['odds'])} ({book['short_name']}) — "
            f"model {prob:.0%} vs implied {ev['implied_prob']:.0%} "
            f"(edge **+{ev['edge'] * 100:.1f} pts**, "
            f"EV **+${ev['ev']:.2f}** per $1)  \n"
            f"_{r['fighter_a_name']} vs {r['fighter_b_name']}_"
        )
else:
    st.info(
        "No value bets at this book above the current edge threshold. "
        "Try another sportsbook or lower the threshold."
    )

# ---- full comparison table ---------------------------------------------------
st.markdown("### All fights")
table_rows = []
for r in rows:
    if r["status"] != "ok":
        table_rows.append({
            "Fight": f"{r['fighter_a_name']} vs {r['fighter_b_name']}",
            "Model": "—",
            "Corner A": "—",
            "Corner B": "—",
            "Value bet": "debut / no data",
        })
        continue

    fav_a = r["prob_a"] >= r["prob_b"]
    model = (f"{r['fighter_a_name']} {r['prob_a']:.0%}" if fav_a
             else f"{r['fighter_b_name']} {r['prob_b']:.0%}")

    if r["odds_status"] == "ok":
        a_cell = _corner_cell(r["prob_a"], r["eval_a"])
        b_cell = _corner_cell(r["prob_b"], r["eval_b"])
        if r["value_side"]:
            ev = r["eval_a"] if r["value_side"] == "a" else r["eval_b"]
            bet = (f"✅ {r['value_fighter']} {_fmt_american(ev['odds'])} "
                   f"(EV +${ev['ev']:.2f})")
        else:
            bet = "—"
    else:
        reason = {"no_match": "not on fightodds", "cancelled": "cancelled",
                  "no_price": f"no {book['short_name']} line"}
        a_cell = b_cell = "—"
        bet = reason.get(r["odds_status"], r["odds_status"])

    table_rows.append({
        "Fight": f"{r['fighter_a_name']} vs {r['fighter_b_name']}",
        "Model": model,
        "Corner A": a_cell,
        "Corner B": b_cell,
        "Value bet": bet,
    })

st.dataframe(pd.DataFrame(table_rows), hide_index=True, width="stretch")
st.caption(
    "Corner cells show *model% · book odds · implied% · edge*. "
    "Edge = model probability − implied probability. "
    "EV is expected profit per $1 staked at the shown price."
)
