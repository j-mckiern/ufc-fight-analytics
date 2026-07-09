"""Betting-value math: compare model probabilities against sportsbook prices.

A prediction is only *actionable* if the model's win probability for a fighter
beats the price you can actually get on that fighter. We convert a book's
American moneyline odds to the implied probability it charges (vig included),
and flag a side when ``model_prob > implied_prob`` — i.e. positive expected
value at the offered price.

All functions here are pure (no I/O). ``match_odds_to_predictions`` joins the
model's card predictions (keyed on ufcstats fighters) to the fightodds feed
(keyed on fighter names) by normalized name, since the two sources share no ids.
"""

import unicodedata

import numpy as np


def american_to_decimal(odds: float) -> float:
    """American moneyline odds -> decimal odds (total return per 1 unit staked)."""
    if odds > 0:
        return 1.0 + odds / 100.0
    return 1.0 + 100.0 / abs(odds)


def american_to_implied_prob(odds: float) -> float:
    """American moneyline odds -> implied win probability (includes the vig)."""
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def expected_value(prob: float, odds: float) -> float:
    """Expected profit per 1 unit staked at ``odds`` given true win ``prob``.

    Positive means a long-run profitable bet. EV = p*(decimal-1) - (1-p).
    """
    return prob * (american_to_decimal(odds) - 1.0) - (1.0 - prob)


def evaluate_side(model_prob: float, odds: float) -> dict:
    """Value metrics for backing one fighter at ``odds`` with model ``model_prob``."""
    implied = american_to_implied_prob(odds)
    return {
        "odds": odds,
        "implied_prob": implied,
        "decimal_odds": american_to_decimal(odds),
        "edge": model_prob - implied,
        "ev": expected_value(model_prob, odds),
    }


def normalize_name(name: str) -> str:
    """Casefold + strip accents/punctuation for cross-source name matching."""
    if not name:
        return ""
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = decomposed.encode("ascii", "ignore").decode("ascii")
    cleaned = "".join(ch if ch.isalnum() else " " for ch in ascii_only.lower())
    return " ".join(cleaned.split())


def _orient_offer(pred: dict, fight: dict, book_slug: str) -> tuple | None:
    """Return (odds_a, odds_b) for a predicted bout from a matched odds fight.

    Tries both fighter orderings; returns None if the names don't match or the
    selected book has no price for this fight.
    """
    a, b = normalize_name(pred["fighter_a_name"]), normalize_name(pred["fighter_b_name"])
    f1 = normalize_name(fight["fighter1_name"])
    f2 = normalize_name(fight["fighter2_name"])
    offer = fight.get("offers", {}).get(book_slug)
    if offer is None:
        return None
    if {a, b} != {f1, f2} or a == b:
        return None
    # offer.odds1 belongs to fighter1, odds2 to fighter2 — orient to A/B.
    if a == f1:
        return offer["odds1"], offer["odds2"]
    return offer["odds2"], offer["odds1"]


def _find_fight(pred: dict, odds_fights: list[dict]) -> dict | None:
    a, b = normalize_name(pred["fighter_a_name"]), normalize_name(pred["fighter_b_name"])
    for fight in odds_fights:
        pair = {normalize_name(fight["fighter1_name"]),
                normalize_name(fight["fighter2_name"])}
        if pair == {a, b} and a != b:
            return fight
    return None


def match_odds_to_predictions(
    predictions: list[dict],
    odds_fights: list[dict],
    book_slug: str,
    min_edge: float = 0.0,
) -> list[dict]:
    """Join card predictions to one book's odds and score each bout for value.

    ``predictions`` is the output of ``predict_card``; ``odds_fights`` is the
    ``fights`` list from ``fetch_event_odds``. Returns one row per prediction
    (order preserved) with the model probabilities, the selected book's odds,
    per-corner value metrics, and the recommended +EV side (if any).

    ``odds_status`` is one of: ``ok`` (odds attached), ``cancelled`` (bout
    pulled by the book), ``no_price`` (book has no line / one side missing),
    ``no_match`` (fight not found in the feed).
    """
    rows: list[dict] = []
    for pred in predictions:
        row = {
            "fighter_a_name": pred.get("fighter_a_name"),
            "fighter_b_name": pred.get("fighter_b_name"),
            "weight_class": pred.get("weight_class"),
            "status": pred.get("status", "ok"),
            "prob_a": pred.get("prob_a"),
            "prob_b": pred.get("prob_b"),
            "odds_status": "no_match",
            "eval_a": None,
            "eval_b": None,
            "value_side": None,
            "value_fighter": None,
            "value_ev": None,
        }

        # Only bouts the model actually predicted can be scored for value.
        if pred.get("status") == "ok":
            fight = _find_fight(pred, odds_fights)
            if fight is None:
                pass  # keep no_match
            elif fight.get("is_cancelled"):
                row["odds_status"] = "cancelled"
            else:
                oriented = _orient_offer(pred, fight, book_slug)
                if oriented is None or oriented[0] is None or oriented[1] is None:
                    row["odds_status"] = "no_price"
                else:
                    odds_a, odds_b = oriented
                    row["odds_status"] = "ok"
                    row["eval_a"] = evaluate_side(pred["prob_a"], odds_a)
                    row["eval_b"] = evaluate_side(pred["prob_b"], odds_b)
                    row.update(_pick_value_side(pred, row["eval_a"], row["eval_b"],
                                                min_edge))
        rows.append(row)
    return rows


def _pick_value_side(pred: dict, eval_a: dict, eval_b: dict, min_edge: float) -> dict:
    """Choose the +EV corner (best edge above threshold), if either qualifies."""
    candidates = [
        ("a", pred["fighter_a_name"], eval_a),
        ("b", pred["fighter_b_name"], eval_b),
    ]
    qualified = [
        c for c in candidates if c[2]["ev"] > 0 and c[2]["edge"] >= min_edge
    ]
    if not qualified:
        return {}
    side, fighter, ev_dict = max(qualified, key=lambda c: c[2]["edge"])
    return {"value_side": side, "value_fighter": fighter, "value_ev": ev_dict["ev"]}


def sort_key_by_value(row: dict) -> float:
    """Sort helper: best value first, unscored rows last."""
    if row.get("value_ev") is not None:
        return -float(row["value_ev"])
    return np.inf
