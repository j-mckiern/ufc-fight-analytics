"""Betting-value math and odds/prediction matching."""

import pytest

from ufc.modeling.value import (
    american_to_decimal,
    american_to_implied_prob,
    evaluate_side,
    expected_value,
    match_odds_to_predictions,
    normalize_name,
)


def test_american_to_decimal():
    assert american_to_decimal(100) == pytest.approx(2.0)
    assert american_to_decimal(150) == pytest.approx(2.5)
    assert american_to_decimal(-200) == pytest.approx(1.5)
    assert american_to_decimal(-110) == pytest.approx(1.909, abs=1e-3)


def test_american_to_implied_prob():
    assert american_to_implied_prob(100) == pytest.approx(0.5)
    assert american_to_implied_prob(150) == pytest.approx(0.4)
    assert american_to_implied_prob(-200) == pytest.approx(2 / 3)
    # A book's two sides sum to >1 (the vig).
    assert american_to_implied_prob(-140) + american_to_implied_prob(118) > 1.0


def test_expected_value_sign_and_value():
    # Fair coin at even money is break-even.
    assert expected_value(0.5, 100) == pytest.approx(0.0)
    # EV at +100 is 2p-1.
    assert expected_value(0.6, 100) == pytest.approx(0.2)
    # Model underdog the market undervalues -> positive EV.
    assert expected_value(0.5, 180) > 0
    # Overpaying for a favorite the model dislikes -> negative EV.
    assert expected_value(0.5, -200) < 0


def test_evaluate_side_edge():
    ev = evaluate_side(0.5, 180)
    assert ev["implied_prob"] == pytest.approx(100 / 280)
    assert ev["edge"] == pytest.approx(0.5 - 100 / 280)
    assert ev["ev"] > 0


def test_normalize_name():
    assert normalize_name("Benoît Saint-Denis") == "benoit saint denis"
    assert normalize_name("Conor McGregor") == "conor mcgregor"
    assert normalize_name("  Max   Holloway ") == "max holloway"
    assert normalize_name("José Aldo Jr.") == "jose aldo jr"


# --- matching -----------------------------------------------------------------

def _fight(f1, f2, offers, cancelled=False):
    return {
        "fighter1_name": f1, "fighter2_name": f2,
        "best_odds1": None, "best_odds2": None,
        "is_cancelled": cancelled, "offers": offers,
    }


def _pred(a, b, prob_a, status="ok"):
    return {
        "fighter_a_name": a, "fighter_b_name": b,
        "prob_a": prob_a, "prob_b": 1 - prob_a,
        "weight_class": "Lightweight", "status": status,
    }


ODDS = [
    _fight("Conor McGregor", "Max Holloway",
           {"draftkings": {"odds1": 180, "odds2": -220}}),
    _fight("Benoit Saint-Denis", "Paddy Pimblett",
           {"draftkings": {"odds1": -140, "odds2": 118}}),
    _fight("Jane Doe", "Mary Roe", {}, cancelled=True),
]


def test_match_flags_positive_ev_side():
    preds = [_pred("Conor McGregor", "Max Holloway", 0.50)]
    rows = match_odds_to_predictions(preds, ODDS, "draftkings")
    row = rows[0]
    assert row["odds_status"] == "ok"
    # McGregor at +180 with a 50% model prob is +EV; Holloway is not.
    assert row["value_side"] == "a"
    assert row["value_fighter"] == "Conor McGregor"
    assert row["eval_a"]["edge"] > 0
    assert row["eval_b"]["edge"] < 0
    assert row["value_ev"] > 0


def test_match_orients_odds_when_corners_reversed():
    # Prediction lists fighters in the opposite order to the odds feed.
    preds = [_pred("Max Holloway", "Conor McGregor", 0.50)]
    rows = match_odds_to_predictions(preds, ODDS, "draftkings")
    row = rows[0]
    assert row["odds_status"] == "ok"
    # A = Holloway should get the -220 line, B = McGregor the +180 line.
    assert row["eval_a"]["odds"] == -220
    assert row["eval_b"]["odds"] == 180
    assert row["value_side"] == "b"
    assert row["value_fighter"] == "Conor McGregor"


def test_match_respects_min_edge():
    preds = [_pred("Conor McGregor", "Max Holloway", 0.50)]
    # McGregor's edge is ~0.143; a 0.20 threshold suppresses the flag.
    rows = match_odds_to_predictions(preds, ODDS, "draftkings", min_edge=0.20)
    assert rows[0]["value_side"] is None


def test_match_missing_book_is_no_price():
    preds = [_pred("Benoit Saint-Denis", "Paddy Pimblett", 0.6)]
    rows = match_odds_to_predictions(preds, ODDS, "fanduel")
    assert rows[0]["odds_status"] == "no_price"
    assert rows[0]["value_side"] is None


def test_match_cancelled_and_no_match_and_skipped():
    preds = [
        _pred("Jane Doe", "Mary Roe", 0.5),               # cancelled bout
        _pred("Nobody Known", "Someone Else", 0.5),        # not in feed
        _pred("A B", "C D", 0.5, status="skipped_debut"),  # no model prediction
    ]
    rows = match_odds_to_predictions(preds, ODDS, "draftkings")
    assert rows[0]["odds_status"] == "cancelled"
    assert rows[1]["odds_status"] == "no_match"
    assert rows[2]["status"] == "skipped_debut"
    assert all(r["value_side"] is None for r in rows)


def test_match_accent_insensitive():
    preds = [_pred("Benoît Saint-Denis", "Paddy Pimblett", 0.7)]
    rows = match_odds_to_predictions(preds, ODDS, "draftkings")
    assert rows[0]["odds_status"] == "ok"
    assert rows[0]["eval_a"]["odds"] == -140
