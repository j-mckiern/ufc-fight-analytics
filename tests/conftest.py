"""Small synthetic raw dataset covering the tricky cases:

- duplicate fighter names (two Bruno Silvas)
- missing physical stats (Charlie Cat: reach/height; Delta Dog: stance/age)
- a debut fighter with a profile but no fights (Echo Eel)
- a fighter appearing in fight_stats with no profile row (f8, orphan)
- a draw and a DQ that cleaning must drop
"""

import numpy as np
import pandas as pd
import pytest


def make_fighters_df():
    cols = [
        "fighter_id", "name", "nickname", "wins", "losses", "ties",
        "height_in", "weight_lb", "reach_in", "stance", "age",
        "slpm", "str_acc_dec", "sapm", "str_def_dec",
        "td_avg", "td_acc_dec", "td_def_dec", "sub_avg",
    ]
    rows = [
        ["f1", "Alpha Ape", "The First", 10, 2, 0, 70, 155, 72, "Orthodox", 28,
         4.5, 0.48, 3.2, 0.55, 1.5, 0.40, 0.60, 0.5],
        ["f2", "Bravo Bear", "", 8, 4, 0, 72, 155, 74, "Southpaw", 31,
         3.8, 0.42, 4.0, 0.50, 2.0, 0.35, 0.55, 1.0],
        ["f3", "Bruno Silva", "", 12, 5, 0, 68, 155, 70, "Orthodox", 33,
         5.1, 0.50, 4.8, 0.48, 0.5, 0.30, 0.65, 0.2],
        ["f4", "Bruno Silva", "Blindado", 9, 3, 0, 73, 185, 75, "Orthodox", 34,
         4.0, 0.45, 3.9, 0.52, 1.0, 0.38, 0.58, 0.4],
        ["f5", "Charlie Cat", "", 6, 6, 0, np.nan, 155, np.nan, "Southpaw", 27,
         3.0, 0.40, 3.5, 0.53, 1.2, 0.33, 0.50, 0.8],
        ["f6", "Delta Dog", "", 7, 1, 0, 74, 185, 78, np.nan, np.nan,
         4.2, 0.46, 3.0, 0.57, 2.5, 0.42, 0.62, 1.2],
        ["f7", "Echo Eel", "", 5, 0, 0, 71, 155, 73, "Switch", 24,
         np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
    ]
    return pd.DataFrame(rows, columns=cols)


def make_fights_df():
    rows = [
        ["ft1", "2020-01-01", "Lightweight", "U-DEC", 3],
        ["ft2", "2020-01-01", "Lightweight", "KO/TKO", 1],
        ["ft3", "2021-06-01", "Lightweight", "SUB", 2],
        ["ft4", "2021-06-01", "Middleweight", "U-DEC", 3],
        ["ft5", "2023-03-01", "Lightweight", "KO/TKO", 3],
        ["ft6", "2023-03-01", "Middleweight", "DQ", 1],
        ["ft7", "2023-03-01", "Catch Weight", "U-DEC", 3],
        ["ft8", "2025-01-01", "Lightweight", "U-DEC", 3],
        ["ft9", "2025-01-01", "Middleweight", "SUB", 1],
    ]
    df = pd.DataFrame(
        rows, columns=["fight_id", "event_date", "weight_class", "method", "round"]
    )
    df["event_date"] = pd.to_datetime(df["event_date"])
    return df


def make_fight_stats_df():
    cols = [
        "fight_id", "fighter_id", "result", "sig_strikes", "sig_attempted",
        "td", "td_attempted", "sub_attempts", "control_time",
    ]
    rows = [
        ["ft1", "f1", "W", 50, 100, 2, 4, 1, 120],
        ["ft1", "f2", "L", 30, 90, 0, 2, 0, 60],
        ["ft2", "f3", "W", 20, 35, 0, 0, 0, 30],
        ["ft2", "f5", "L", 10, 40, 1, 3, 0, 45],
        ["ft3", "f1", "W", 40, 80, 3, 5, 2, 200],
        ["ft3", "f3", "L", 25, 60, 0, 1, 0, 20],
        ["ft4", "f4", "W", 60, 110, 1, 2, 0, 150],
        ["ft4", "f6", "L", 45, 100, 0, 0, 1, 90],
        ["ft5", "f2", "W", 55, 95, 2, 3, 0, 180],
        ["ft5", "f5", "L", 35, 85, 0, 4, 0, 30],
        # ft6 is a DQ (dropped by method); f8 has no profile row
        ["ft6", "f4", "W", 15, 25, 0, 0, 0, 10],
        ["ft6", "f8", "L", 12, 30, 0, 0, 0, 5],
        # ft7 is a draw: no W/L pair, dropped by cleaning
        ["ft7", "f1", "D", 33, 70, 1, 2, 0, 100],
        ["ft7", "f4", "D", 31, 68, 1, 1, 0, 95],
        ["ft8", "f1", "W", 48, 92, 2, 2, 1, 140],
        ["ft8", "f8", "L", 22, 55, 0, 1, 0, 25],
        ["ft9", "f6", "W", 10, 15, 3, 3, 2, 240],
        ["ft9", "f4", "L", 8, 20, 0, 0, 0, 15],
    ]
    return pd.DataFrame(rows, columns=cols)


def write_raw_dir(raw):
    raw.mkdir(parents=True, exist_ok=True)
    make_fighters_df().to_csv(raw / "fighters.csv", index=False)
    make_fights_df().to_csv(raw / "fights.csv", index=False)
    make_fight_stats_df().to_csv(raw / "fight_stats.csv", index=False)
    return raw


@pytest.fixture
def fighters_df():
    return make_fighters_df()


@pytest.fixture
def fights_df():
    return make_fights_df()


@pytest.fixture
def fight_stats_df():
    return make_fight_stats_df()


@pytest.fixture
def raw_dir(tmp_path):
    return write_raw_dir(tmp_path / "raw")
