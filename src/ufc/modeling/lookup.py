"""Fighter lookup by name — a UI convenience that never raises.

All internal plumbing keys on fighter_id; name search only helps a human find
the right id. Duplicate names are real in the UFC (two Bruno Silvas), so this
returns every candidate and lets the caller disambiguate.
"""

import pandas as pd


def find_fighters(fighter_features: pd.DataFrame, name: str) -> pd.DataFrame:
    """Case-insensitive name search returning all candidates (possibly empty).

    An exact (case-insensitive) match narrows the result only when unique;
    otherwise all substring matches are returned.
    """
    name = name.strip()
    if not name:
        return fighter_features.iloc[0:0]

    matches = fighter_features[
        fighter_features["name"].str.contains(name, case=False, na=False, regex=False)
    ]
    exact = matches[matches["name"].str.lower() == name.lower()]
    if len(exact) == 1:
        return exact
    return matches
