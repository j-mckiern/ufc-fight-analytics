"""Live betting odds from fightodds.io's public GraphQL API.

fightodds.io is a single-page app backed by an unauthenticated GraphQL endpoint
(`https://api.fightodds.io/gql`). We use two operations:

- ``allEvents`` for the upcoming schedule (filtered to UFC), and
- ``eventOfferTable`` for one event's fights with per-sportsbook moneyline odds.

Odds are American (moneyline); a book with no price for a fighter returns null.
This module only fetches and reshapes the feed — the value math lives in
``ufc.modeling.value`` and fighter-name matching happens there too, since this
feed keys fighters by name (no ufcstats ids) and exposes no weight class.
"""

import requests

GQL_URL = "https://api.fightodds.io/gql"

# fightodds serves the SPA from fightodds.io; the API accepts requests without
# auth but expects browser-like Origin/Referer headers.
_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://fightodds.io",
    "Referer": "https://fightodds.io/",
}

_EVENTS_QUERY = """
query($first: Int, $dateGte: Date, $orderBy: String) {
  allEvents(first: $first, date_Gte: $dateGte, orderBy: $orderBy) {
    edges { node { pk name date slug promotion { shortName } } }
  }
}
"""

_EVENT_ODDS_QUERY = """
query($eventPk: Int!) {
  sportsbooks: allSportsbooks(hasOdds: true) {
    edges { node { shortName slug } }
  }
  eventOfferTable(pk: $eventPk) {
    name
    fightOffers {
      edges {
        node {
          fighter1 { firstName lastName }
          fighter2 { firstName lastName }
          bestOdds1
          bestOdds2
          isCancelled
          straightOffers {
            edges {
              node {
                sportsbook { slug shortName }
                outcome1 { odds }
                outcome2 { odds }
              }
            }
          }
        }
      }
    }
  }
}
"""


def _gql(session: requests.Session, query: str, variables: dict) -> dict:
    """POST a GraphQL query and return its ``data`` payload (raises on errors)."""
    resp = session.post(
        GQL_URL,
        json={"query": query, "variables": variables},
        headers=_HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise RuntimeError(f"fightodds GraphQL error: {payload['errors']}")
    return payload["data"]


def _full_name(fighter: dict | None) -> str:
    if not fighter:
        return ""
    return " ".join(
        part for part in (fighter.get("firstName"), fighter.get("lastName")) if part
    ).strip()


def fetch_upcoming_odds_events(
    session: requests.Session,
    today: str,
    first: int = 60,
    promotion: str | None = "UFC",
) -> list[dict]:
    """Upcoming events on/after ``today`` (YYYY-MM-DD), soonest first.

    Each event: ``{pk, name, date, slug}``. Filtered to ``promotion`` when given
    (default UFC), since the model is trained on UFC fights only.
    """
    data = _gql(
        session,
        _EVENTS_QUERY,
        {"first": first, "dateGte": today, "orderBy": "date"},
    )
    events = []
    for edge in data["allEvents"]["edges"]:
        node = edge["node"]
        if promotion and (node.get("promotion") or {}).get("shortName") != promotion:
            continue
        events.append({
            "pk": node["pk"],
            "name": node["name"],
            "date": node["date"],
            "slug": node["slug"],
        })
    return events


def fetch_event_odds(session: requests.Session, event_pk: int) -> dict:
    """One event's odds table.

    Returns ``{sportsbooks: [{slug, short_name}], fights: [...]}`` where each
    fight is::

        {
            "fighter1_name": str, "fighter2_name": str,
            "best_odds1": int | None, "best_odds2": int | None,
            "is_cancelled": bool,
            "offers": {book_slug: {"odds1": int | None, "odds2": int | None}},
        }

    ``offers`` maps a sportsbook slug to that book's American odds for each
    fighter (either side may be null when the book has no price).
    """
    data = _gql(session, _EVENT_ODDS_QUERY, {"eventPk": event_pk})

    sportsbooks = [
        {"slug": e["node"]["slug"], "short_name": e["node"]["shortName"]}
        for e in data["sportsbooks"]["edges"]
    ]

    table = data.get("eventOfferTable")
    fights: list[dict] = []
    if table:
        for edge in table["fightOffers"]["edges"]:
            node = edge["node"]
            offers: dict[str, dict] = {}
            for so in node["straightOffers"]["edges"]:
                son = so["node"]
                slug = son["sportsbook"]["slug"]
                offers[slug] = {
                    "odds1": (son["outcome1"] or {}).get("odds"),
                    "odds2": (son["outcome2"] or {}).get("odds"),
                }
            fights.append({
                "fighter1_name": _full_name(node["fighter1"]),
                "fighter2_name": _full_name(node["fighter2"]),
                "best_odds1": node.get("bestOdds1"),
                "best_odds2": node.get("bestOdds2"),
                "is_cancelled": bool(node.get("isCancelled")),
                "offers": offers,
            })

    return {"sportsbooks": sportsbooks, "fights": fights}
