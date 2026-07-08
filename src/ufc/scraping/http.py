"""Shared HTTP client for ufcstats.com.

Handles the site's anti-bot gate (a SHA-256 proof-of-work challenge served
before the first real response; solving it sets a session cookie) plus 429
exponential backoff and timeout retries.
"""

import hashlib
import logging
import re
import time

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://ufcstats.com"
MAX_RETRIES = 5
USER_AGENT = "Mozilla/5.0 (compatible; UFC-Stats-Scraper/1.0)"

_CHALLENGE_RE = re.compile(r'nonce="([0-9a-f]+)",\s*target=new Array\((\d+)\+1\)')

logger = logging.getLogger(__name__)


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def _solve_challenge(session: requests.Session, nonce: str, zeros: int) -> None:
    prefix = "0" * zeros
    n = 0
    while not hashlib.sha256(f"{nonce}:{n}".encode()).hexdigest().startswith(prefix):
        n += 1
    session.post(f"{BASE_URL}/__c", data={"nonce": nonce, "n": n}, timeout=30)
    logger.debug("Solved proof-of-work challenge (n=%d)", n)


def get_html(url: str, session: requests.Session) -> str:
    """Fetch a URL, transparently solving the PoW challenge and backing off on 429."""
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=30)
        except requests.Timeout as exc:
            last_exc = exc
            time.sleep(2**attempt)
            continue
        if resp.status_code == 429:
            time.sleep(2**attempt)
            continue
        resp.raise_for_status()

        challenge = _CHALLENGE_RE.search(resp.text)
        if challenge:
            _solve_challenge(session, challenge.group(1), int(challenge.group(2)))
            continue
        return resp.text

    if last_exc is not None:
        raise last_exc
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def get_soup(url: str, session: requests.Session) -> BeautifulSoup:
    return BeautifulSoup(get_html(url, session), "html.parser")
