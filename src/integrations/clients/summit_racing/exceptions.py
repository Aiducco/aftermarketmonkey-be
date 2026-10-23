"""Failure modes of summitracing.com that callers have to tell apart.

Imperva answers a blocked request with HTTP 200 and a body, never a 4xx, so every one of these
is detected by inspecting the body -- see ``client.classify_response``.
"""


class SummitRacingError(Exception):
    """Base for every summitracing.com failure."""


class SummitRacingChallenged(SummitRacingError):
    """Imperva served its "Pardon Our Interruption" interstitial.

    The session cookie is absent, expired, or was minted for a browser fingerprint Imperva no
    longer accepts. Rotating the exit IP does not fix this -- a fresh ``reese84`` must be minted
    in a real browser.
    """


class SummitRacingThrottled(SummitRacingError):
    """Imperva rate-limited this exit IP on this route.

    Observed as a ~930-byte NOINDEX stub in place of the page. Unlike a challenge this is
    per-IP and per-route: the JSON endpoints keep answering while the HTML route is stubbed,
    and a different exit IP is served normally. Recoverable by backing off and rotating.
    """


class SummitRacingPartNotFound(SummitRacingError):
    """Summit rendered a page but it carries no applications tab -- usually a SKU we guessed wrong."""
