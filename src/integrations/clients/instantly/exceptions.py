class InstantlyAPIException(Exception):
    """Any non-success answer from Instantly (HTTP error, unparseable body, transport error)."""

    pass


class InstantlyTimeout(InstantlyAPIException):
    """Instantly did not answer inside the request timeout."""

    pass


class InstantlyAuthError(InstantlyAPIException):
    """Instantly rejected the key (401/403). Either INSTANTLY_API_KEY is wrong, or the key exists
    but lacks the scope for this endpoint -- the two are indistinguishable in the response, so the
    message names the endpoint to make the second case diagnosable."""

    pass


class InstantlyRateLimited(InstantlyAPIException):
    """Instantly returned 429. ``GET /emails`` is metered at 20 requests/minute, far tighter than
    the rest of the API, so this is almost always that endpoint. The caller is expected to stop the
    pass rather than retry: nothing is lost, because the next run re-reads from the watermark."""

    pass
