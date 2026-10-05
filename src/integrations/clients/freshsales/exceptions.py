class FreshsalesAPIException(Exception):
    """Any non-success answer from FreshSales (HTTP error, unparseable body, transport error)."""

    pass


class FreshsalesTimeout(FreshsalesAPIException):
    """FreshSales did not answer inside the request timeout."""

    pass


class FreshsalesAuthError(FreshsalesAPIException):
    """FreshSales rejected the request (401/403).

    403 does not necessarily mean a bad key: the API key is tied to a *user* and inherits that
    user's permissions, so a valid key returns 403 for an operation its owner cannot perform.
    ``GET /selector/owners`` already answers 403 for the key in use, which is why a write failing
    this way should be read as "this user cannot create that record" before "the key is wrong".
    """

    pass


class FreshsalesRateLimited(FreshsalesAPIException):
    """FreshSales returned 429. The limit is 1000 requests/hour per *account*, shared with
    everything else touching that CRM, so this can fire because of someone else's integration."""

    pass


class FreshsalesConfigError(FreshsalesAPIException):
    """The CRM is not shaped the way the sync expects -- e.g. a contact status it maps onto no
    longer exists under that name. Raised instead of writing a stale numeric id."""

    pass
