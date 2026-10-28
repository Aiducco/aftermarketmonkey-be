class AllProException(Exception):
    pass


class AllProFTPConnectionError(AllProException):
    """Host unreachable (DNS failure, timeout, connection refused) -- not a credentials problem."""
    pass


class AllProAuthError(AllProException):
    """FTP server rejected the username/password (530 Login incorrect)."""
    pass


class AllProAPIError(AllProException):
    """
    Any failure from the Fulfillment Master API (see order_client.py). Note these arrive as
    HTTP 200 with ``<error msg="..."/>`` as the entire body, so this is raised off the payload
    rather than off a status code.
    """
    pass


class AllProAPIConnectionError(AllProAPIError):
    """FM host unreachable (DNS, timeout, connection refused) -- not a credentials problem."""
    pass


class AllProAPIAuthError(AllProAPIError):
    """FM rejected the username/password: "Invalid username/password"."""
    pass


class AllProUpstreamNotProvisionedError(AllProAPIError):
    """
    FM authenticated us, then failed opening a session to its own upstream inventory system:
    "Invalid OW username/password".

    Deliberately NOT an auth error despite what the message says. The credentials that failed are
    the ones AllPro stores on their side for this account, not the dealer's -- proven against
    their sandbox, where the identical error comes back for a request containing no <part> at
    all, for AllPro's own documented example part, and for every branch value, i.e. before our
    payload is examined. Reporting it as a bad password sends a dealer to re-enter a correct one;
    the fix is for AllPro to provision the account.
    """
    pass
