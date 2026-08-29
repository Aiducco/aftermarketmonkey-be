class TdgException(Exception):
    """Base exception for TDG Access integration errors."""


class TdgAuthError(TdgException):
    """401 -- the API key is wrong, or a sandbox key was sent to production (or the reverse)."""


class TdgPermissionError(TdgException):
    """403 -- the key is valid but this account is not entitled to the endpoint."""


class TdgRequestError(TdgException):
    """Transport failure, a non-2xx TDG did not explain, or a body that is not the JSON we expect."""
