class PremierException(Exception):
    pass


class PremierFTPConnectionError(PremierException):
    pass


class PremierFTPAuthError(PremierException):
    """Server actively rejected the login (bad ftp_user/ftp_password)."""


class PremierFileNotFoundError(PremierException):
    pass


class PremierDataValidationError(PremierException):
    pass


# -- Order API (REST, apiKey -> Bearer JWT session token) --------------------------------
# Separate from the FTP feed errors above since the order API is a different transport with its
# own auth and failure modes. Premier's docs don't specify an error response shape, success
# status code, or session-token expiry — the client treats these conservatively (see
# order_client.py for exactly what's assumed vs. documented).

class PremierOrderAPIException(PremierException):
    """Transport-level or unexpected failure calling the Premier Order API."""


class PremierOrderAuthError(PremierException):
    """Authentication rejected — invalid apiKey, or the session token was rejected on a
    subsequent call."""


class PremierOrderValidationError(PremierException):
    """The distributor rejected the request for a business reason. Premier's docs don't
    document an error body shape, so ``message`` is best-effort (parsed JSON error fields if
    present, otherwise the raw response body)."""

    def __init__(self, message: str, code: str = None) -> None:
        PremierException.__init__(self, message)
        self.message = message
        self.code = code
