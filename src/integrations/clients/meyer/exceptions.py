class MeyerException(Exception):
    pass


class MeyerSFTPConnectionError(MeyerException):
    pass


class MeyerFileNotFoundError(MeyerException):
    pass


class MeyerDataValidationError(MeyerException):
    pass


# -- Order API (REST, Espresso apikey auth) -----------------------------------------------
# Separate from the SFTP/feed errors above since the order API is a different transport with
# its own auth (username/password -> 30-day apikey) and failure modes (JSON errorCode/errorMessage
# bodies, not FTP protocol errors).

class MeyerOrderAPIException(MeyerException):
    """Transport-level or unexpected failure calling the Meyer Order API."""


class MeyerOrderAuthError(MeyerException):
    """401 from the Order API — invalid/expired apikey, or bad username/password at
    Authentication time."""


class MeyerOrderValidationError(MeyerException):
    """The distributor rejected the request for a business reason (invalid item, bad ship
    method, insufficient quantity, customer on hold, etc.) — parsed from the response's
    {"errorCode": ..., "errorMessage": ...} body, rather than a transport/auth failure."""

    def __init__(self, message: str, code: str = None) -> None:
        MeyerException.__init__(self, message)
        self.message = message
        self.code = code
