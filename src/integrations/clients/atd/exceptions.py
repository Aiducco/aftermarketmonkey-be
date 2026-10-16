class ATDAPIException(Exception):
    pass


class ATDAPIBadResponseCodeError(ATDAPIException):
    def __init__(self, message: str, code: int) -> None:
        ATDAPIException.__init__(self, message)
        self.message = message
        self.code = code


class ATDAuthError(ATDAPIBadResponseCodeError):
    """Bad username/password/clientId, or the credentials don't have access to this location --
    see atd-connect-api.md section 2's two documented error shapes, both normalized to this."""


class ATDConnectionError(ATDAPIException):
    """Network/timeout/unreachable-host failures, and any 5xx or malformed response."""
