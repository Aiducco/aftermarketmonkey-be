class MotorStateAPIException(Exception):
    pass


class MotorStateAPIBadResponseCodeError(MotorStateAPIException):
    def __init__(self, message: str, code: int) -> None:
        MotorStateAPIException.__init__(self)
        self.message = message
        self.code = code


class MotorStatePermissionError(MotorStateAPIBadResponseCodeError):
    """The API key is not entitled for the requested resource.

    Motor State returns 403 both for a bad/unknown key and for a valid key that
    lacks access to an endpoint (e.g. the undocumented ProductSearch/ProductQuery
    routes), so the two cannot be told apart from the response alone.
    """


class MotorStateAPIConnectionError(MotorStateAPIException):
    """
    The API host was unreachable -- DNS, timeout, refused connection. Not a credentials problem
    and not a business rejection.

    Unlike MotorStateAPIBadResponseCodeError above, this passes its message up to Exception, so
    ``str(e)`` carries it. (That sibling stores the text on ``.message`` and calls
    ``Exception.__init__`` with no arguments, which leaves ``str(e)`` empty -- callers must read
    ``.message``.)
    """


class MotorStateFTPException(Exception):
    """Base for the FTP feed client (separate from the ordering API's exception tree)."""


class MotorStateFTPConnectionError(MotorStateFTPException):
    pass


class MotorStateFeedNotFoundError(MotorStateFTPException):
    pass
