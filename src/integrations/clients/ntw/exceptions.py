class NtwException(Exception):
    pass


class NtwSFTPConnectionError(NtwException):
    def __init__(self, message: str) -> None:
        NtwException.__init__(self)
        self.message = message


class NtwFileNotFoundError(NtwException):
    def __init__(self, message: str) -> None:
        NtwException.__init__(self)
        self.message = message


class NtwApiError(NtwException):
    """Transport/auth/gateway failure talking to TBC's IBM API Connect gateway."""

    def __init__(self, message: str, status: int = None, payload: dict = None) -> None:
        NtwException.__init__(self)
        self.message = message
        self.status = status
        self.payload = payload

    def __str__(self) -> str:
        return self.message


class NtwApiBusinessError(NtwApiError):
    """
    TBC answered, and said no for a business reason: a bad customerId, an unknown part, nothing
    in stock. Separate from NtwApiError because these arrive as **HTTP 200** with an ``errorCd``
    body and no ``product`` key, and because a caller should surface them to the user rather
    than retry them.
    """
    pass


class NtwApiNotConfiguredError(NtwApiError):
    """
    A required piece of API configuration is missing: the gateway credentials, the CA bundle
    that completes TBC's incomplete certificate chain, or the order-signing secret. Deliberately
    not a transport error -- nothing was sent, and nothing should be retried.
    """
    pass
