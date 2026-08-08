class WpsAPIException(Exception):
    pass


class WpsAPIBadResponseCodeError(WpsAPIException):
    def __init__(self, message: str, code: int) -> None:
        WpsAPIException.__init__(self)
        self.message = message
        self.code = code


class WpsPermissionError(WpsAPIBadResponseCodeError):
    """Token is rejected, or valid but not entitled for the requested resource."""
