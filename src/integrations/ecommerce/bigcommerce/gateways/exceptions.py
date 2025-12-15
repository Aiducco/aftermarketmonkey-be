import typing


class BigCommerceAPIException(Exception):
    pass


class BigCommerceAPIBadResponseCodeError(BigCommerceAPIException):
    def __init__(self, message: str, code: int) -> None:
        BigCommerceAPIException.__init__(self)
        self.message = message
        self.code = code


class BigCommerceAPIRateLimitError(BigCommerceAPIException):
    def __init__(self, message: str, retry_after_ms: typing.Optional[int] = None) -> None:
        BigCommerceAPIException.__init__(self)
        self.message = message
        self.retry_after_ms = retry_after_ms

