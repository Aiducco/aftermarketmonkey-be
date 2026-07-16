class AsapAPIException(Exception):
    pass


class AsapAPIBadResponseCodeError(AsapAPIException):
    def __init__(self, message: str, code: int) -> None:
        AsapAPIException.__init__(self)
        self.message = message
        self.code = code
