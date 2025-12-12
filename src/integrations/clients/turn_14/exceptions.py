class Turn14APIException(Exception):
    pass


class Turn14APIBadResponseCodeError(Turn14APIException):
    def __init__(self, message: str, code: int) -> None:
        Turn14APIException.__init__(self)
        self.message = message
        self.code = code