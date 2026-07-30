class TireRackException(Exception):
    pass


class TireRackSFTPConnectionError(TireRackException):
    def __init__(self, message: str) -> None:
        TireRackException.__init__(self)
        self.message = message


class TireRackFileNotFoundError(TireRackException):
    def __init__(self, message: str) -> None:
        TireRackException.__init__(self)
        self.message = message
