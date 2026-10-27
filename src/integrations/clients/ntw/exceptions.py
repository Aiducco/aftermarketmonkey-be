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
