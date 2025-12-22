class SDCException(Exception):
    pass


class SDCSFTPConnectionError(SDCException):
    def __init__(self, message: str) -> None:
        SDCException.__init__(self)
        self.message = message


class SDCFileNotFoundError(SDCException):
    def __init__(self, message: str) -> None:
        SDCException.__init__(self)
        self.message = message


