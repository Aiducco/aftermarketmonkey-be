class EliteWheelException(Exception):
    pass


class EliteWheelDownloadError(EliteWheelException):
    pass


class EliteWheelParseError(EliteWheelException):
    pass


class EliteWheelFileNotFoundError(EliteWheelException):
    pass


class EliteWheelSFTPConnectionError(EliteWheelException):
    def __init__(self, message: str) -> None:
        EliteWheelException.__init__(self)
        self.message = message
