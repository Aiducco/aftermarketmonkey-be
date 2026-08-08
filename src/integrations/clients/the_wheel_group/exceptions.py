class TheWheelGroupException(Exception):
    pass


class TheWheelGroupDownloadError(TheWheelGroupException):
    pass


class TheWheelGroupParseError(TheWheelGroupException):
    pass


class TheWheelGroupFileNotFoundError(TheWheelGroupException):
    pass


class TheWheelGroupSFTPConnectionError(TheWheelGroupException):
    def __init__(self, message: str) -> None:
        TheWheelGroupException.__init__(self)
        self.message = message
