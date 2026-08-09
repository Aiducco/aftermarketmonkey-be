class EliteWheelException(Exception):
    pass


class EliteWheelDownloadError(EliteWheelException):
    pass


class EliteWheelParseError(EliteWheelException):
    pass


class EliteWheelFileNotFoundError(EliteWheelException):
    pass


class EliteWheelSFTPConnectionError(EliteWheelException):
    """
    ``.message`` is kept for the connect-time validator, which reports it verbatim to the dealer.
    The message is ALSO passed to the base Exception so ``str(e)`` is populated -- callers that log
    ``str(e)`` (the ingest pipeline's audited-step handler among them) would otherwise record a
    blank error and leave a failed nightly run with no explanation of what went wrong.
    """
    def __init__(self, message: str) -> None:
        EliteWheelException.__init__(self, message)
        self.message = message
