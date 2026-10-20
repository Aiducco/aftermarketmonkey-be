class SbException(Exception):
    pass


class SbDownloadError(SbException):
    pass


class SbParseError(SbException):
    pass


class SbSFTPConnectionError(SbException):
    pass


class SbFileNotFoundError(SbException):
    pass
