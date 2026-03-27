class MeyerException(Exception):
    pass


class MeyerSFTPConnectionError(MeyerException):
    pass


class MeyerFileNotFoundError(MeyerException):
    pass


class MeyerDataValidationError(MeyerException):
    pass
