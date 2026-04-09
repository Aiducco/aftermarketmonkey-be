class DlgException(Exception):
    pass


class DlgSFTPConnectionError(DlgException):
    pass


class DlgFileNotFoundError(DlgException):
    pass


class DlgDataValidationError(DlgException):
    pass
