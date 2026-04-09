class AtechException(Exception):
    pass


class AtechSFTPConnectionError(AtechException):
    pass


class AtechFileNotFoundError(AtechException):
    pass


class AtechDataValidationError(AtechException):
    pass
