class KeystoneException(Exception):
    pass


class KeystoneFTPConnectionError(KeystoneException):
    pass


class KeystoneFileNotFoundError(KeystoneException):
    pass


class KeystoneDataValidationError(KeystoneException):
    pass
