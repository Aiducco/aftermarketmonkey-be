class KeystoneException(Exception):
    pass


class KeystoneFTPConnectionError(KeystoneException):
    pass


class KeystoneFTPAuthError(KeystoneException):
    """Server actively rejected the login (bad ftp_user/ftp_password)."""


class KeystoneFileNotFoundError(KeystoneException):
    pass


class KeystoneDataValidationError(KeystoneException):
    pass
