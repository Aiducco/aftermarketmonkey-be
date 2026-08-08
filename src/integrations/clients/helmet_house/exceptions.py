class HelmetHouseException(Exception):
    pass


class HelmetHouseFTPConnectionError(HelmetHouseException):
    pass


class HelmetHouseFTPAuthError(HelmetHouseException):
    """Server actively rejected the login (FTP 530) — distinct from an unreachable host."""


class HelmetHouseFileNotFoundError(HelmetHouseException):
    """The catalog file is not present in the account's FTP directory."""


class HelmetHouseDataValidationError(HelmetHouseException):
    pass
