class RoughCountryException(Exception):
    pass


class RoughCountryDownloadError(RoughCountryException):
    pass


class RoughCountryParseError(RoughCountryException):
    pass
