class PremierException(Exception):
    pass


class PremierFTPConnectionError(PremierException):
    pass


class PremierFileNotFoundError(PremierException):
    pass


class PremierDataValidationError(PremierException):
    pass
