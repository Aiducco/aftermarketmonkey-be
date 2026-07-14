class PremierException(Exception):
    pass


class PremierFTPConnectionError(PremierException):
    pass


class PremierFTPAuthError(PremierException):
    """Server actively rejected the login (bad ftp_user/ftp_password)."""


class PremierFileNotFoundError(PremierException):
    pass


class PremierDataValidationError(PremierException):
    pass
