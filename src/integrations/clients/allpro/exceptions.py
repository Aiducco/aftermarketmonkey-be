class AllProException(Exception):
    pass


class AllProFTPConnectionError(AllProException):
    """Host unreachable (DNS failure, timeout, connection refused) -- not a credentials problem."""
    pass


class AllProAuthError(AllProException):
    """FTP server rejected the username/password (530 Login incorrect)."""
    pass
