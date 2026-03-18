class WheelProsException(Exception):
    """Base exception for WheelPros integration errors."""


class WheelProsSFTPConnectionError(WheelProsException):
    """Raised when the SFTP connection fails."""


class WheelProsDownloadError(WheelProsException):
    """Raised when the WheelPros CSV cannot be downloaded."""


class WheelProsParseError(WheelProsException):
    """Raised when the WheelPros CSV cannot be parsed."""


class WheelProsFileNotFoundError(WheelProsException):
    """Raised when the remote WheelPros CSV is missing."""
