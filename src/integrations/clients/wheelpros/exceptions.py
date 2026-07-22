class WheelProsException(Exception):
    """Base exception for WheelPros integration errors."""


class WheelProsSFTPConnectionError(WheelProsException):
    """Raised when the SFTP connection fails."""


class WheelProsAuthError(WheelProsException):
    """Server actively rejected the login (bad sftp_user/sftp_password)."""


class WheelProsPermissionError(WheelProsException):
    """Authenticated successfully, but the account lacks access to a required feed path."""


class WheelProsDownloadError(WheelProsException):
    """Raised when the WheelPros CSV cannot be downloaded."""


class WheelProsParseError(WheelProsException):
    """Raised when the WheelPros CSV cannot be parsed."""


class WheelProsFileNotFoundError(WheelProsException):
    """Raised when the remote WheelPros CSV is missing."""


# -- Order API (REST, Bearer JWT auth) ----------------------------------------------------
# Separate from the SFTP/feed errors above since the order API is a different transport with
# its own auth (username/password -> 1hr Bearer token) and failure modes (JSON error bodies,
# not FTP protocol errors).

class WheelProsOrderAPIException(WheelProsException):
    """Transport-level or unexpected failure calling the Wheel Pros Order API."""


class WheelProsOrderAuthError(WheelProsException):
    """401 from /auth/v1/authorize — invalid Product Data Portal username/password."""


class WheelProsOrderPermissionError(WheelProsException):
    """403 from an Order/Inventory API call — token is valid but this account lacks
    permission for the API being called (e.g. Inventory Search requires a separate grant
    from Orders)."""


class WheelProsOrderValidationError(WheelProsException):
    """The distributor rejected the request for a business reason (bad item, insufficient
    quantity, invalid address, etc.) — parsed from the response's error body, rather than a
    transport/auth failure."""

    def __init__(self, message: str, code: str = None) -> None:
        WheelProsException.__init__(self, message)
        self.message = message
        self.code = code
