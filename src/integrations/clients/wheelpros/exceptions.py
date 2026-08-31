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


# -- Vehicle API (REST, Bearer JWT auth) --------------------------------------------------
# Shares /auth/v1/authorize with the Order API above, but is a separately-entitled API area:
# a Product Data Portal account can authenticate fine and still be refused by /vehicles.

class WheelProsVehicleAPIException(WheelProsException):
    """Transport-level or unexpected failure calling the Wheel Pros Vehicle API."""


class WheelProsVehiclePermissionError(WheelProsException):
    """403 from a /vehicles call. The token is valid — the account simply is not entitled to
    the Vehicle API. Wheel Pros gates it behind a Cognito group the Orders/Pricing/Product
    grants do not imply, so this is an account provisioning problem, not a code one: it is
    fixed by Wheel Pros adding the entitlement, never by retrying."""


class WheelProsVehicleNotFound(WheelProsException):
    """404 from a /vehicles call — that year/make/model/submodel has no record. Expected during
    a crawl (a listing can name a model whose detail endpoint 404s); callers skip, not abort."""


# -- Product API (REST, Bearer JWT auth) --------------------------------------------------

class WheelProsProductAPIException(WheelProsException):
    """Transport-level or unexpected failure calling the Wheel Pros Product API."""


class WheelProsProductPermissionError(WheelProsException):
    """403 from a /products call — the account is not entitled to the Product API."""
