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


# -- Electronic Order Web Service (SOAP order-placement API) -----------------------------
# Separate from the FTP catalog errors above since the order API is a different transport with
# its own failure modes (SOAP faults vs. errors embedded in a 200 response body).

class KeystoneOrderAPIException(KeystoneException):
    """Transport-level or unexpected failure calling the Electronic Order Web Service."""


class KeystoneOrderAuthError(KeystoneException):
    """SOAP fault: invalid/unrecognized security key ("*** Illegal use of this web service !!! ***")."""


class KeystoneOrderPermissionError(KeystoneException):
    """SOAP fault: valid key/IP but not authorized for this function
    ("*** You are not authorized to use this function ***")."""


class KeystoneOrderValidationError(KeystoneException):
    """The distributor rejected the request for a business reason (bad part, insufficient
    quantity, blocked part, bad address, etc.) — parsed from an "Error: Code NNN..." string
    embedded in an otherwise-200 SOAP response, rather than a transport/auth failure."""

    def __init__(self, message: str, code: str = None) -> None:
        KeystoneException.__init__(self, message)
        self.message = message
        self.code = code
