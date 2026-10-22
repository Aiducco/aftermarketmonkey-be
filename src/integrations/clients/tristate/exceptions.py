class TriStateException(Exception):
    pass


class TriStateAuthError(TriStateException):
    """Invalid or missing Api-Key/App-Id."""


class TriStatePermissionError(TriStateException):
    """Valid credentials, but this App-Id isn't allowed to call this endpoint (PACE's own
    per-endpoint ACL -- confirmed live: TridentMotorsports' key can call test/get_warehouses/
    get_inventory_and_pricing but not get_brands/get_purchase_orders)."""


class TriStateTransportError(TriStateException):
    """Network/timeout failure, or a response that doesn't match either the documented JSON
    success shape or the documented plain-text ERROR shape."""


# -- SFTP relay feed (Tri-State pushes files to our own relay box) -------------------------
# Separate from the PACE API errors above: a different transport with different failure modes
# (paramiko/protocol errors and a missing file, not HTTP status codes or PACE's plain-text
# ERROR body). Same split Meyer draws between its SFTP client and its order API client.


class TriStateSFTPConnectionError(TriStateException):
    """Could not reach or authenticate against our relay with this company's relay credentials."""


class TriStateFileNotFoundError(TriStateException):
    """Connected fine, but the expected feed file isn't on the relay -- Tri-State hasn't
    delivered it yet, or delivered it under a different name (see TRISTATE_*_REMOTE_FILE)."""


class TriStateDataValidationError(TriStateException):
    """A delivered file was readable but its contents don't match what the parser expects."""
