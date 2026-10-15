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
