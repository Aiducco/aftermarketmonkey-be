class LiveInventoryError(Exception):
    """Base class for all live-inventory-refresh errors."""
    pass


class LiveInventoryNotFoundError(LiveInventoryError):
    """The distributor has no inventory record for this item right now."""
    pass


class LiveInventoryTransportError(LiveInventoryError):
    """
    Wraps a distributor-specific API/transport failure (auth, timeout, rate limit, 5xx) so
    callers outside src.integrations.live_inventory never need to know per-distributor
    exception types (Turn14APIException, a future KeystoneAPIException, etc.).
    """
    pass
