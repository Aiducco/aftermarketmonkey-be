class OrderAdapterError(Exception):
    """Base class for all distributor order-adapter errors."""
    pass


class OrderAdapterNotImplementedError(OrderAdapterError):
    """Raised by an adapter whose distributor schema isn't confirmed/implemented yet."""
    pass


class OrderValidationError(OrderAdapterError):
    """The distributor rejected the request for a business reason (invalid part, blocked
    part, insufficient quantity, bad address, etc.) rather than a transport/auth failure."""

    def __init__(self, message: str, code: str = None) -> None:
        OrderAdapterError.__init__(self, message)
        self.message = message
        self.code = code


class OrderNotSupportedError(OrderAdapterError):
    """Raised when an operation (e.g. cancel) is called on an adapter that doesn't support it."""
    pass
