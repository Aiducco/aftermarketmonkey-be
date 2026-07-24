class OrderAdapterError(Exception):
    """Base class for all distributor order-adapter errors."""
    pass


class OrderAdapterNotImplementedError(OrderAdapterError):
    """Raised by an adapter whose distributor schema isn't confirmed/implemented yet."""
    pass


class OrderValidationError(OrderAdapterError):
    """The distributor rejected the request for a business reason (invalid part, blocked
    part, insufficient quantity, bad address, etc.) rather than a transport/auth failure."""

    def __init__(self, message: str, code: str = None, request_payload: dict = None) -> None:
        OrderAdapterError.__init__(self, message)
        self.message = message
        self.code = code
        # The logical request body/params being sent when the failure happened, if the caller
        # had one built yet — same role as base.ShippingQuoteResult/DistributorOrderResult's
        # own request_payload field, but for the failure path, where there's no result object
        # to carry it. purchase_order_jobs.py reads this via getattr() to log a failed
        # QUOTE/SUBMIT attempt's request payload, not just its error message.
        self.request_payload = request_payload


class OrderNotSupportedError(OrderAdapterError):
    """Raised when an operation (e.g. cancel) is called on an adapter that doesn't support it."""
    pass
