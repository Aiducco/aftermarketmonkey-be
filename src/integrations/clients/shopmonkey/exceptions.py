class ShopMonkeyException(Exception):
    pass


class ShopMonkeyAPIException(ShopMonkeyException):
    """Transport-level or unexpected failure calling the ShopMonkey API."""


class ShopMonkeyAuthError(ShopMonkeyException):
    """ShopMonkey rejected the api_key (401/403)."""


class ShopMonkeyValidationError(ShopMonkeyException):
    """ShopMonkey rejected the request for a business reason (any other non-2xx status)."""

    def __init__(self, message: str, code: str = None) -> None:
        ShopMonkeyException.__init__(self, message)
        self.message = message
        self.code = code
