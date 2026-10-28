class RandysException(Exception):
    """Base for every RANDYS Worldwide failure."""


class RandysAPIError(RandysException):
    """Convermax / Shopify returned something unusable."""


class RandysSFTPConnectionError(RandysException):
    """Could not reach, or authenticate against, the relay holding the inventory push."""


class RandysFileNotFoundError(RandysException):
    """No ``Inventory_RWW_*.csv`` has landed in this connection's relay folder yet."""


class RandysDataValidationError(RandysException):
    """A feed arrived but its shape is not what the parser can safely read."""
