class AddressProviderError(Exception):
    """
    Base class for every address-provider failure. Callers outside
    ``src.integrations.address`` only ever see this (or a subclass) -- never a vendor
    exception type such as GeoapifyAPIException -- so swapping providers can't leak into the
    API layer's error handling.
    """
    pass


class AddressProviderTimeout(AddressProviderError):
    """The provider did not answer inside settings.ADDRESS_PROVIDER_TIMEOUT_SECONDS."""
    pass


class AddressProviderNotConfigured(AddressProviderError):
    """
    No usable provider: ADDRESS_PROVIDER names something unknown, or the named provider's
    API key is missing. Raised at construction time, never mid-request.
    """
    pass
