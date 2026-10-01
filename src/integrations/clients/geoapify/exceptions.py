class GeoapifyAPIException(Exception):
    """Any non-success answer from Geoapify (HTTP error, unparseable body, transport error)."""
    pass


class GeoapifyTimeout(GeoapifyAPIException):
    """Geoapify did not answer inside the request timeout."""
    pass


class GeoapifyAuthError(GeoapifyAPIException):
    """Geoapify rejected the apiKey (401/403). Almost always a misconfigured
    GEOAPIFY_API_KEY rather than anything about the address being looked up."""
    pass
