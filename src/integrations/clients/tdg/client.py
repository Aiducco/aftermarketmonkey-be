"""
Transport client for the TDG Access API (Tire Discounter Group).

Docs: "TDG Access API Documentation", v1.0.44. Auth is a static API key in an ``Authorization:
ApiKey <key>`` header -- no token exchange, no expiry, nothing to refresh, which is why this
client is a good deal thinner than the Wheel Pros ones.

Two environments, two key prefixes
----------------------------------
Production keys start ``rst``, sandbox keys ``rstsb``, and each only works against its own host.
A sandbox key sent to production comes back 401, so :func:`_check_key_prefix` warns on an obvious
mismatch rather than letting it surface as "bad credentials" three layers up.

Endpoints this covers
---------------------
Only the two catalog reads, because those are the whole of what the catalog pull needs::

    POST /api/product/all      every product, all types, no parameters
    POST /api/product/search   the same shape, filtered by part/item number, brand, size...

Both are POSTs despite being reads, and both return a bare JSON array. ``/product/all`` has no
pagination: it answers the entire catalogue in one response -- ~32 MB and ~45k products at the
time of writing -- so it is streamed to a file rather than held twice in memory as bytes and
then as objects.

Order, quote, inventory and account endpoints are deliberately absent. Inventory in particular
would need a ``shippingAddress`` and returns per-branch stock and pricing; it is a different
job with different freshness requirements, and mixing it into a catalog pull would put a
perishable number in a table meant to hold durable facts.
"""
import json
import logging
import tempfile
import typing

import requests
from django.conf import settings

from src.integrations.clients.tdg import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TDG-CLIENT]"

REQUEST_TIMEOUT_SECONDS = 300
STREAM_CHUNK_BYTES = 1 << 20

_ENVIRONMENT_BASE_URLS = {
    "production": "https://www.tdgaccess.ca",
    "sandbox": "https://sandbox.tdgaccess.ca",
}
_ENVIRONMENT_KEY_PREFIXES = {
    # Longest first: every sandbox key also starts with "rst".
    "sandbox": "rstsb",
    "production": "rst",
}


class TdgApiClient(object):
    """One instance per (api_key, environment). Not shared across threads -- it does not need to be."""

    def __init__(self, api_key: str = "", environment: str = "") -> None:
        self.api_key = api_key or getattr(settings, "TDG_API_KEY", "")
        if not self.api_key:
            raise ValueError("Invalid credentials parameter: a TDG API key is required (set TDG_API_KEY).")

        environment = environment or getattr(settings, "TDG_ENVIRONMENT", "production")
        if environment not in _ENVIRONMENT_BASE_URLS:
            raise ValueError(
                "Invalid environment: {}. Must be one of {}.".format(
                    environment, ", ".join(sorted(_ENVIRONMENT_BASE_URLS))
                )
            )
        self.environment = environment
        self.api_base_url = str(
            getattr(settings, "TDG_BASE_URL", "") or _ENVIRONMENT_BASE_URLS[environment]
        ).rstrip("/")
        _check_key_prefix(self.api_key, environment)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"ApiKey {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    # -- catalog reads ---------------------------------------------------------------------------

    def fetch_all_products(self, *, shipping_address: typing.Optional[int] = None) -> typing.List[dict]:
        """
        Every product TDG lists, of every type.

        Omit ``shipping_address`` unless you actually want inventory: with it, each product grows
        a ``locations`` array of per-branch stock, which is perishable data this caller does not
        store and roughly doubles the response.
        """
        payload: dict = {}
        if shipping_address is not None:
            payload["shippingAddress"] = shipping_address
        return self._post_list("/api/product/all", payload)

    def search_products(self, **criteria) -> typing.List[dict]:
        """
        ``/api/product/search`` -- same object shape as :meth:`fetch_all_products`, filtered.

        Criteria are passed through as given (``itemnumbers``, ``partnumbers``, ``brands``,
        ``tireSizes``, ``serviceType``, ``tireSeason``, ``gtin``); an empty list is not the same
        as an omitted key to TDG, so falsy values are dropped rather than sent.
        """
        payload = {key: value for key, value in criteria.items() if value not in (None, [], "")}
        return self._post_list("/api/product/search", payload)

    def stream_all_products_to_file(self, *, shipping_address: typing.Optional[int] = None) -> str:
        """
        Write ``/api/product/all`` to a temporary file and return its path.

        The full catalogue is tens of megabytes of JSON. Streaming it to disk keeps one copy of
        the bytes instead of holding the response body and the parsed list at once, and leaves a
        file the caller can re-parse after a mapping bug without a second 30-second download.
        The caller owns the file, and is expected to delete it.
        """
        payload: dict = {}
        if shipping_address is not None:
            payload["shippingAddress"] = shipping_address

        url = f"{self.api_base_url}/api/product/all"
        logger.info("%s streaming POST %s", _LOG_PREFIX, url)
        try:
            response = self.session.post(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS, stream=True)
        except requests.RequestException as exc:
            raise exceptions.TdgRequestError(f"TDG request to {url} failed: {exc}") from exc

        with response:
            _raise_for_status(response, url)
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".json", prefix="tdg_products_", delete=False
            ) as handle:
                written = 0
                for chunk in response.iter_content(chunk_size=STREAM_CHUNK_BYTES):
                    if chunk:
                        handle.write(chunk)
                        written += len(chunk)
                path = handle.name

        logger.info("%s wrote %.1f MB to %s", _LOG_PREFIX, written / (1 << 20), path)
        return path

    # -- internals -------------------------------------------------------------------------------

    def _post_list(self, path: str, payload: dict) -> typing.List[dict]:
        url = f"{self.api_base_url}{path}"
        logger.info("%s POST %s %s", _LOG_PREFIX, url, payload or "{}")
        try:
            response = self.session.post(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            raise exceptions.TdgRequestError(f"TDG request to {url} failed: {exc}") from exc

        _raise_for_status(response, url)
        try:
            body = response.json()
        except ValueError as exc:
            raise exceptions.TdgRequestError(
                f"TDG returned a non-JSON body from {url} (first 200 bytes: {response.text[:200]!r})"
            ) from exc
        return _as_product_list(body, url)


def _raise_for_status(response: requests.Response, url: str) -> None:
    if response.status_code == 401:
        raise exceptions.TdgAuthError(
            f"TDG rejected the API key at {url} (401). Check TDG_API_KEY, and that its prefix "
            "matches TDG_ENVIRONMENT -- 'rst' is production, 'rstsb' sandbox."
        )
    if response.status_code == 403:
        raise exceptions.TdgPermissionError(f"TDG denied access to {url} (403): this account is not entitled to it.")
    if not response.ok:
        raise exceptions.TdgRequestError(
            f"TDG returned HTTP {response.status_code} from {url}: {response.text[:200]!r}"
        )


def _as_product_list(body, url: str) -> typing.List[dict]:
    """
    Both catalog endpoints document a bare array; accept the obvious envelope shapes anyway.

    A single product object would deserialize as a dict and silently iterate as its keys, so the
    shape is checked rather than assumed -- that failure would otherwise reach the mapper as a
    string where a product was expected.
    """
    if isinstance(body, dict):
        for key in ("products", "data", "results", "items"):
            if isinstance(body.get(key), list):
                body = body[key]
                break
        else:
            raise exceptions.TdgRequestError(f"TDG returned an object, not a product array, from {url}: {sorted(body)}")
    if not isinstance(body, list):
        raise exceptions.TdgRequestError(f"TDG returned {type(body).__name__}, not a product array, from {url}")
    return [item for item in body if isinstance(item, dict)]


def load_products_from_file(path: str) -> typing.List[dict]:
    """Parse a file written by :meth:`TdgApiClient.stream_all_products_to_file`."""
    with open(path, "r", encoding="utf-8") as handle:
        try:
            body = json.load(handle)
        except ValueError as exc:
            raise exceptions.TdgRequestError(f"{path} does not contain valid JSON: {exc}") from exc
    return _as_product_list(body, path)


def _check_key_prefix(api_key: str, environment: str) -> None:
    expected = _ENVIRONMENT_KEY_PREFIXES[environment]
    other = next(
        (env for env, prefix in _ENVIRONMENT_KEY_PREFIXES.items() if env != environment and api_key.startswith(prefix)),
        None,
    )
    # "rstsb..." also starts with "rst", so a production key check only fails when the key looks
    # like the *other* environment's -- not merely when it lacks the prefix.
    if other is not None and not (environment == "sandbox" and api_key.startswith(expected)):
        logger.warning(
            "%s API key looks like a %s key but TDG_ENVIRONMENT is %s -- expect 401.",
            _LOG_PREFIX,
            other,
            environment,
        )
