"""
Transport client for Wheel Pros' Vehicle API — the year/make/model/submodel tree and the
wheel-and-tire fitment envelope published for each vehicle.
Spec: https://developer.wheelpros.com/assets/specs/vehicle-api/openapi/api.html

Separate from ``order_client.py`` for the same reason that one is separate from ``client.py``:
a different capability (vehicle reference data) behind a different entitlement, read-only, and
driven by a long concurrent crawl rather than one-off order calls. It shares only the auth
endpoint.

Auth: the same Product Data Portal username/password exchanged at POST /auth/v1/authorize for a
1-hour Bearer JWT. Read-only throughout — there is no write path in this API, so unlike the
order client nothing here can have a side effect at Wheel Pros.

ENTITLEMENT: /vehicles is gated behind its own Cognito group. An account can authenticate
successfully, hold ``wp-api-core-orders``/``-pricing``/``-product``, and still get 403 from every
/vehicles route with "no identity-based policy allows the execute-api:Invoke action" — that is
AWS API Gateway refusing the *account*, not a bad token or a wrong path, and no amount of
retrying changes it. It surfaces as :class:`WheelProsVehiclePermissionError` so a crawl fails
loudly on the first call with an actionable message instead of grinding through 500k retries.

Thread-safety: one instance is meant to be shared by every worker thread of a crawl. The token
is refreshed under a lock so a mid-crawl expiry costs one refresh rather than one per thread,
and ``requests.Session`` instances are thread-local (a Session is not safe to drive from several
threads at once).
"""
import logging
import threading
import time
import typing

import requests
from django.conf import settings

from src.integrations.clients.wheelpros import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELPROS-VEHICLE-CLIENT]"

TOKEN_EXPIRATION_BUFFER_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 4
BACKOFF_BASE_SECONDS = 1.0

_ENVIRONMENT_BASE_URLS = {
    "production": "https://api.wheelpros.com",
    "staging": "https://stage.api.wheelpros.com",
}

# The API's own words for the two catalogs it can filter a listing by. Passing neither returns
# the union, which is what a full crawl wants.
VEHICLE_TYPES = ("wheel", "tire")


class WheelProsVehicleApiClient(object):
    """One instance per (credentials, environment) pair; safe to share across threads."""

    def __init__(self, credentials: typing.Dict, environment: str = "production") -> None:
        self.username = credentials.get("username", "")
        self.password = credentials.get("password", "")
        if not self.username or not self.password:
            raise ValueError("Invalid credentials parameter: username and password are required.")

        if environment not in _ENVIRONMENT_BASE_URLS:
            raise ValueError(
                "Invalid environment: {}. Must be one of {}.".format(
                    environment, ", ".join(sorted(_ENVIRONMENT_BASE_URLS))
                )
            )
        self.environment = environment
        self.api_base_url = str(
            getattr(settings, "WHEELPROS_VEHICLE_BASE_URL", "") or _ENVIRONMENT_BASE_URLS[environment]
        ).rstrip("/")

        self._cached_token: typing.Optional[str] = None
        self._token_expires_at: typing.Optional[float] = None
        self._token_lock = threading.Lock()
        self._thread_state = threading.local()

        self.requests_made = 0
        self._counter_lock = threading.Lock()

    # -- Auth ---------------------------------------------------------------------------------

    def _is_token_valid(self) -> bool:
        if self._cached_token is None or self._token_expires_at is None:
            return False
        return time.time() < (self._token_expires_at - TOKEN_EXPIRATION_BUFFER_SECONDS)

    def _get_valid_token(self) -> str:
        # Double-checked: the common case (a warm token) never takes the lock, while a refresh
        # that several workers notice at once only happens once.
        if self._is_token_valid():
            return self._cached_token

        with self._token_lock:
            if self._is_token_valid():
                return self._cached_token

            try:
                response = requests.post(
                    "{}/auth/v1/authorize".format(self.api_base_url),
                    headers={"Content-Type": "application/json"},
                    json={"userName": self.username, "password": self.password},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except requests.RequestException as e:
                raise exceptions.WheelProsVehicleAPIException(
                    "Authentication request failed. Error: {}".format(str(e))
                )

            if response.status_code == 401:
                raise exceptions.WheelProsOrderAuthError("Invalid Wheel Pros username/password.")
            if response.status_code != 200:
                raise exceptions.WheelProsVehicleAPIException(
                    "Invalid authorize response (status_code={}, body={})".format(
                        response.status_code, response.text[:500]
                    )
                )

            data = response.json()
            access_token = data.get("accessToken")
            if not access_token:
                raise exceptions.WheelProsVehicleAPIException("No accessToken in authorize response.")

            self._cached_token = access_token
            self._token_expires_at = time.time() + int(data.get("expiresIn") or 3600)
            logger.info("{} Obtained a Vehicle API token for {}.".format(_LOG_PREFIX, self.username))
            return self._cached_token

    def _clear_token_cache(self) -> None:
        with self._token_lock:
            self._cached_token = None
            self._token_expires_at = None

    # -- Transport ----------------------------------------------------------------------------

    def _session(self) -> requests.Session:
        session = getattr(self._thread_state, "session", None)
        if session is None:
            session = requests.Session()
            self._thread_state.session = session
        return session

    def _discard_session(self) -> None:
        """Drop this thread's Session after a transport error, so a half-open connection is
        never retried on — it would hang rather than fail."""
        session = getattr(self._thread_state, "session", None)
        self._thread_state.session = None
        if session is not None:
            try:
                session.close()
            except Exception:
                logger.debug("{} ignoring error closing a broken session".format(_LOG_PREFIX), exc_info=True)

    def _count_request(self) -> None:
        with self._counter_lock:
            self.requests_made += 1

    def _get(self, path: str, params: typing.Optional[typing.Dict] = None) -> typing.Any:
        """
        GET ``{base}/vehicles{path}``, retrying transport errors, 5xx and 429.

        A 403 is raised immediately as :class:`WheelProsVehiclePermissionError` — it means the
        account is not entitled to this API, which retrying cannot fix. A 404 becomes
        :class:`WheelProsVehicleNotFound` so a crawl can skip that vehicle. Any other 4xx is a
        bug in how the URL was built and is raised straight away rather than retried.
        """
        url = "{}/vehicles{}".format(self.api_base_url, path)
        last_error: typing.Optional[Exception] = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            token = self._get_valid_token()
            try:
                self._count_request()
                response = self._session().get(
                    url,
                    headers={"Authorization": "Bearer {}".format(token), "Accept": "application/json"},
                    params=params or {},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except requests.RequestException as e:
                last_error = e
                self._discard_session()
                logger.warning(
                    "{} transport error on {} (attempt {}/{}): {}".format(
                        _LOG_PREFIX, url, attempt, MAX_ATTEMPTS, str(e)
                    )
                )
            else:
                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError as e:
                        raise exceptions.WheelProsVehicleAPIException(
                            "Non-JSON 200 from {}: {}".format(url, response.text[:300])
                        ) from e

                if response.status_code == 403:
                    raise exceptions.WheelProsVehiclePermissionError(
                        "403 from {}. The token authenticated, but this Wheel Pros account is not "
                        "entitled to the Vehicle API. Ask Wheel Pros to grant the account "
                        "({}) access to the Vehicle API; the Orders/Pricing/Product grants do "
                        "not include it. Response: {}".format(url, self.username, response.text[:300])
                    )
                if response.status_code == 404:
                    raise exceptions.WheelProsVehicleNotFound(url)
                if response.status_code == 401:
                    # Token rejected mid-crawl (revoked, or clock skew beat the expiry buffer).
                    # Worth exactly one forced refresh before treating it as fatal.
                    last_error = exceptions.WheelProsVehicleAPIException("401 from {}".format(url))
                    self._clear_token_cache()
                    logger.warning("{} 401 on {}; refreshing token".format(_LOG_PREFIX, url))
                elif response.status_code == 429 or response.status_code >= 500:
                    last_error = exceptions.WheelProsVehicleAPIException(
                        "{} from {}".format(response.status_code, url)
                    )
                    logger.warning(
                        "{} HTTP {} on {} (attempt {}/{})".format(
                            _LOG_PREFIX, response.status_code, url, attempt, MAX_ATTEMPTS
                        )
                    )
                else:
                    raise exceptions.WheelProsVehicleAPIException(
                        "{} from {}: {}".format(response.status_code, url, response.text[:300])
                    )

            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))

        raise exceptions.WheelProsVehicleAPIException(
            "Giving up on {} after {} attempts: {}".format(url, MAX_ATTEMPTS, last_error)
        )

    @staticmethod
    def _quote(value: typing.Any) -> str:
        """Percent-encode one path segment. ``safe=""`` matters: makes and models contain
        slashes and spaces ("Chevrolet Silverado 1500", "RAM ProMaster 1500"), and a raw slash
        would silently address a different route."""
        return requests.utils.quote(str(value), safe="")

    # -- Listing endpoints ----------------------------------------------------------------------
    # Each returns a bare JSON array of scalars. ``vehicle_type``, when given, restricts the
    # listing to Wheel Pros' "wheel" or "tire" catalog; omitting it returns the union.

    def get_years(self, vehicle_type: typing.Optional[str] = None) -> typing.List[int]:
        """GET /v1/years -> [2026, 2025, ...]"""
        data = self._get("/v1/years", self._type_params(vehicle_type))
        return [int(year) for year in (data or []) if str(year).strip().isdigit()]

    def get_makes(self, year: int, vehicle_type: typing.Optional[str] = None) -> typing.List[str]:
        """GET /v1/years/{year}/makes -> ["Acura", "Alfa Romeo", ...]"""
        data = self._get("/v1/years/{}/makes".format(int(year)), self._type_params(vehicle_type))
        return [str(make) for make in (data or []) if str(make).strip()]

    def get_models(self, year: int, make: str, vehicle_type: typing.Optional[str] = None) -> typing.List[str]:
        """GET /v1/years/{year}/makes/{make}/models -> ["F-150", "Bronco", ...]"""
        data = self._get(
            "/v1/years/{}/makes/{}/models".format(int(year), self._quote(make)),
            self._type_params(vehicle_type),
        )
        return [str(model) for model in (data or []) if str(model).strip()]

    def get_submodels(
        self, year: int, make: str, model: str, vehicle_type: typing.Optional[str] = None
    ) -> typing.List[str]:
        """GET /v1/years/{year}/makes/{make}/models/{model}/submodels -> ["Raptor", "King Ranch", ...]"""
        data = self._get(
            "/v1/years/{}/makes/{}/models/{}/submodels".format(
                int(year), self._quote(make), self._quote(model)
            ),
            self._type_params(vehicle_type),
        )
        return [str(submodel) for submodel in (data or []) if str(submodel).strip()]

    # -- Detail endpoints -----------------------------------------------------------------------
    # Same payload shape; the submodel one additionally carries "subModel".

    def get_model_info(self, year: int, make: str, model: str) -> typing.Dict:
        """GET /v1/years/{year}/makes/{make}/models/{model}"""
        return self._get(
            "/v1/years/{}/makes/{}/models/{}".format(int(year), self._quote(make), self._quote(model))
        )

    def get_submodel_info(self, year: int, make: str, model: str, submodel: str) -> typing.Dict:
        """GET /v1/years/{year}/makes/{make}/models/{model}/submodels/{submodel}"""
        return self._get(
            "/v1/years/{}/makes/{}/models/{}/submodels/{}".format(
                int(year), self._quote(make), self._quote(model), self._quote(submodel)
            )
        )

    # -- Helpers ---------------------------------------------------------------------------------

    @staticmethod
    def _type_params(vehicle_type: typing.Optional[str]) -> typing.Dict:
        if not vehicle_type:
            return {}
        if vehicle_type not in VEHICLE_TYPES:
            raise ValueError("Invalid vehicle_type: {}. Must be one of {}.".format(vehicle_type, VEHICLE_TYPES))
        return {"type": vehicle_type}

    def test_connection(self) -> None:
        """
        Validate credentials *and* Vehicle API entitlement in one cheap call.

        GET /v1/years is the smallest real request the API has. It separates the three ways this
        can fail: bad username/password raise WheelProsOrderAuthError from the auth step, a valid
        account without the entitlement raises WheelProsVehiclePermissionError, and anything else
        is a transport problem.
        """
        self.get_years()
