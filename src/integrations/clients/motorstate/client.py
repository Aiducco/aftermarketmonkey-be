"""
Client for the Motor State Distributing read-only API (https://api.motorstate.com).

Auth is a single ``apiKey`` request header issued per dealer account. The API has
no bulk product export; the three endpoints used here are:

  * GET /api/Brands
  * GET /api/ProductAvailabilityChange?fromDateTime=&brand=&recordsLimit=
        Returns changed/known part numbers with StatusType and (when non-zero)
        QuantityAvailable. Hard-capped at 5000 rows per call; the documented
        ``partnumberGreaterThan`` keyset parameter is a server-side no-op, so the
        only way to page past 5000 for a large brand is to advance ``fromDateTime``.
  * GET /api/Product?partNumbers=a,b,c
        Per-part detail + account pricing. Hard-capped at 15 part numbers per call
        (exceeding it returns HTTP 403, not 400).

Rate limiting is intentionally left to the caller: Motor State did not throttle a
20-way concurrent burst in testing, and the sync service caps its own worker pool,
so this client stays a thin, thread-safe request wrapper (no shared lock state).
"""
import decimal
import logging
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.motorstate import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MOTOR-STATE-API-CLIENT]"

# /api/Product accepts at most this many comma-separated part numbers per call;
# more returns HTTP 403 "This endpoint can used for only 15 products at a time".
MAX_PRODUCT_BATCH = 15

# /api/ProductAvailabilityChange caps returned rows at 5000 regardless of recordsLimit.
MAX_AVAILABILITY_RECORDS = 5000

REQUEST_TIMEOUT_SECONDS = 30


class MotorStateApiClient(object):
    API_BASE_URL = settings.MOTOR_STATE_BASE_URL
    VALID_STATUS_CODES = [200]

    def __init__(self, credentials: typing.Dict):
        self.api_key = (credentials or {}).get("api_key", "")
        if not self.api_key:
            raise ValueError("Invalid credentials parameter: missing api_key.")
        # Reuse one HTTP connection pool across calls (and across worker threads) so the
        # product hydrate isn't paying a fresh TCP+TLS handshake on every 15-part request.
        # requests.Session's underlying urllib3 pool is safe for concurrent GETs.
        self._session = requests.Session()
        self._session.headers.update({"accept": "application/json", "apiKey": self.api_key})

    def test_connection(self) -> None:
        """Validate the key against the cheapest real endpoint (Brands)."""
        try:
            self.get_brands()
        except exceptions.MotorStateAPIBadResponseCodeError as e:
            if e.code == 403:
                raise exceptions.MotorStatePermissionError(
                    message=(
                        "Connected to Motor State, but this API key was rejected (HTTP 403). "
                        "Confirm the key is active for your account."
                    ),
                    code=e.code,
                )
            raise

    def get_brands(self) -> typing.List[typing.Dict]:
        """GET /api/Brands — full brand catalog (single unpaginated call)."""
        response = self._request(
            endpoint="api/Brands",
            method=common_enums.HttpMethod.GET,
        )
        data = self._get_response_data(response)
        return data if isinstance(data, list) else []

    def get_product_availability_changes(
        self,
        from_date_time: str,
        brand: typing.Optional[str] = None,
        records_limit: int = MAX_AVAILABILITY_RECORDS,
    ) -> typing.List[typing.Dict]:
        """
        GET /api/ProductAvailabilityChange — part numbers changed since ``from_date_time``
        (ISO date/datetime), optionally filtered to a single ``brand`` code. Each row:
        {PartNumber, UpdatedOn, StatusType, QuantityAvailable?}. QuantityAvailable is
        omitted when zero. Never returns more than 5000 rows.
        """
        params: typing.Dict[str, typing.Any] = {
            "fromDateTime": from_date_time,
            "recordsLimit": records_limit,
        }
        if brand:
            params["brand"] = brand

        response = self._request(
            endpoint="api/ProductAvailabilityChange",
            method=common_enums.HttpMethod.GET,
            params=params,
        )
        data = self._get_response_data(response)
        return data if isinstance(data, list) else []

    def get_products(self, part_numbers: typing.List[str]) -> typing.List[typing.Dict]:
        """
        GET /api/Product — detail + account pricing for up to 15 part numbers.
        Each row: {Found, PartNumber, Product?}. Raises ValueError above the cap
        rather than letting Motor State reject the batch with a 403.
        """
        cleaned = [str(pn).strip() for pn in (part_numbers or []) if str(pn).strip()]
        if not cleaned:
            return []
        if len(cleaned) > MAX_PRODUCT_BATCH:
            raise ValueError(
                "get_products accepts at most {} part numbers per call (got {}).".format(
                    MAX_PRODUCT_BATCH, len(cleaned)
                )
            )

        response = self._request(
            endpoint="api/Product",
            method=common_enums.HttpMethod.GET,
            params={"partNumbers": ",".join(cleaned)},
        )
        data = self._get_response_data(response)
        return data if isinstance(data, list) else []

    def _request(
        self,
        endpoint: str,
        method: common_enums.HttpMethod,
        params: typing.Optional[dict] = None,
        payload: typing.Optional[dict] = None,
    ) -> requests.Response:
        url = "{}/{}".format(self.API_BASE_URL, endpoint)

        try:
            response = self._session.request(
                url=url,
                method=method.value,
                params=params,
                json=payload,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code not in self.VALID_STATUS_CODES:
                msg = "Invalid API client response (status_code={}, data={}).".format(
                    response.status_code,
                    response.content.decode("utf-8", errors="replace"),
                )
                logger.error("{} {}".format(_LOG_PREFIX, msg))
                raise exceptions.MotorStateAPIBadResponseCodeError(
                    message=msg, code=response.status_code
                )
        except requests.exceptions.ConnectTimeout as e:
            msg = "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.MotorStateAPIException(msg)
        except requests.RequestException as e:
            msg = "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.MotorStateAPIException(msg)

        return response

    @staticmethod
    def _get_response_data(response: requests.Response) -> typing.Any:
        # parse_float=Decimal so price fields land as Decimal for the DecimalField columns.
        return simplejson.loads(response.content, parse_float=decimal.Decimal)
