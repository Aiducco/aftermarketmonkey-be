import decimal
import time
import typing
import requests
import logging
import simplejson
from django.conf import settings
from ratelimit import limits, sleep_and_retry  # Add ratelimit imports

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.turn_14 import exceptions

logger = logging.getLogger(__name__)

# Define rate limits
SECOND_LIMIT = 5
HOUR_LIMIT = 5000
DAY_LIMIT = 30000

# Token expiration buffer (refresh token 60 seconds before it expires)
TOKEN_EXPIRATION_BUFFER_SECONDS = 60


class Turn14ApiClient(object):
    API_BASE_URL = settings.TURN14_BASE_URL
    VALID_STATUS_CODES = [200]

    LOG_PREFIX = "[TURN14-API-CLIENT]"

    def __init__(self, credentials: typing.Dict):
        self.client_id = credentials.get("client_id", "")
        self.client_secret = credentials.get("client_secret", "")

        if not self.client_id or not self.client_secret:
            raise ValueError("Invalid credentials parameter.")
        
        # Token caching
        self._cached_token: typing.Optional[str] = None
        self._token_expires_at: typing.Optional[float] = None

    def _is_token_valid(self) -> bool:
        """Check if the cached token is still valid (not expired)."""
        if self._cached_token is None or self._token_expires_at is None:
            return False
        
        # Check if token expires within the buffer time
        current_time = time.time()
        return current_time < (self._token_expires_at - TOKEN_EXPIRATION_BUFFER_SECONDS)

    def _get_valid_token(self) -> str:
        """
        Get a valid access token, using cached token if available and not expired.
        Creates a new token only when necessary.
        """
        if self._is_token_valid():
            logger.debug("{} Using cached authorization token.".format(self.LOG_PREFIX))
            return self._cached_token
        
        # Token is expired or doesn't exist, create a new one
        logger.debug("{} Creating new authorization token.".format(self.LOG_PREFIX))
        auth_data = self._create_authorization_token()
        
        access_token = auth_data.get('access_token')
        if not access_token:
            raise exceptions.Turn14APIException("No access_token in authorization response.")
        
        # Cache the token
        self._cached_token = access_token
        
        # Calculate expiration time
        # expires_in is typically in seconds (OAuth2 standard)
        expires_in = auth_data.get('expires_in')
        if expires_in:
            # Convert to int if it's a Decimal
            if isinstance(expires_in, decimal.Decimal):
                expires_in = int(expires_in)
            self._token_expires_at = time.time() + expires_in
            logger.debug(
                "{} Token cached. Expires in {} seconds (at {}).".format(
                    self.LOG_PREFIX, expires_in, self._token_expires_at
                )
            )
        else:
            # If expires_in is not provided, assume 1 hour (3600 seconds) as default
            # This is a common OAuth2 default
            logger.warning(
                "{} No expires_in in token response. Assuming 1 hour expiration.".format(self.LOG_PREFIX)
            )
            self._token_expires_at = time.time() + 3600
        
        return self._cached_token

    @sleep_and_retry
    @limits(calls=20, period=60)
    def _create_authorization_token(self) -> typing.Dict:
        """
        Internal method to create a new authorization token.
        This method is rate-limited and should only be called when necessary.
        """
        try:
            response = requests.request(
                url=f"{self.API_BASE_URL}/token",
                method=common_enums.HttpMethod.POST.value,
                headers={
                    "Content-Type": "application/json",
                },
                json={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }
            )

            if response.status_code not in self.VALID_STATUS_CODES:
                msg = "Invalid API client response (status_code={}, data={})".format(
                    response.status_code,
                    response.content.decode(encoding="utf-8"),
                )
                logger.error("{} {}.".format(self.LOG_PREFIX, msg))
                raise exceptions.Turn14APIBadResponseCodeError(message=msg, code=response.status_code)

        except requests.exceptions.ConnectTimeout as e:
            msg = "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.Turn14APIException(msg)
        except requests.RequestException as e:
            msg = "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.Turn14APIException(msg)

        return simplejson.loads(response.content, parse_float=decimal.Decimal)

    def get_pricelists(
            self, brand_id: int, page: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        response = simplejson.loads(
            self._request(
                endpoint="pricing/brand/{}".format(brand_id),
                method=common_enums.HttpMethod.GET,
                params={
                    "page": page,
                },
            ).content
        )

        data = response.get("data", [])
        potential_next_page = page + 1
        next_page = None if page == response.get("meta", {}).get("total_pages", 1) else potential_next_page

        return data, next_page

    def get_items_for_brand(
            self, brand_id: int, page: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        response = simplejson.loads(
            self._request(
                endpoint="items/brand/{}".format(brand_id),
                method=common_enums.HttpMethod.GET,
                params={
                    "page": page,
                },
            ).content
        )

        data = response.get("data", [])
        potential_next_page = page + 1
        next_page = None if page == response.get("meta", {}).get("total_pages", 1) else potential_next_page

        return data, next_page

    def get_inventory_items_for_brand(
            self, brand_id: int, page: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        response = simplejson.loads(
            self._request(
                endpoint="inventory/brand/{}".format(brand_id),
                method=common_enums.HttpMethod.GET,
                params={
                    "page": page,
                },
            ).content
        )

        data = response.get("data", [])
        potential_next_page = page + 1
        next_page = None if page == response.get("meta", {}).get("total_pages", 1) else potential_next_page

        return data, next_page

    def get_inventory_items_updates(
            self, page: int = 1, minutes: int = 60
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        response = simplejson.loads(
            self._request(
                endpoint="inventory/updates",
                method=common_enums.HttpMethod.GET,
                params={
                    "page": page,
                    "minutes": str(minutes),
                },
            ).content
        )

        data = response.get("data", [])
        potential_next_page = page + 1
        next_page = None if page == response.get("meta", {}).get("total_pages", 1) else potential_next_page

        return data, next_page

    def get_items_updates(self, page: int = 1, days: int = 1) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        response = simplejson.loads(
            self._request(
                endpoint="items/updates",
                method=common_enums.HttpMethod.GET,
                params={
                    "page": page,
                    "days": str(days),
                },
            ).content
        )

        data = response.get("data", [])
        potential_next_page = page + 1
        next_page = None if page == response.get("meta", {}).get("total_pages", 1) else potential_next_page

        return data, next_page

    def get_brand_media(self, brand_id: str, page: int = 1) -> typing.Tuple[
        typing.List[typing.Dict], typing.Optional[int]]:
        response = simplejson.loads(
            self._request(
                endpoint="items/data/brand/{}".format(brand_id),
                method=common_enums.HttpMethod.GET,
                params={
                    "page": page,
                },
            ).content
        )

        data = response.get("data", [])
        potential_next_page = page + 1
        next_page = None if page == response.get("meta", {}).get("total_pages", 1) else potential_next_page

        return data, next_page

    def get_brands(self) -> typing.List[typing.Dict]:
        return simplejson.loads(
            self._request(
                endpoint="brands",
                method=common_enums.HttpMethod.GET,
            ).content
        ).get("data", [])

    def _clear_token_cache(self) -> None:
        """Clear the cached token and expiration time."""
        self._cached_token = None
        self._token_expires_at = None
        logger.debug("{} Cleared cached authorization token.".format(self.LOG_PREFIX))

    @sleep_and_retry
    @limits(calls=SECOND_LIMIT, period=1)  # 5 requests per second
    @limits(calls=20, period=60)  # 5 requests per second
    @limits(calls=HOUR_LIMIT, period=3600)  # 5000 requests per hour
    @limits(calls=DAY_LIMIT, period=86400)  # 30000 requests per day
    def _request(
            self,
            endpoint: str,
            method: common_enums.HttpMethod,
            params: typing.Optional[dict] = None,
            payload: typing.Optional[dict] = None,
            include_auth: bool = True,
            retry_on_401: bool = True,
    ) -> requests.Response:
        url = f"{self.API_BASE_URL}/{endpoint}"
        headers = {
            "Content-Type": "application/json",
        }

        if include_auth:
            access_token = self._get_valid_token()
            headers["Authorization"] = f"Bearer {access_token}"

        try:
            response = requests.request(
                url=url,
                method=method.value,
                params=params,
                json=payload,
                headers=headers,
            )

            # Handle 401 Unauthorized - token might be invalid
            if response.status_code == 401 and include_auth and retry_on_401:
                logger.warning(
                    "{} Received 401 Unauthorized. Clearing token cache and retrying once (endpoint={}).".format(
                        self.LOG_PREFIX, endpoint
                    )
                )
                self._clear_token_cache()
                # Retry once with a fresh token
                return self._request(
                    endpoint=endpoint,
                    method=method,
                    params=params,
                    payload=payload,
                    include_auth=include_auth,
                    retry_on_401=False,  # Don't retry again to avoid infinite loop
                )

            if response.status_code not in self.VALID_STATUS_CODES:
                msg = f"Invalid API client response (status_code={response.status_code}, data={response.content.decode('utf-8')})"
                logger.error(f"{self.LOG_PREFIX} {msg}.")
                raise exceptions.Turn14APIBadResponseCodeError(message=msg, code=response.status_code)

            logger.debug(
                f"{self.LOG_PREFIX} Successful response (endpoint={endpoint}, status_code={response.status_code}, payload={payload}, params={params}, raw_response={response.content.decode('utf-8')})."
            )
        except requests.exceptions.ConnectTimeout as e:
            msg = f"Connect timeout. Error: {common_utils.get_exception_message(exception=e)}"
            logger.exception(f"{self.LOG_PREFIX} {msg}.")
            raise exceptions.Turn14APIException(msg)
        except requests.RequestException as e:
            msg = f"Request exception. Error: {common_utils.get_exception_message(exception=e)}"
            logger.exception(f"{self.LOG_PREFIX} {msg}.")
            raise exceptions.Turn14APIException(msg)

        return response

    @staticmethod
    def _get_response_data(response: requests.Response) -> typing.Dict:
        return simplejson.loads(
            response.content,
            parse_float=decimal.Decimal,
        )