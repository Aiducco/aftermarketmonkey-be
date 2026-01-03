import typing
import time
import requests
import logging
import simplejson
from django.conf import settings
from ratelimit import limits, sleep_and_retry

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.ecommerce.bigcommerce.gateways import exceptions

logger = logging.getLogger(__name__)

# Define rate limits based on BigCommerce's 30-second quota window
# Standard/Plus: 150 requests per 30 seconds
# Pro: 450 requests per 30 seconds
# Enterprise: varies by plan
# Using conservative default of 150 per 30 seconds (safe for Standard/Plus)
REQUESTS_PER_30_SECONDS = 150


class BigCommerceApiClient(object):
    API_BASE_URL = "https://api.bigcommerce.com/stores"
    VALID_STATUS_CODES = [200, 201, 204, 207]
    RATE_LIMIT_STATUS_CODE = 429

    LOG_PREFIX = "[BIGCOMMERCE-API-CLIENT]"

    def __init__(self, credentials: typing.Dict):
        self.store_hash = credentials.get("store_hash", "")
        self.access_token = credentials.get("access_token", "")

        if not self.store_hash or not self.access_token:
            raise ValueError("Invalid credentials parameter. Both store_hash and access_token are required.")

    @sleep_and_retry
    @limits(calls=REQUESTS_PER_30_SECONDS, period=30)  # BigCommerce quota refreshes every 30 seconds
    def _request(
            self,
            endpoint: str,
            method: common_enums.HttpMethod,
            params: typing.Optional[dict] = None,
            payload: typing.Optional[dict] = None,
            max_retries: int = 3,
    ) -> requests.Response:
        url = f"{self.API_BASE_URL}/{self.store_hash}/v3/{endpoint}"
        headers = {
            "X-Auth-Token": self.access_token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        for attempt in range(max_retries):
            try:
                response = requests.request(
                    url=url,
                    method=method.value,
                    params=params,
                    json=payload,
                    headers=headers,
                )

                # Handle rate limit (429) responses
                if response.status_code == self.RATE_LIMIT_STATUS_CODE:
                    retry_after_ms = self._extract_retry_after_ms(response)
                    if retry_after_ms and attempt < max_retries - 1:
                        wait_time_seconds = (retry_after_ms / 1000.0) + 0.1  # Add small buffer
                        logger.warning(
                            f"{self.LOG_PREFIX} Rate limit hit (endpoint={endpoint}, attempt={attempt + 1}/{max_retries}). "
                            f"Waiting {wait_time_seconds:.2f} seconds before retry."
                        )
                        time.sleep(wait_time_seconds)
                        continue
                    else:
                        msg = f"Rate limit exceeded (status_code={response.status_code}, endpoint={endpoint})"
                        logger.error(f"{self.LOG_PREFIX} {msg}.")
                        raise exceptions.BigCommerceAPIRateLimitError(
                            message=msg,
                            retry_after_ms=retry_after_ms
                        )

                if response.status_code not in self.VALID_STATUS_CODES:
                    msg = f"Invalid API client response (status_code={response.status_code}, data={response.json})"
                    logger.error(f"{self.LOG_PREFIX} {msg}.")
                    raise exceptions.BigCommerceAPIBadResponseCodeError(message=msg, code=response.status_code)

                # Log rate limit headers for monitoring
                self._log_rate_limit_headers(response, endpoint)

                logger.debug(
                    f"{self.LOG_PREFIX} Successful response (endpoint={endpoint}, status_code={response.status_code}, payload={payload}, params={params}, raw_response={response.content.decode('utf-8')})."
                )
                return response

            except (exceptions.BigCommerceAPIRateLimitError, exceptions.BigCommerceAPIBadResponseCodeError):
                raise
            except requests.exceptions.ConnectTimeout as e:
                msg = f"Connect timeout. Error: {common_utils.get_exception_message(exception=e)}"
                logger.exception(f"{self.LOG_PREFIX} {msg}.")
                raise exceptions.BigCommerceAPIException(msg)
            except requests.RequestException as e:
                msg = f"Request exception. Error: {common_utils.get_exception_message(exception=e)}"
                logger.exception(f"{self.LOG_PREFIX} {msg}.")
                raise exceptions.BigCommerceAPIException(msg)

        # Should not reach here, but just in case
        raise exceptions.BigCommerceAPIException("Max retries exceeded for request")

    def _extract_retry_after_ms(self, response: requests.Response) -> typing.Optional[int]:
        retry_after_header = response.headers.get("X-Rate-Limit-Time-Reset-Ms")
        if retry_after_header:
            try:
                return int(retry_after_header)
            except (ValueError, TypeError):
                logger.warning(
                    f"{self.LOG_PREFIX} Invalid X-Rate-Limit-Time-Reset-Ms header value: {retry_after_header}"
                )
        return None

    def _log_rate_limit_headers(self, response: requests.Response, endpoint: str) -> None:
        rate_limit_headers = {
            "X-Rate-Limit-Time-Window-Ms": response.headers.get("X-Rate-Limit-Time-Window-Ms"),
            "X-Rate-Limit-Time-Reset-Ms": response.headers.get("X-Rate-Limit-Time-Reset-Ms"),
            "X-Rate-Limit-Requests-Quota": response.headers.get("X-Rate-Limit-Requests-Quota"),
            "X-Rate-Limit-Requests-Left": response.headers.get("X-Rate-Limit-Requests-Left"),
        }
        if any(rate_limit_headers.values()):
            logger.debug(
                f"{self.LOG_PREFIX} Rate limit headers (endpoint={endpoint}): {rate_limit_headers}"
            )

    def get_brands(self, page: int = 1) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        response = simplejson.loads(
            self._request(
                endpoint="catalog/brands",
                method=common_enums.HttpMethod.GET,
                params={
                    "page": page,
                },
            ).content
        )

        data = response.get("data", [])
        pagination = response.get("meta", {}).get("pagination", {})
        total_pages = pagination.get("total_pages", 1)
        potential_next_page = page + 1
        next_page = None if page >= total_pages else potential_next_page

        return data, next_page

    def get_products(self, page: int = 1) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        response = simplejson.loads(
            self._request(
                endpoint="catalog/products",
                method=common_enums.HttpMethod.GET,
                params={
                    "page": page,
                },
            ).content
        )

        data = response.get("data", [])
        pagination = response.get("meta", {}).get("pagination", {})
        total_pages = pagination.get("total_pages", 1)
        potential_next_page = page + 1
        next_page = None if page >= total_pages else potential_next_page

        return data, next_page

    def create_product(self, product_data: typing.Dict) -> typing.Dict:
        response = simplejson.loads(
            self._request(
                endpoint="catalog/products",
                method=common_enums.HttpMethod.POST,
                payload=product_data,
                params={
                    "include": "images,custom_fields",
                },
            ).content
        )
        return response.get("data", {})

    def update_product(self, product_id: int, product_data: typing.Dict) -> typing.Dict:
        response = simplejson.loads(
            self._request(
                endpoint=f"catalog/products/{product_id}",
                method=common_enums.HttpMethod.PUT,
                payload=product_data,
                params={
                    "include": "images,custom_fields",
                },
            ).content
        )
        return response.get("data", {})

    def get_product_images(self, product_id: int) -> typing.List[typing.Dict]:
        response = simplejson.loads(
            self._request(
                endpoint=f"catalog/products/{product_id}/images",
                method=common_enums.HttpMethod.GET,
            ).content
        )
        return response.get("data", [])

    def delete_product_image(self, product_id: int, image_id: int) -> None:
        self._request(
            endpoint=f"catalog/products/{product_id}/images/{image_id}",
            method=common_enums.HttpMethod.DELETE,
        )

    def create_product_image(self, product_id: int, image_data: typing.Dict) -> typing.Dict:
        response = simplejson.loads(
            self._request(
                endpoint=f"catalog/products/{product_id}/images",
                method=common_enums.HttpMethod.POST,
                payload=image_data,
            ).content
        )
        return response.get("data", {})

    def get_product(self, product_id: int) -> typing.Dict:
        response = simplejson.loads(
            self._request(
                endpoint=f"catalog/products/{product_id}",
                method=common_enums.HttpMethod.GET,
                params={
                    "include": "images,custom_fields",
                },
            ).content
        )
        return response.get("data", {})

    def get_product_custom_fields(self, product_id: int) -> typing.List[typing.Dict]:
        response = simplejson.loads(
            self._request(
                endpoint=f"catalog/products/{product_id}/custom-fields",
                method=common_enums.HttpMethod.GET,
            ).content
        )
        return response.get("data", [])

    def update_product_custom_field(self, product_id: int, custom_field_id: int, custom_field_data: typing.Dict) -> typing.Dict:
        response = simplejson.loads(
            self._request(
                endpoint=f"catalog/products/{product_id}/custom-fields/{custom_field_id}",
                method=common_enums.HttpMethod.PUT,
                payload=custom_field_data,
            ).content
        )
        return response.get("data", {})

    def create_product_custom_field(self, product_id: int, custom_field_data: typing.Dict) -> typing.Dict:
        response = simplejson.loads(
            self._request(
                endpoint=f"catalog/products/{product_id}/custom-fields",
                method=common_enums.HttpMethod.POST,
                payload=custom_field_data,
            ).content
        )
        return response.get("data", {})

    def delete_product_custom_field(self, product_id: int, custom_field_id: int) -> None:
        self._request(
            endpoint=f"catalog/products/{product_id}/custom-fields/{custom_field_id}",
            method=common_enums.HttpMethod.DELETE,
        )

    def create_category(self, category_data: typing.List[typing.Dict]) -> typing.List[typing.Dict]:
        response = simplejson.loads(
            self._request(
                endpoint="catalog/trees/categories",
                method=common_enums.HttpMethod.POST,
                payload=category_data,
            ).content
        )
        return response.get("data", [])

    def create_brand(self, brand_data: typing.Dict) -> typing.Dict:
        response = simplejson.loads(
            self._request(
                endpoint="catalog/brands",
                method=common_enums.HttpMethod.POST,
                payload=brand_data,
            ).content
        )
        return response.get("data", {})

    def delete_products(self, product_ids: typing.List[int]) -> None:
        """
        Delete multiple products by their IDs.
        Uses the id:in query parameter to delete products in bulk.
        """
        if not product_ids:
            return
        
        # Format product IDs as comma-separated string
        ids_str = ','.join(str(pid) for pid in product_ids)
        
        self._request(
            endpoint="catalog/products",
            method=common_enums.HttpMethod.DELETE,
            params={
                "id:in": ids_str,
            },
        )

