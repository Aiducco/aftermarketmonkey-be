import logging
import time
import typing

import requests
from django.conf import settings

from common import utils as common_utils
from src.integrations.clients.asap import exceptions

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 30
# ASAP Network's rate limit isn't documented anywhere we could confirm (docs page is behind a
# bot-check wall) - one conservative retry on 429/5xx, not a tuned rate limiter.
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
RETRY_BACKOFF_SECONDS = 2


class AsapApiClient(object):
    API_BASE_URL = "https://api.asapnetwork.org/webapi"
    VALID_STATUS_CODES = [200]

    LOG_PREFIX = "[ASAP-API-CLIENT]"

    def __init__(self, api_token: typing.Optional[str] = None):
        self.api_token = api_token or getattr(settings, "ASAP_NETWORK_API_TOKEN", "")
        if not self.api_token:
            raise ValueError("Missing ASAP_NETWORK_API_TOKEN.")

    def get_brands(self) -> typing.Dict:
        """GET /brands -> {"count": int, "brands": {brand_id: {term_name, brand_id, name}}}"""
        return self._request("brands").get("brands", {})

    def get_products(self, brand_id: str) -> typing.Dict:
        """GET /products/{brand_id} -> {"count": int, "products": {sku: {sku, title, changed}}}"""
        return self._request("products/{}".format(brand_id)).get("products", {})

    def get_product_detail(self, sku: str) -> typing.Dict:
        """GET /product/{sku} -> full product payload, including the ``fitment`` array."""
        return self._request("product/{}".format(sku))

    def _request(self, endpoint: str, retry_count: int = 1) -> typing.Dict:
        url = "{}/{}".format(self.API_BASE_URL, endpoint)
        headers = {"Authorization": "Bearer {}".format(self.api_token)}

        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)

            if response.status_code in RETRY_STATUS_CODES and retry_count > 0:
                logger.warning(
                    "{} Retryable status_code={} for endpoint={}; retrying once after {}s.".format(
                        self.LOG_PREFIX, response.status_code, endpoint, RETRY_BACKOFF_SECONDS
                    )
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
                return self._request(endpoint, retry_count=retry_count - 1)

            if response.status_code not in self.VALID_STATUS_CODES:
                msg = "Invalid API client response (status_code={}, endpoint={}, data={})".format(
                    response.status_code, endpoint, response.content.decode("utf-8", errors="replace")
                )
                logger.error("{} {}.".format(self.LOG_PREFIX, msg))
                raise exceptions.AsapAPIBadResponseCodeError(message=msg, code=response.status_code)
        except requests.exceptions.ConnectTimeout as e:
            msg = "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.AsapAPIException(msg)
        except requests.RequestException as e:
            msg = "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.AsapAPIException(msg)

        return response.json()
