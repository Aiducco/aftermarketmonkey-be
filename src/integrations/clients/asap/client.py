import logging
import time
import typing

import requests
from django.conf import settings
from ratelimit import limits, sleep_and_retry

from common import utils as common_utils
from src.integrations.clients.asap import exceptions

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 30
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
RETRY_BACKOFF_SECONDS = 2
RETRY_ATTEMPTS = 2  # applies to both retryable status codes and timeouts/connection errors

# ASAP Network's real rate limit isn't documented anywhere we could confirm (docs page is behind
# a bot-check wall). We were seeing read timeouts at 8 concurrent workers with no per-second cap
# - likely us overloading their API, not just transient network flakiness - so this adds an actual
# requests/second ceiling (shared across all worker threads via the ratelimit library, same
# pattern as Turn14's client) on top of the existing concurrency cap. Paid per call, so avoiding
# self-inflicted timeouts matters as much as avoiding wasted retries.
ASAP_MAX_REQUESTS_PER_SECOND = int(getattr(settings, "ASAP_NETWORK_MAX_REQUESTS_PER_SECOND", 5))


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

    @sleep_and_retry
    @limits(calls=ASAP_MAX_REQUESTS_PER_SECOND, period=1)
    def _request(self, endpoint: str, retry_count: int = RETRY_ATTEMPTS) -> typing.Dict:
        url = "{}/{}".format(self.API_BASE_URL, endpoint)
        headers = {"Authorization": "Bearer {}".format(self.api_token)}

        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # Covers both ConnectTimeout and ReadTimeout (ReadTimeout used to fall through to the
            # generic RequestException branch below and never got retried - that was the bug: a
            # single slow response permanently lost that SKU's fitment data and wasted the call).
            if retry_count > 0:
                wait_s = RETRY_BACKOFF_SECONDS * (RETRY_ATTEMPTS - retry_count + 1)
                logger.warning(
                    "{} {} for endpoint={}; retrying after {}s ({} attempt(s) left).".format(
                        self.LOG_PREFIX, type(e).__name__, endpoint, wait_s, retry_count
                    )
                )
                time.sleep(wait_s)
                return self._request(endpoint, retry_count=retry_count - 1)
            msg = "{} (exhausted retries). Error: {}".format(
                type(e).__name__, common_utils.get_exception_message(exception=e)
            )
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.AsapAPIException(msg)
        except requests.RequestException as e:
            msg = "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.AsapAPIException(msg)

        if response.status_code in RETRY_STATUS_CODES and retry_count > 0:
            wait_s = RETRY_BACKOFF_SECONDS * (RETRY_ATTEMPTS - retry_count + 1)
            logger.warning(
                "{} Retryable status_code={} for endpoint={}; retrying after {}s ({} attempt(s) left).".format(
                    self.LOG_PREFIX, response.status_code, endpoint, wait_s, retry_count
                )
            )
            time.sleep(wait_s)
            return self._request(endpoint, retry_count=retry_count - 1)

        if response.status_code not in self.VALID_STATUS_CODES:
            msg = "Invalid API client response (status_code={}, endpoint={}, data={})".format(
                response.status_code, endpoint, response.content.decode("utf-8", errors="replace")
            )
            logger.error("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.AsapAPIBadResponseCodeError(message=msg, code=response.status_code)

        return response.json()
