"""
Transport client for ShopMonkey's REST API. Auth is a single ``api_key`` sent as a Bearer
token on every request — no token exchange, unlike Premier's apiKey->session-token flow (see
``src/integrations/clients/premier/order_client.py``, the closest existing template this
mirrors).

Confirmed 2026-10-14 against shopmonkey.dev: a bearer token (an API key generated in the
ShopMonkey app at Settings > Integration > API Keys, not an OAuth2 client_id/client_secret flow —
no app-registration or authorization-code flow exists in their public docs), base URL
``https://api.shopmonkey.cloud/v3`` (``settings.SHOPMONKEY_API_BASE_URL``'s own default), and
their own purpose-built key-validation endpoint (``GET auth/api_key/status``) for
``test_connection()`` rather than a guessed general-purpose one.

Write operations (e.g. pushing parts onto a repair order) are intentionally not implemented yet
— this client currently only supports validating that an api_key works.
"""
import decimal
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.shopmonkey import exceptions

REQUEST_TIMEOUT_SECONDS = 30


class ShopMonkeyApiClient(object):
    """One instance per set of credentials (one CompanyShopManagementProviders connection)."""

    def __init__(self, credentials: typing.Dict) -> None:
        self.api_key = (credentials or {}).get("api_key", "")
        if not self.api_key:
            raise ValueError("Invalid credentials parameter.")
        self.api_base_url = settings.SHOPMONKEY_API_BASE_URL

    def _request(
        self,
        endpoint: str,
        method: common_enums.HttpMethod,
        params: typing.Optional[dict] = None,
        json_body: typing.Optional[dict] = None,
    ) -> typing.Union[typing.Dict, typing.List]:
        url = "{}/{}".format(self.api_base_url, endpoint)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.api_key),
        }
        try:
            response = requests.request(
                url=url,
                method=method.value,
                params=params,
                json=json_body,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.ConnectTimeout as e:
            raise exceptions.ShopMonkeyAPIException(
                "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            )
        except requests.RequestException as e:
            raise exceptions.ShopMonkeyAPIException(
                "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            )

        if response.status_code in (401, 403):
            raise exceptions.ShopMonkeyAuthError(
                "ShopMonkey API rejected the api_key (status_code={}).".format(response.status_code)
            )
        if not (200 <= response.status_code < 300):
            raise exceptions.ShopMonkeyValidationError(
                message="ShopMonkey API error (status_code={}): {}".format(
                    response.status_code, response.content.decode("utf-8", errors="replace")[:2000]
                ),
                code=str(response.status_code),
            )

        if not response.content:
            return {}
        try:
            return simplejson.loads(response.content, parse_float=decimal.Decimal)
        except simplejson.JSONDecodeError as e:
            raise exceptions.ShopMonkeyAPIException(
                "Non-JSON response from ShopMonkey {} {} ({}). Raw: {}".format(
                    method.value, endpoint, e, response.content.decode("utf-8", errors="replace")[:2000]
                )
            )

    def test_connection(self) -> None:
        """Cheap connectivity/auth probe — confirms the api_key is accepted without touching
        any customer/repair-order data. ShopMonkey's own documented example:
        ``curl https://api.shopmonkey.cloud/v3/auth/api_key/status -H 'Authorization: Bearer ...'``"""
        self._request(endpoint="auth/api_key/status", method=common_enums.HttpMethod.GET)
