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

Vendor/brand/inventory-part methods below are ported from a standalone single-tenant tool built
for Trident Motorsports (still live, ~88K parts pushed) -- endpoints and payload shapes are
copied from that tool's own working client, not re-guessed, including its one real quirk:
``vendorId`` is always sent as ``null`` on inventory_part create/update (the tool creates vendors
but never actually attaches one to a part) -- preserved deliberately rather than "fixed", per
explicit instruction when porting this. Response envelope there is ``{"success": bool, "data":
...}``; ``_request`` unwraps ``data`` the same way.
"""
import decimal
import time
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.shopmonkey import exceptions

REQUEST_TIMEOUT_SECONDS = 30
# Matches the old tool's own retry ceiling for 429s.
MAX_RATE_LIMIT_RETRIES = 5


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

        # Same 429 handling as the old tool's client: respect Retry-After, retry up to
        # MAX_RATE_LIMIT_RETRIES times rather than failing the whole sync on a transient throttle.
        retry_count = 0
        while True:
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

            if response.status_code == 429 and retry_count < MAX_RATE_LIMIT_RETRIES:
                retry_after = int(response.headers.get("Retry-After") or 1)
                time.sleep(retry_after)
                retry_count += 1
                continue
            break

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

    def _request_data(
        self,
        endpoint: str,
        method: common_enums.HttpMethod,
        params: typing.Optional[dict] = None,
        json_body: typing.Optional[dict] = None,
    ) -> typing.Union[typing.Dict, typing.List]:
        """Same as _request, but unwraps the vendor/brand/inventory_part endpoints' own envelope
        (``{"success": bool, "data": ...}``) -- the old tool's client did this unconditionally;
        kept separate from _request itself since auth/api_key/status (test_connection) does not
        use this envelope shape."""
        parsed = self._request(endpoint=endpoint, method=method, params=params, json_body=json_body)
        if isinstance(parsed, dict) and "data" in parsed:
            return parsed["data"]
        return parsed

    def test_connection(self) -> None:
        """Cheap connectivity/auth probe — confirms the api_key is accepted without touching
        any customer/repair-order data. ShopMonkey's own documented example:
        ``curl https://api.shopmonkey.cloud/v3/auth/api_key/status -H 'Authorization: Bearer ...'``"""
        self._request(endpoint="auth/api_key/status", method=common_enums.HttpMethod.GET)

    # ------------------------------------------------------------------------------------
    # Vendors
    # ------------------------------------------------------------------------------------
    def get_vendors(self) -> typing.List[typing.Dict]:
        return self._request_data(endpoint="vendor", method=common_enums.HttpMethod.GET)

    def create_vendor(self, vendor_name: str) -> typing.Dict:
        return self._request_data(
            endpoint="vendor",
            method=common_enums.HttpMethod.POST,
            json_body={"name": vendor_name, "entity": "part"},
        )

    def delete_vendor(self, external_id: str) -> typing.Dict:
        return self._request_data(
            endpoint="vendor/{}".format(external_id), method=common_enums.HttpMethod.DELETE
        )

    # ------------------------------------------------------------------------------------
    # Brands
    # ------------------------------------------------------------------------------------
    def get_brands(self) -> typing.List[typing.Dict]:
        return self._request_data(endpoint="brand", method=common_enums.HttpMethod.GET)

    def create_brand(self, entity: str, brand_name: str) -> typing.Dict:
        return self._request_data(
            endpoint="brand",
            method=common_enums.HttpMethod.POST,
            json_body={"name": brand_name, "entity": entity},
        )

    def update_brand(self, external_id: str, entity: str, name: str) -> typing.Dict:
        return self._request_data(
            endpoint="brand/{}".format(external_id),
            method=common_enums.HttpMethod.PUT,
            json_body={"entity": entity, "name": name},
        )

    def delete_brand(self, external_id: str) -> typing.Dict:
        return self._request_data(
            endpoint="brand/{}".format(external_id), method=common_enums.HttpMethod.DELETE
        )

    # ------------------------------------------------------------------------------------
    # Inventory parts
    # ------------------------------------------------------------------------------------
    def create_inventory_part(
        self,
        name: str,
        vendor_id: typing.Optional[str],
        brand_id: str,
        number: str,
        sku: str,
        retail_cost: int,
        wholesale_cost: int,
        vendor_bin_location: typing.Optional[str] = None,
    ) -> typing.Dict:
        # vendor_id is accepted (and threaded through from the vendor we create) but deliberately
        # never sent — the old tool's own client always sent vendorId: null on both create and
        # update, and this mirrors that exactly rather than guessing it should be wired up. See
        # the module docstring.
        return self._request_data(
            endpoint="inventory_part",
            method=common_enums.HttpMethod.POST,
            json_body={
                "name": name,
                "vendorId": None,
                "brandId": brand_id,
                "number": number,
                "sku": sku,
                "retailCostCents": retail_cost,
                "wholesaleCostCents": wholesale_cost,
                "binLocation": vendor_bin_location,
                "showNote": False,
            },
        )

    def update_inventory_part(
        self,
        external_id: str,
        name: str,
        vendor_id: typing.Optional[str],
        brand_id: str,
        number: str,
        sku: str,
        retail_cost: int,
        wholesale_cost: int,
        vendor_bin_location: typing.Optional[str] = None,
    ) -> typing.Dict:
        return self._request_data(
            endpoint="inventory_part/{}".format(external_id),
            method=common_enums.HttpMethod.PUT,
            json_body={
                "name": name,
                "vendorId": None,
                "brandId": brand_id,
                "number": number,
                "sku": sku,
                "retailCostCents": retail_cost,
                "wholesaleCostCents": wholesale_cost,
                "binLocation": vendor_bin_location,
                "showNote": False,
            },
        )

    def get_inventory_part(self, external_id: str) -> typing.Dict:
        return self._request_data(
            endpoint="inventory_part/{}".format(external_id), method=common_enums.HttpMethod.GET
        )

    def delete_inventory_part(self, external_id: str) -> typing.Dict:
        """Soft-delete -- the old tool's client PATCHes .../delete rather than issuing a real
        DELETE; mirrored as-is."""
        return self._request_data(
            endpoint="inventory_part/{}/delete".format(external_id), method=common_enums.HttpMethod.PATCH
        )
