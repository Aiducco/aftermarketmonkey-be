"""
Turn14OrderAdapter — the reference DistributorOrderAdapter implementation (see the Purchase
Orders plan for why Turn14 was chosen first). Wraps
``src.integrations.clients.turn_14.order_client.Turn14OrderApiClient``.

SAFETY: submit_order() places a REAL order against Turn 14 (even in their "testing"
environment, per their docs this creates a real test order in their system, not a dry-run).
It must only ever be invoked from an explicit, user-approved submission — never from
exploratory/dev code, automated tests, or ad-hoc scripts.
"""
import decimal
import logging
import typing

from django.conf import settings

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.turn_14 import exceptions as turn14_client_exceptions
from src.integrations.clients.turn_14.order_client import Turn14OrderApiClient
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[TURN14-ORDER-ADAPTER]"

# Turn 14's "default" location lets them auto-select the best warehouse(s) to fulfill from,
# instead of us pinning specific location codes — matches their own recommended approach for
# integrators who don't need manual per-warehouse control.
_DEFAULT_LOCATION = "default"

# "Best Premium Ground" shipping group (UPS Ground / FedEx Ground / OnTrac Ground, cheapest of
# the group). Used only when a PurchaseOrder has no ship_method set yet — see submit_order().
_DEFAULT_SHIPPING_GROUP_CODE = "7002"


class Turn14OrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.TURN_14.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider)
        environment = getattr(settings, "TURN14_ORDER_ENVIRONMENT", "testing")
        self._client = Turn14OrderApiClient(
            credentials=company_provider.credentials or {},
            environment=environment,
        )

    # -- Request building -----------------------------------------------------------------

    @staticmethod
    def _build_recipient(ship_to: base.ShipToAddress) -> typing.Dict:
        recipient = {
            "company": ship_to.attention or ship_to.name,
            "name": ship_to.name,
            "address": ship_to.address1,
            "city": ship_to.city,
            "state": ship_to.state,
            "country": ship_to.country,
            "zip": ship_to.postal_code,
            "phone_number": ship_to.phone or "",
            # We don't yet distinguish "shipping to the shop's own address" from "drop-shipping
            # to an end customer" at the internal PO level, so default to the drop-ship (False)
            # semantics — this is informational to Turn 14, not fulfillment-blocking.
            "is_shop_address": False,
        }
        if ship_to.address2:
            recipient["address_2"] = ship_to.address2
        if ship_to.email:
            recipient["email_address"] = ship_to.email
        return recipient

    @staticmethod
    def _build_locations(
        line_items: typing.List[base.OrderLineItemRequest],
        shipping_code: typing.Optional[str] = None,
    ) -> typing.List[typing.Dict]:
        items = [
            {
                "item_identifier": li.provider_part.provider_external_id,
                "item_identifier_type": "item_id",
                "quantity": li.quantity,
            }
            for li in line_items
        ]
        location = {
            "location": _DEFAULT_LOCATION,
            "combine_in_out_stock": False,
            "items": items,
        }
        if shipping_code is not None:
            location["shipping"] = {
                "shipping_code": shipping_code,
                "saturday_delivery": False,
                "signature_required": False,
            }
        return [location]

    def _handle_error(self, e: turn14_client_exceptions.Turn14APIBadResponseCodeError) -> None:
        raise order_exceptions.OrderValidationError(message=e.message, code=str(e.code))

    # -- DistributorOrderAdapter ------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        # Per Turn 14's docs: on a "default" location quote, providing a shipping_code
        # doesn't select it outright — it filters the computed rate to that carrier only
        # (e.g. shipping_code=3 -> only UPS rates considered). There is no single call that
        # returns multiple priced options; comparing methods means re-quoting per method.
        data = {
            "environment": self._client.environment,
            "locations": self._build_locations(line_items, shipping_code=ship_method),
            "recipient": self._build_recipient(ship_to),
        }
        try:
            response = self._client.create_quote(data)
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

        attrs = response.get("data", {}).get("attributes", {})
        by_external_id = {li.provider_part.provider_external_id: li for li in line_items}

        lines: typing.List[base.ShippingQuoteLine] = []
        for shipment in attrs.get("shipment", []):
            shipping_block = shipment.get("shipping", {}) or {}
            ship_option = base.ShipOption(
                service_level_code=str(shipping_block.get("shipping_code", "")),
                service_level_name=shipping_block.get("verbose_eta", ""),
                cost=(
                    decimal.Decimal(str(shipping_block["cost"]))
                    if shipping_block.get("cost") is not None
                    else None
                ),
            )
            for item in shipment.get("items", []):
                external_id = str(item.get("item_id", ""))
                li = by_external_id.get(external_id)
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=li.line_item_id if li else 0,
                        provider_external_id=external_id,
                        quantity_available=item.get("quantity", 0),
                        warehouse_code=shipment.get("location"),
                        ship_options=[ship_option],
                        flags=(
                            ["backorder"] if shipment.get("type") == "out_of_stock" else []
                        ),
                    )
                )

        return base.ShippingQuoteResult(lines=lines, raw_response=response)

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        quote_raw = purchase_order.quote_raw_response or {}
        quote_id = quote_raw.get("data", {}).get("id")

        try:
            if quote_id:
                shipping_ids = self._extract_shipping_quote_ids(quote_raw)
                response = self._client.promote_quote_to_order(
                    {
                        "environment": self._client.environment,
                        "quote_id": quote_id,
                        "po_number": purchase_order.po_number,
                        "acknowledge_prop_65": False,
                        "acknowledge_epa": False,
                        "acknowledge_carb": False,
                        "shipping": shipping_ids,
                    }
                )
            else:
                shipping_code = purchase_order.ship_method or _DEFAULT_SHIPPING_GROUP_CODE
                response = self._client.create_order(
                    {
                        "environment": self._client.environment,
                        "po_number": purchase_order.po_number,
                        "locations": self._build_locations(line_items, shipping_code=shipping_code),
                        "acknowledge_prop_65": False,
                        "acknowledge_epa": False,
                        "acknowledge_carb": False,
                        "recipient": self._build_recipient(ship_to),
                    }
                )
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

        return self._parse_order_response(response, line_items)

    @staticmethod
    def _extract_shipping_quote_ids(quote_raw: typing.Dict) -> typing.List[typing.Dict]:
        attrs = quote_raw.get("data", {}).get("attributes", {})
        shipping_ids = []
        for shipment in attrs.get("shipment", []):
            shipping_block = shipment.get("shipping", {}) or {}
            quote_id = shipping_block.get("shipping_quote_id")
            if quote_id is not None:
                shipping_ids.append(
                    {
                        "shipping_id": quote_id,
                        "saturday_delivery": False,
                        "signature_required": False,
                    }
                )
        return shipping_ids

    @staticmethod
    def _parse_order_response(
        response: typing.Dict, line_items: typing.List[base.OrderLineItemRequest]
    ) -> base.DistributorOrderResult:
        by_external_id = {li.provider_part.provider_external_id: li for li in line_items}
        order_data = response.get("data", {})
        order_id = str(order_data.get("id", ""))
        attrs = order_data.get("attributes", {})

        placements: typing.List[base.LineItemPlacement] = []
        for shipment in attrs.get("shipment", []):
            for item in shipment.get("items", []):
                external_id = str(item.get("item_id", ""))
                li = by_external_id.get(external_id)
                placements.append(
                    base.LineItemPlacement(
                        line_item_id=li.line_item_id if li else 0,
                        distributor_order_number=order_id,
                        quantity_confirmed=item.get("quantity", 0),
                        warehouse_code=shipment.get("location"),
                        status_message=shipment.get("type"),
                    )
                )

        return base.DistributorOrderResult(
            distributor_order_numbers=[order_id] if order_id else [],
            line_item_placements=placements,
            raw_response=response,
        )

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        try:
            response = self._client.get_orders_by_po_number(purchase_order.po_number)
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

        raw_orders = response.get("data", [])
        if isinstance(raw_orders, dict):
            raw_orders = [raw_orders]

        orders = []
        for order in raw_orders:
            attrs = order.get("attributes", {}) or {}
            orders.append(
                base.DistributorOrderStatus(
                    distributor_order_number=str(order.get("id", "")),
                    status_code=str(attrs.get("status", "")),
                    tracking_numbers=attrs.get("tracking_numbers", []) or [],
                    carrier=attrs.get("carrier"),
                    raw_response=order,
                )
            )
        return base.OrderStatusResult(orders=orders)

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "Turn 14's Electronic Order API does not expose a cancel-order endpoint."
        )

    def supports_cancel(self) -> bool:
        return False

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        try:
            response = self._client.get_shipping_options()
        except turn14_client_exceptions.Turn14APIBadResponseCodeError as e:
            self._handle_error(e)

        methods = []
        for row in response.get("data", []):
            attrs = row.get("attributes", {}) or {}
            methods.append(
                base.ShippingMethod(
                    code=str(row.get("id", "")),
                    name=attrs.get("transportation_name", ""),
                    carrier_name=attrs.get("carrier_name"),
                )
            )
        return methods
