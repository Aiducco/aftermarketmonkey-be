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
from src.integrations import credentials as credentials_helper
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


def _normalize_warehouse_code(location: typing.Any) -> typing.Optional[str]:
    """Turn14's ``location`` comes back as a JSON string for some warehouses ("02") but a bare
    number for others (59, confirmed against a live quote) — normalize to str so warehouse_code
    is consistently typed everywhere it's compared/keyed on (our own JSON responses, the FE)."""
    return str(location) if location is not None else None


def _load_warehouse_names() -> typing.Dict[str, str]:
    """
    {external_id: "City, ST"} from Turn14Location (populated by fetch_turn14_locations from
    GET /v1/locations), so quote responses can show a real place instead of a bare code — this
    is exactly what Turn14's own ordering portal does (it groups shipping options by state, e.g.
    "PA In Stock Items" for warehouse "01"). Same zero-pad tolerance as
    master_parts._get_turn14_location_map, since a quote's ``location`` can come back
    unpadded (e.g. int 59) after _normalize_warehouse_code.
    """
    names = {}
    for row in src_models.Turn14Location.objects.all().values("external_id", "name", "state"):
        external_id = (row.get("external_id") or "").strip()
        if not external_id:
            continue
        label = ", ".join(part for part in (row.get("name"), row.get("state")) if part)
        names[external_id] = label
        if external_id.isdigit():
            names[external_id.zfill(2)] = label
            names[str(int(external_id))] = label
    return names


class Turn14OrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.TURN_14.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider)
        environment = getattr(settings, "TURN14_ORDER_ENVIRONMENT", "testing")
        self._client = Turn14OrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider),
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

        logger.info("{} Quote response: {}".format(_LOG_PREFIX, repr(response)[:4000]))

        try:
            attrs = response.get("data", {}).get("attributes", {})
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}

            # Per-item pricing promos (e.g. "6% Added Overstock Item Discount") are a top-level
            # array keyed by item_id, not nested under each shipment — collect them once and
            # attach the same list to every shipment line for that item below.
            promotions_by_item_id: typing.Dict[str, typing.List[base.LinePromotion]] = {}
            for promo in attrs.get("promos", []):
                item_id = str(promo.get("item_id", ""))
                promotions_by_item_id.setdefault(item_id, []).append(
                    base.LinePromotion(
                        description=promo.get("promo_description") or "",
                        amount=(
                            decimal.Decimal(str(promo["promo_amount"]))
                            if promo.get("promo_amount") is not None
                            else None
                        ),
                    )
                )

            warehouse_names = _load_warehouse_names()

            lines: typing.List[base.ShippingQuoteLine] = []
            for shipment in attrs.get("shipment", []):
                # Contrary to the docs' single-object example, an unfiltered quote actually
                # returns a LIST of priced shipping options per shipment here (confirmed
                # against a live response) — this is what lets the FE offer a real picker
                # with live prices from one quote call, no re-quoting needed.
                ship_options = [
                    base.ShipOption(
                        service_level_code=str(opt.get("shipping_code", "")),
                        service_level_name=opt.get("verbose_eta") or "",
                        cost=(decimal.Decimal(str(opt["cost"])) if opt.get("cost") is not None else None),
                    )
                    for opt in (shipment.get("shipping") or [])
                ]
                is_out_of_stock = shipment.get("type") == "out_of_stock"
                for item in shipment.get("items", []):
                    external_id = str(item.get("item_id", ""))
                    li = by_external_id.get(external_id)
                    quantity = item.get("quantity", 0)
                    warehouse_code = _normalize_warehouse_code(shipment.get("location"))
                    lines.append(
                        base.ShippingQuoteLine(
                            line_item_id=li.line_item_id if li else 0,
                            provider_external_id=external_id,
                            quantity_available=0 if is_out_of_stock else quantity,
                            quantity_backordered=quantity if is_out_of_stock else 0,
                            warehouse_code=warehouse_code,
                            warehouse_name=warehouse_names.get(warehouse_code) if warehouse_code else None,
                            ship_options=ship_options,
                            flags=(["backorder"] if is_out_of_stock else []),
                            promotions=promotions_by_item_id.get(external_id, []),
                        )
                    )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            # Response came back 200 (no Turn14APIBadResponseCodeError), but didn't match the
            # shape we expected to parse — surface the actual payload instead of a bare
            # AttributeError, so the real shape is visible in PurchaseOrder.error_message
            # without needing server-log access to diagnose it.
            raise order_exceptions.OrderValidationError(
                "Unexpected quote response shape from Turn14 ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
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
                shipping_ids = self._extract_shipping_quote_ids(quote_raw, ship_method=purchase_order.ship_method)
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
    def _extract_shipping_quote_ids(
        quote_raw: typing.Dict, ship_method: typing.Optional[str] = None
    ) -> typing.List[typing.Dict]:
        """
        Each shipment in the quote carries a LIST of priced options (see get_shipping_quote) —
        exactly one must be picked per shipment to promote to an order. Picks the option whose
        shipping_code matches ``ship_method`` (the user's choice from
        GET .../shipping-methods/) when given; otherwise defaults to the cheapest, matching
        Turn 14's own recommendation ("we recommend you not [pre-select] ... to ensure you're
        always selecting the best possible option").
        """
        attrs = quote_raw.get("data", {}).get("attributes", {})
        shipping_ids = []
        for shipment in attrs.get("shipment", []):
            options = shipment.get("shipping") or []
            if not options:
                continue
            chosen = None
            if ship_method is not None:
                chosen = next(
                    (o for o in options if str(o.get("shipping_code")) == str(ship_method)), None
                )
            if chosen is None:
                chosen = min(options, key=lambda o: o.get("cost", float("inf")))
            quote_id = chosen.get("shipping_quote_id")
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
        try:
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
                            warehouse_code=_normalize_warehouse_code(shipment.get("location")),
                            status_message=shipment.get("type"),
                        )
                    )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order response shape from Turn14 ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
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

        try:
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
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order-status response shape from Turn14 ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
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
