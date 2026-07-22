"""
WheelProsOrderAdapter — DistributorOrderAdapter implementation for Wheel Pros' Orders API
(REST/JSON, https://developer.wheelpros.com). Wraps
``src.integrations.clients.wheelpros.order_client.WheelProsOrderApiClient``.

Confirmed, real API limitations that shape this adapter (from Wheel Pros' own OpenAPI specs):

1. There is NO shipping-rate/freight-quote endpoint at all — same limitation as Premier. The
   only availability signal is the Inventory Search API (POST /inventory/v1/search), which
   Wheel Pros' own docs mark "Internal Use Only"; some dealer accounts may not be granted access
   to it even though their Orders API access works fine. get_shipping_quote() reports
   availability per warehouse (from Inventory Search) but every ShippingQuoteLine's ship_options
   stays empty, never priced — there is no live rate to attach. If the account lacks Inventory
   API access, get_shipping_quote() raises rather than silently reporting wrong availability.
2. Order creation (POST /orders/v1/create?orderType=edi) requires a specific numeric
   ``warehouseCode`` per item (or one header-level default) — Wheel Pros does not offer a
   Turn14-style "let the distributor auto-pick" option. This adapter reads
   ``PurchaseOrderLineItem.warehouse_code`` (set from the line's last quote/selection) at submit
   time; submit_order() raises if a line has no warehouse selected yet.
3. The order-create response only confirms overall success + a single ``supplierOrderNumber`` —
   no per-line confirmation detail. Every submitted line is treated as confirmed at the
   submitted quantity, same fallback Premier's adapter uses for the identical limitation.
4. GET /orders/v1/track's "salesOrders"/"trackings" response schema is not expanded in Wheel
   Pros' public docs (collapsed to bare ``[{}]`` in their own OpenAPI example) — the field names
   read in get_order_status() are inferred from REST conventions used elsewhere in this same
   API, not confirmed against a live response. Verify against Wheel Pros' test environment
   before relying on this in production.
5. There is no pre-shipment cancel endpoint — only a post-fulfillment return/RMA flow
   (POST /orders/v1/return), which is a distinct capability this adapter doesn't implement (see
   cancel_order()).

SAFETY: submit_order() places a REAL order against Wheel Pros. It must only ever be invoked from
an explicit, user-approved submission — never from exploratory/dev code, automated tests, or
ad-hoc scripts. See ``src/integrations/orders/turn_14.py`` for the reference adapter this mirrors.
"""
import logging
import typing

from django.conf import settings

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.wheelpros import exceptions as wheelpros_client_exceptions
from src.integrations.clients.wheelpros.order_client import WheelProsOrderApiClient
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[WHEELPROS-ORDER-ADAPTER]"

# Verbatim from the ship method examples in Wheel Pros' Orders API docs — no live "list methods"
# endpoint exists, same static-reference-table pattern as Keystone/Premier.
_SHIPPING_METHODS = [
    base.ShippingMethod(code="FG", name="FedEx Ground", carrier_name="FedEx"),
    base.ShippingMethod(code="FE", name="FedEx (Economy) LTL", carrier_name="FedEx"),
    base.ShippingMethod(code="F2", name="FedEx 2nd Day Air", carrier_name="FedEx"),
    base.ShippingMethod(code="F1", name="FedEx NDA 1st Overnight", carrier_name="FedEx"),
    base.ShippingMethod(code="FR", name="FedEx Ground Residential", carrier_name="FedEx"),
    base.ShippingMethod(code="PU", name="Purolator (Canada)", carrier_name="Purolator"),
]


class WheelProsOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.WHEELPROS.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider)
        environment = getattr(settings, "WHEELPROS_ORDER_ENVIRONMENT", "production")
        self._client = WheelProsOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider),
            environment=environment,
        )

    # -- Request building -----------------------------------------------------------------

    def _handle_error(self, e: Exception) -> None:
        code = getattr(e, "code", None)
        raise order_exceptions.OrderValidationError(message=str(e), code=str(code) if code else None)

    def _load_warehouse_codes(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
    ) -> typing.Dict[str, str]:
        """{provider_external_id: warehouse_code}, from the last quote's selection stored on
        each PurchaseOrderLineItem — see module docstring, point 2."""
        part_ids = [li.provider_part.id for li in line_items]
        rows = purchase_order.line_items.filter(provider_part_id__in=part_ids).values(
            "provider_part__provider_external_id", "warehouse_code"
        )
        return {
            row["provider_part__provider_external_id"]: row["warehouse_code"]
            for row in rows
            if row["warehouse_code"]
        }

    # -- DistributorOrderAdapter ------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        skus = [li.provider_part.provider_external_id for li in line_items]
        try:
            response = self._client.search_inventory(
                skus=skus, country_codes=[(ship_to.country or "US").upper()]
            )
        except wheelpros_client_exceptions.WheelProsOrderPermissionError as e:
            raise order_exceptions.OrderValidationError(
                "Wheel Pros denied access to the Inventory API needed for a shipping quote — "
                "this account may not be approved for it yet. Contact Wheel Pros support to "
                "request Inventory API access. ({})".format(e)
            )
        except wheelpros_client_exceptions.WheelProsException as e:
            self._handle_error(e)

        logger.info("{} Inventory response: {}".format(_LOG_PREFIX, repr(response)[:4000]))

        try:
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}
            lines: typing.List[base.ShippingQuoteLine] = []
            seen: typing.Set[str] = set()

            for entry in response.get("skus", []):
                external_id = entry.get("sku", "")
                li = by_external_id.get(external_id)
                seen.add(external_id)
                warehouses = entry.get("warehouses") or []
                if not warehouses:
                    lines.append(
                        base.ShippingQuoteLine(
                            line_item_id=li.line_item_id if li else 0,
                            provider_external_id=external_id,
                            quantity_available=0,
                            flags=["not_returned_in_quote"],
                        )
                    )
                    continue
                for warehouse in warehouses:
                    atp = warehouse.get("atp", 0) or 0
                    requested = li.quantity if li else 0
                    # No shipping-rate endpoint exists — ship_options is always empty (see
                    # module docstring, point 1).
                    lines.append(
                        base.ShippingQuoteLine(
                            line_item_id=li.line_item_id if li else 0,
                            provider_external_id=external_id,
                            quantity_available=min(atp, requested) if li else atp,
                            quantity_backordered=warehouse.get("backOrderQty", 0) or 0,
                            warehouse_code=warehouse.get("warehouseId"),
                            flags=[] if atp > 0 else ["backorder"],
                        )
                    )

            for external_id, li in by_external_id.items():
                if external_id in seen:
                    continue
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=li.line_item_id,
                        provider_external_id=external_id,
                        quantity_available=0,
                        flags=["not_returned_in_quote"],
                    )
                )

            if not lines:
                raise order_exceptions.OrderValidationError(
                    "Unexpected/empty inventory response shape from Wheel Pros. Raw response: {}".format(
                        repr(response)[:2000]
                    )
                )
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected inventory response shape from Wheel Pros ({}: {}). Raw response: {}".format(
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
        if not purchase_order.ship_method:
            raise order_exceptions.OrderValidationError(
                "Wheel Pros requires a ship method to be selected before an order can be submitted."
            )

        warehouse_codes = self._load_warehouse_codes(purchase_order, line_items)

        items_payload = []
        for li in line_items:
            external_id = li.provider_part.provider_external_id
            warehouse_code = warehouse_codes.get(external_id)
            if not warehouse_code:
                raise order_exceptions.OrderValidationError(
                    "No warehouse selected for item {} — run a shipping quote first.".format(external_id)
                )
            items_payload.append(
                {
                    "itemprice": "0",
                    "partNumber": external_id,
                    "quantity": li.quantity,
                    "warehouseCode": int(warehouse_code) if str(warehouse_code).isdigit() else warehouse_code,
                }
            )

        data = {
            "purchaseOrderNumber": purchase_order.po_number,
            "purchaseOrderMethod": "EDI",
            "allowPartialDelivery": True,
            "items": items_payload,
            "shipping": {
                "method": purchase_order.ship_method,
                "shipToName": ship_to.name,
                "address1": ship_to.address1,
                "address2": ship_to.address2 or "",
                "city": ship_to.city,
                "stateOrProvinceCode": ship_to.state,
                "postalCode": ship_to.postal_code,
                "phone": ship_to.phone or "",
                "email": ship_to.email or "",
                "countryCode": (ship_to.country or "US").upper(),
            },
        }

        try:
            response = self._client.create_sales_order_edi(data)
        except wheelpros_client_exceptions.WheelProsException as e:
            self._handle_error(e)

        return self._parse_submit_response(response, line_items)

    @staticmethod
    def _parse_submit_response(
        response: typing.Dict, line_items: typing.List[base.OrderLineItemRequest]
    ) -> base.DistributorOrderResult:
        try:
            order_number = response.get("supplierOrderNumber", "")
            if not order_number:
                raise order_exceptions.OrderValidationError(
                    "Unexpected/empty order response shape from Wheel Pros. Raw response: {}".format(
                        repr(response)[:2000]
                    )
                )
            # Wheel Pros' create response only confirms overall success — no per-line
            # confirmation detail is returned (see module docstring, point 3). Every submitted
            # line is treated as confirmed at the submitted quantity, same fallback Premier's
            # adapter uses for the identical limitation.
            placements = [
                base.LineItemPlacement(
                    line_item_id=li.line_item_id,
                    distributor_order_number=order_number,
                    quantity_confirmed=li.quantity,
                )
                for li in line_items
            ]
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order response shape from Wheel Pros ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                )
            )

        return base.DistributorOrderResult(
            distributor_order_numbers=[order_number],
            line_item_placements=placements,
            raw_response=response,
        )

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        distributor_orders = list(purchase_order.distributor_orders.all())
        sales_order_number = distributor_orders[0].distributor_order_number if distributor_orders else None

        try:
            if sales_order_number:
                response = self._client.get_order_tracking(salesOrderNumber=sales_order_number)
            else:
                response = self._client.get_order_tracking(poNumber=purchase_order.po_number)
        except wheelpros_client_exceptions.WheelProsException as e:
            self._handle_error(e)

        # See module docstring, point 4 — "salesOrders"/"trackings" field names below are
        # inferred, not confirmed against a live response. Any parse failure falls back to a
        # single "OPEN, no tracking yet" entry rather than raising, since that's the far more
        # common case for a routine status-poll on a fresh order (same defensive default
        # Premier's adapter uses for its own unconfirmed tracking schema).
        fallback_order_number = sales_order_number or purchase_order.po_number
        try:
            sales_orders = response.get("salesOrders") or []
            trackings = response.get("trackings") or []
            if not sales_orders:
                return base.OrderStatusResult(
                    orders=[
                        base.DistributorOrderStatus(
                            distributor_order_number=fallback_order_number,
                            status_code="OPEN",
                            tracking_numbers=[t.get("trackingNumber") for t in trackings if t.get("trackingNumber")],
                        )
                    ]
                )

            orders: typing.List[base.DistributorOrderStatus] = []
            for so in sales_orders:
                so_number = str(so.get("salesOrderNumber") or so.get("orderNumber") or fallback_order_number)
                related = [
                    t
                    for t in trackings
                    if not t.get("salesOrderNumber") or str(t.get("salesOrderNumber")) == so_number
                ]
                orders.append(
                    base.DistributorOrderStatus(
                        distributor_order_number=so_number,
                        status_code=str(so.get("status") or so.get("orderStatus") or "OPEN"),
                        tracking_numbers=[t.get("trackingNumber") for t in related if t.get("trackingNumber")],
                        carrier=next((t.get("carrier") for t in related if t.get("carrier")), None),
                        raw_response=so,
                    )
                )
            return base.OrderStatusResult(orders=orders)
        except (AttributeError, TypeError, KeyError, IndexError):
            logger.warning(
                "{} Unexpected tracking response shape from Wheel Pros, falling back to OPEN. "
                "Raw response: {}".format(_LOG_PREFIX, repr(response)[:2000])
            )
            return base.OrderStatusResult(
                orders=[
                    base.DistributorOrderStatus(
                        distributor_order_number=fallback_order_number,
                        status_code="OPEN",
                        tracking_numbers=[],
                    )
                ]
            )

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "Wheel Pros' Orders API does not expose a pre-shipment cancel endpoint — only "
            "post-fulfillment returns (RMA) via a separate flow, which this adapter doesn't handle."
        )

    def supports_cancel(self) -> bool:
        return False

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        return list(_SHIPPING_METHODS)
