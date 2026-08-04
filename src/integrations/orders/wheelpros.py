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

   UNRESOLVED, confirmed against the live CreateOrderEdiRequest schema (developer.wheelpros.com
   -> Ordering -> Specifications; the ReDoc page is JS-rendered, its embedded __redoc_state
   has the real schema): the request's own top-level ``required`` list includes
   ``addressCode``, even though the only place that field is documented at all is nested under
   ``shipping`` (there marked "required if shipping address information is not available in the
   request" — which it always is here, see _client's shipping payload). No top-level
   ``addressCode`` property even exists in the schema, so this may just be a spec-generation
   artifact; but GET /v1/track's own ``addressCode`` query param ("must be supplied ... if
   addressCode was supplied in the initial sales order create request") implies this is Wheel
   Pros' equivalent of Meyer's registered AccountAddresses — a customer-specific ship-to code,
   not a freeform address. No "list registered addresses" endpoint exists anywhere in Wheel
   Pros' docs to resolve one. This adapter never sends one. As of this writing no real Wheel
   Pros order has been placed/confirmed through this system — the first live submit_order()
   call is the only way to learn whether Wheel Pros actually enforces this: if it 400s citing
   addressCode, that confirms a real per-account code is needed (open a ticket with Wheel Pros
   support to get one, then thread it through CompanyProviderOrderAccount the same way Meyer's
   AddressCode is resolved); if it succeeds, this was a harmless spec artifact.
3. The order-create response only confirms overall success + a single ``supplierOrderNumber`` —
   no per-line confirmation detail. Every submitted line is treated as confirmed at the
   submitted quantity, same fallback Premier's adapter uses for the identical limitation.
4. GET /orders/v1/track's "salesOrders"/"trackings" response schema IS fully specified in Wheel
   Pros' live OrderTrackingResponseSchema (confirmed the same way as point 2 above — a prior
   pass here believed it was collapsed/unexpanded, but that was this same JS-rendering issue,
   not a real documentation gap). Two real corrections from that confirmed schema, both fixed
   in get_order_status(): (a) the per-order id field is ``salesOrder``, not
   ``salesOrderNumber``/``orderNumber`` (neither of which exist); (b) ``salesOrders[]`` carries
   NO order-level status/orderStatus field at all — the only completion signal is each order's
   own single ``trackingNumber`` cross-referenced into ``trackings[].trackingInfo[].statusCode``
   (ZN/ZP/ZR/ZU/ZC/ZX/DL, spelled out in the schema's own field description). ``trackings[]``
   itself carries no per-order linking field (no salesOrderNumber, no carrier) — matching a
   tracking entry to a specific sales order is only possible via that order's own singular
   ``trackingNumber``, not by filtering the whole trackings array.
5. There is no pre-shipment cancel endpoint — only a post-fulfillment return/RMA flow
   (POST /orders/v1/return), which is a distinct capability this adapter doesn't implement (see
   cancel_order()).
6. There is no invoice API anywhere in Wheel Pros' docs (confirmed against both the Core API
   and as much of the Legacy Postman collection as was reachable) — supports_invoices() stays
   at its inherited False default, deliberately, not by omission.
7. CORRECTED (previously not handled at all): ``ProviderPart.provider_external_id`` is NOT
   Wheel Pros' real SKU/part number for this distributor — it's a composite
   ``"{wp_brand_id}_{part_number}"`` key (see master_parts._wheelpros_provider_external_id),
   needed only for our own DB uniqueness since the same part_number can recur across different
   Wheel Pros brands. A previous version of get_shipping_quote()/submit_order() sent that
   composite key straight through as ``sku``/``partNumber`` to Wheel Pros' real Inventory
   Search/Orders API, which would never match a real part — every quote would report every line
   as unavailable, and every real submission would likely be rejected or silently drop every
   line. Fixed by ``_wheelpros_item_number`` below — the exact same shape as
   ``orders.premier._premier_item_number`` (product_details' "sku" entry carries the real
   part_number; see master_parts._wheelpros_product_details), used everywhere this adapter
   talks to Wheel Pros' real API.

SAFETY: submit_order() places a REAL order against Wheel Pros. It must only ever be invoked from
an explicit, user-approved submission — never from exploratory/dev code, automated tests, or
ad-hoc scripts. See ``src/integrations/orders/turn_14.py`` for the reference adapter this mirrors.
"""
import datetime
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

# GET /orders/v1/track's trackingInfo[].statusCode vocabulary, confirmed directly from Wheel
# Pros' live OpenAPI spec (see module docstring, point 4) — mapped to DistributorOrderStatus.
# delivery_status's normalized vocabulary. Left unmapped (None) for the earlier pre-shipment
# stages (ZN/ZP/ZR/ZU) and for DB ("drilled from blank wheel" — a wheel-manufacturing status,
# not a shipping one); any other code passes through directly from the carrier per the spec and
# isn't one of these constants.
_STATUS_CODE_DELIVERY_STATUS = {
    # "Complete - ... fully invoiced, but there is no tracking information from the carrier -
    # Local carriers" per Wheel Pros' own field description — this is terminal/done, same as DL,
    # just without carrier-level tracking detail. Previously mapped to "in_transit", which
    # reported a finished, fully-invoiced local-carrier delivery as still moving.
    "ZC": "delivered",
    "DL": "delivered",  # tracking # exists, fully invoiced, delivery finalized
    "ZX": "cancelled",  # order cancelled
}


def _parse_wheelpros_event_date(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    """trackingInfo[].statusDate's exact format isn't shown in Wheel Pros' spec (only the field
    name/type) — try ISO 8601 first (the spec's own convention elsewhere), then MM/DD/YYYY,
    else give up rather than guess further; same defensive pattern as Premier's date parsing."""
    if not value or not value.strip():
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(value[:10], fmt).date()
        except ValueError:
            continue
    return None


def _summarize_wheelpros_tracking_events(
    tracking_info: typing.List[typing.Dict],
) -> typing.Tuple[typing.Optional[str], typing.Optional[str], typing.Optional[datetime.date]]:
    """Collapses one tracking number's trackingInfo[] events (only populated when
    realtimeShipmentStatus=True was requested) into (raw_status_code, delivery_status,
    latest_event_date) — the most recent event by statusDate/statusTime wins. raw_status_code
    (Wheel Pros' own ZN/ZP/ZR/ZU/ZC/ZX/DL/etc vocabulary) is returned alongside the normalized
    delivery_status so get_order_status() can carry the real distributor status code in
    DistributorOrderStatus.status_code, the same "raw code, not just the normalized bucket"
    convention Keystone/Premier's own status_code already follows — salesOrders[] itself has no
    status field of its own to use instead (see module docstring, point 4)."""
    if not tracking_info:
        return None, None, None
    latest = max(tracking_info, key=lambda e: (e.get("statusDate") or "", e.get("statusTime") or ""))
    status_code = latest.get("statusCode")
    return (
        status_code,
        _STATUS_CODE_DELIVERY_STATUS.get(status_code),
        _parse_wheelpros_event_date(latest.get("statusDate")),
    )


def _wheelpros_item_number(provider_part: src_models.ProviderPart) -> str:
    """Wheel Pros' real part number/SKU — NOT ProviderPart.provider_external_id, which for Wheel
    Pros is a composite ``"{wp_brand_id}_{part_number}"`` key (see master_parts.
    _wheelpros_provider_external_id), needed only for our own DB uniqueness since the same
    part_number can recur across different Wheel Pros brands. Sending that composite key
    straight to Wheel Pros' Inventory Search/Orders API would never match a real SKU (see module
    docstring, point 7). Mirrors orders.premier._premier_item_number exactly: product_details'
    "sku" entry (see master_parts._wheelpros_product_details) carries the raw part number
    directly; fall back to splitting provider_external_id on its first "_" (the brand id prefix
    is always purely numeric) if product_details is ever missing/stale."""
    for entry in provider_part.product_details or []:
        if entry.get("key") == "sku" and entry.get("value"):
            return str(entry["value"]).strip()
    ext_id = provider_part.provider_external_id or ""
    _, _, remainder = ext_id.partition("_")
    return (remainder or ext_id).strip()


def _load_wheelpros_warehouse_names() -> typing.Dict[str, str]:
    """{external_id: "City, ST"} from WheelProsWarehouse (populated by
    fetch_and_save_wheelpros_warehouses from GET /warehouses/v1), so quote responses can show a
    real place instead of a bare warehouseId — same role turn_14.py's _load_warehouse_names /
    meyer.py's _load_meyer_warehouse_names play for their distributors."""
    names = {}
    for row in src_models.WheelProsWarehouse.objects.all().values("external_id", "city", "state"):
        external_id = (row.get("external_id") or "").strip()
        if not external_id:
            continue
        label = ", ".join(part for part in (row.get("city"), row.get("state")) if part)
        if label:
            names[external_id] = label
    return names


class WheelProsOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.WHEELPROS.value

    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
    ) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider, order_account)
        environment = getattr(settings, "WHEELPROS_ORDER_ENVIRONMENT", "production")
        self._client = WheelProsOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider, order_account),
            environment=environment,
        )

    # -- Request building -----------------------------------------------------------------

    def _handle_error(self, e: Exception, request_payload: typing.Optional[typing.Dict] = None) -> None:
        code = getattr(e, "code", None)
        raise order_exceptions.OrderValidationError(
            message=str(e), code=str(code) if code else None, request_payload=request_payload
        )

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
        # Wheel Pros' real SKU, not our own composite provider_external_id -- see module
        # docstring, point 7 / _wheelpros_item_number.
        skus = [_wheelpros_item_number(li.provider_part) for li in line_items]
        data = {"skus": skus, "country_codes": [(ship_to.country or "US").upper()]}
        try:
            response = self._client.search_inventory(
                skus=skus, country_codes=[(ship_to.country or "US").upper()]
            )
        except wheelpros_client_exceptions.WheelProsOrderPermissionError as e:
            raise order_exceptions.OrderValidationError(
                "Wheel Pros denied access to the Inventory API needed for a shipping quote — "
                "this account may not be approved for it yet. Contact Wheel Pros support to "
                "request Inventory API access. ({})".format(e),
                request_payload=data,
            )
        except wheelpros_client_exceptions.WheelProsException as e:
            self._handle_error(e, request_payload=data)

        logger.info("{} Inventory response: {}".format(_LOG_PREFIX, repr(response)[:4000]))

        warehouse_names = _load_wheelpros_warehouse_names()

        try:
            by_external_id = {_wheelpros_item_number(li.provider_part): li for li in line_items}
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
                    warehouse_code = warehouse.get("warehouseId")
                    # No shipping-rate endpoint exists — ship_options is always empty (see
                    # module docstring, point 1).
                    lines.append(
                        base.ShippingQuoteLine(
                            line_item_id=li.line_item_id if li else 0,
                            provider_external_id=external_id,
                            quantity_available=min(atp, requested) if li else atp,
                            quantity_backordered=warehouse.get("backOrderQty", 0) or 0,
                            warehouse_code=warehouse_code,
                            warehouse_name=(
                                warehouse_names.get(str(warehouse_code)) if warehouse_code is not None else None
                            ),
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
                    ),
                    request_payload=data,
                )
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected inventory response shape from Wheel Pros ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                ),
                request_payload=data,
            )

        return base.ShippingQuoteResult(lines=lines, raw_response=response, request_payload=data)

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
            # warehouse_codes is keyed by our own composite provider_external_id (see
            # _load_warehouse_codes, purely an internal DB lookup) -- item_number below is the
            # separate, real Wheel Pros SKU actually sent on the wire (see module docstring,
            # point 7 / _wheelpros_item_number). These must not be conflated: a previous version
            # used provider_external_id for both, which happened to work for the warehouse-code
            # lookup but sent the wrong value as partNumber.
            external_id = li.provider_part.provider_external_id
            item_number = _wheelpros_item_number(li.provider_part)
            warehouse_code = warehouse_codes.get(external_id)
            if not warehouse_code:
                raise order_exceptions.OrderValidationError(
                    "No warehouse selected for item {} — run a shipping quote first.".format(item_number)
                )
            # No itemPrice/itemPriceCurrencyCode here -- confirmed against the live
            # CreateOrderEdiRequest schema, itemPrice is a sell-price *override* gated behind a
            # special API permission, not a required echo of our own price. A previous version
            # sent {"itemprice": "0"} (wrong key casing -- the real field is "itemPrice"), which
            # Wheel Pros' backend simply never matched to anything; omitted entirely now rather
            # than "fixed" to the real key, since sending a real itemPrice override without that
            # permission risks a 403/validation rejection this account may not have granted.
            items_payload.append(
                {
                    "partNumber": item_number,
                    "quantity": li.quantity,
                    "warehouseCode": int(warehouse_code) if str(warehouse_code).isdigit() else warehouse_code,
                }
            )

        data = {
            "purchaseOrderNumber": base.resolve_po_number(purchase_order),
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
            self._handle_error(e, request_payload=data)

        return self._parse_submit_response(response, line_items, request_payload=data)

    @staticmethod
    def _parse_submit_response(
        response: typing.Dict,
        line_items: typing.List[base.OrderLineItemRequest],
        request_payload: typing.Optional[typing.Dict] = None,
    ) -> base.DistributorOrderResult:
        try:
            order_number = response.get("supplierOrderNumber", "")
            if not order_number:
                raise order_exceptions.OrderValidationError(
                    "Unexpected/empty order response shape from Wheel Pros. Raw response: {}".format(
                        repr(response)[:2000]
                    ),
                    request_payload=request_payload,
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
                ),
                request_payload=request_payload,
            )

        return base.DistributorOrderResult(
            distributor_order_numbers=[order_number],
            line_item_placements=placements,
            raw_response=response,
            request_payload=request_payload,
        )

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        distributor_orders = list(purchase_order.distributor_orders.all())
        sales_order_number = distributor_orders[0].distributor_order_number if distributor_orders else None

        try:
            if sales_order_number:
                response = self._client.get_order_tracking(
                    salesOrderNumber=sales_order_number, realtimeShipmentStatus=True
                )
            else:
                response = self._client.get_order_tracking(
                    poNumber=base.resolve_po_number(purchase_order), realtimeShipmentStatus=True
                )
        except wheelpros_client_exceptions.WheelProsException as e:
            self._handle_error(e)

        # See module docstring, point 4 — the shape below is now confirmed directly against Wheel
        # Pros' live OrderTrackingResponseSchema. Any parse failure still falls back to a single
        # "OPEN, no tracking yet" entry rather than raising, since that's the far more common
        # case for a routine status-poll on a fresh order (same defensive default Premier's
        # adapter uses for its own tracking schema).
        fallback_order_number = sales_order_number or base.resolve_po_number(purchase_order)
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

            # {trackingNumber: entry} for the realtimeShipmentStatus=True "trackings" array.
            trackings_by_number = {t.get("trackingNumber"): t for t in trackings if t.get("trackingNumber")}

            orders: typing.List[base.DistributorOrderStatus] = []
            for so in sales_orders:
                # "salesOrder" is the real per-order id field (confirmed against the live schema)
                # -- salesOrders[] has no "salesOrderNumber"/"orderNumber" field at all, so those
                # lookups previously always missed and fell through to fallback_order_number.
                so_number = str(so.get("salesOrder") or fallback_order_number)
                # trackings[] carries no field linking an entry back to a specific sales order
                # (no salesOrderNumber, confirmed against the live schema) -- each order's own
                # singular trackingNumber is the only reliable join key, not a filter over the
                # whole trackings array (which would wrongly attribute every tracking number to
                # every sales order whenever a response ever contains more than one).
                tracking_number = so.get("trackingNumber")
                tracking_numbers = {tracking_number} if tracking_number else set()

                # Wheel Pros' salesOrders[] carries no order-level status field of its own
                # (confirmed against the live schema) -- the tracking entry's own statusCode
                # (ZN/ZP/ZR/ZU/ZC/ZX/DL) is the only status signal available, only populated when
                # realtimeShipmentStatus=True was requested above.
                status_code, delivery_status, ship_date = None, None, None
                if tracking_number:
                    entry = trackings_by_number.get(tracking_number)
                    if entry:
                        status_code, delivery_status, ship_date = _summarize_wheelpros_tracking_events(
                            entry.get("trackingInfo") or []
                        )

                orders.append(
                    base.DistributorOrderStatus(
                        distributor_order_number=so_number,
                        status_code=status_code or "OPEN",
                        tracking_numbers=sorted(tracking_numbers),
                        carrier=so.get("carrierName") or so.get("carrier"),
                        delivery_status=delivery_status,
                        ship_date=ship_date,
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
