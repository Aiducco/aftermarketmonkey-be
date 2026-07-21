"""
PremierOrderAdapter — DistributorOrderAdapter implementation for Premier (APG Wholesale)'s v5
REST Order API. Wraps ``src.integrations.clients.premier.order_client.PremierOrderApiClient``.

Premier's public documentation (https://developer.premierwd.com/) is noticeably thinner than
Turn14/Keystone/Meyer's — confirmed via multiple targeted lookups, not just a scraping gap (the
docs page's own "View on GitHub" source link 404s, and there is no OpenAPI spec). Two real,
confirmed limitations shape this adapter:

1. There is NO shipping/freight-quote endpoint at all. Premier's docs state orders are
   committed immediately on POST with no dry-run/preview mode, so get_shipping_quote() can only
   report availability (via GET /inventory) — every ShippingQuoteLine's ship_options is empty,
   never priced. This is a hard API limitation, not something this adapter is missing.
2. The order-creation response does NOT include Premier's own order number — it only echoes
   back what was submitted. Since GET /sales-orders/{salesOrderNumber} needs exactly that
   number, it's unusable right after submission. This adapter uses ``purchase_order.po_number``
   (Premier's ``customerPurchaseOrderNumber``) as the distributor_order_number throughout, and
   polls status via GET /tracking?purchaseOrderNumber=... instead — the only documented lookup
   keyed by something we actually have. This is inferred, not documented; verify against
   Premier's test environment before relying on it in production.

SAFETY: submit_order() places a REAL order against Premier — their docs describe no dry-run
mode at all, even for a "testing" environment. Must only ever be invoked from an explicit,
user-approved submission. There is no cancel endpoint — see cancel_order() below.
"""
import logging
import typing

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.premier import exceptions as premier_client_exceptions
from src.integrations.clients.premier.order_client import PremierOrderApiClient
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[PREMIER-ORDER-ADAPTER]"

# Verbatim from Premier's docs (https://developer.premierwd.com/#sales-orders) — a mix of named
# and numeric carrier service codes, no carrier/name breakdown given for the numeric ones.
_SHIP_METHOD_CODES = [
    "GROUND", "2ND DAY", "2ND DAY AM", "2ND DAY LETTER", "3 DAY SELECT", "FEDEX 2ND DAY",
    "FEDEX 3 DAY", "FEDEX GROUND", "FEDEX INT 1 DAY", "FEDEX INT 3 DAY", "FEDEX PRIORITY",
    "FEDEX STD OVRNT", "NEXT DAY", "NEXT DAY AM", "NEXT DAY LETTER", "NEXT DAY SAT",
    "NEXT DAY SAVER", "ONTRAC", "P-UP", "Priority Mail", "Express Mail", "EXPRESS INT",
    "PRIORITY INT", "EXPEDITED INT", "STANDARD INT", "EXPRESS INT PLS", "EXPRESS INT SVR",
    "FIRST OVERNIGHT", "EXPRESS SAVER", "HOME DELIVERY", "PARCEL POST", "Global Express",
    "Exp Mail Inter.", "1st Class Int.", "1st Class",
    "20101", "20501", "20102", "20103", "20104", "20105", "20106", "20107", "20108", "20109",
    "20201", "20202", "20203", "20204", "20205", "20401", "20402", "20403", "20404", "20405",
    "20406", "20407", "20408", "20801", "20802", "20803", "20804", "20805", "20806", "20206",
    "FEDEX 2 DAY AM", "20207", "20208", "20209", "20210", "FEDEX INT ECON", "FEDEX INT FIRST",
    "20211", "20212", "ONTRAC SUNRISE", "ONTRAC GOLD",
]


class PremierOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider)
        self._client = PremierOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider)
        )

    def _handle_error(self, e: Exception) -> None:
        code = getattr(e, "code", None)
        raise order_exceptions.OrderValidationError(message=str(e), code=str(code) if code else None)

    # -- DistributorOrderAdapter ------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        item_numbers = [li.provider_part.provider_external_id for li in line_items]
        try:
            inventory = self._client.get_inventory(item_numbers)
        except premier_client_exceptions.PremierException as e:
            self._handle_error(e)

        logger.info("{} Inventory response: {}".format(_LOG_PREFIX, repr(inventory)[:4000]))

        try:
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}
            lines: typing.List[base.ShippingQuoteLine] = []
            seen: typing.Set[str] = set()

            for entry in inventory:
                external_id = entry.get("itemNumber", "")
                li = by_external_id.get(external_id)
                seen.add(external_id)
                total_available = sum(
                    w.get("quantityAvailable", 0) or 0 for w in (entry.get("inventory") or [])
                )
                requested = li.quantity if li else 0
                # No freight-quote endpoint exists — ship_options is always empty (see module
                # docstring). quantity_available is capped at what was actually requested;
                # any shortfall is reported as backordered.
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=li.line_item_id if li else 0,
                        provider_external_id=external_id,
                        quantity_available=min(total_available, requested),
                        quantity_backordered=max(requested - total_available, 0),
                        flags=[] if total_available >= requested else ["backorder"],
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
                    "Unexpected/empty inventory response shape from Premier. Raw response: {}".format(
                        repr(inventory)[:2000]
                    )
                )
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected inventory response shape from Premier ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(inventory)[:2000]
                )
            )

        return base.ShippingQuoteResult(lines=lines, raw_response={"inventory": inventory})

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        phone_digits = "".join(ch for ch in (ship_to.phone or "") if ch.isdigit())
        data = {
            "customerPurchaseOrderNumber": purchase_order.po_number,
            "shipToAddress": {
                "name": ship_to.name,
                "addressLine1": ship_to.address1,
                "addressLine2": ship_to.address2 or "",
                "city": ship_to.city,
                "regionCode": (ship_to.state or "")[:2].upper(),
                "postalCode": ship_to.postal_code,
                "countryCode": (ship_to.country or "US")[:2].upper(),
                "phone": phone_digits,
            },
            "salesOrderLines": [
                {"itemNumber": li.provider_part.provider_external_id, "quantity": li.quantity}
                for li in line_items
            ],
        }
        if purchase_order.ship_method:
            data["shipMethod"] = purchase_order.ship_method

        try:
            response = self._client.create_sales_order(data)
        except premier_client_exceptions.PremierException as e:
            self._handle_error(e)

        return self._parse_submit_response(response, purchase_order, line_items)

    @staticmethod
    def _parse_submit_response(
        response: typing.Dict,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
    ) -> base.DistributorOrderResult:
        try:
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}
            placements: typing.List[base.LineItemPlacement] = []
            # Premier's response only echoes back what was submitted (no confirmed-vs-requested
            # signal, no per-line pricing) — see module docstring. Fall back to our own request
            # data if the response is missing/malformed rather than failing a placed order.
            response_lines = response.get("salesOrderLines") or [
                {"itemNumber": li.provider_part.provider_external_id, "quantity": li.quantity}
                for li in line_items
            ]
            for line in response_lines:
                external_id = line.get("itemNumber", "")
                li = by_external_id.get(external_id)
                placements.append(
                    base.LineItemPlacement(
                        line_item_id=li.line_item_id if li else 0,
                        distributor_order_number=purchase_order.po_number,
                        quantity_confirmed=line.get("quantity", li.quantity if li else 0),
                        warehouse_code=line.get("warehouseCode"),
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order response shape from Premier ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                )
            )

        # No distributor order number is ever returned — see module docstring.
        return base.DistributorOrderResult(
            distributor_order_numbers=[purchase_order.po_number],
            line_item_placements=placements,
            raw_response=response,
        )

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        try:
            tracking_entries = self._client.get_tracking_by_purchase_order_number(purchase_order.po_number)
        except premier_client_exceptions.PremierOrderValidationError:
            # Undocumented whether "no tracking yet" (order placed but not shipped) returns an
            # empty result or an error — treated as "still open" rather than a hard failure,
            # since that's the far more common case for a routine status-poll on a fresh order.
            # Revisit once Premier's actual behavior here is confirmed.
            return base.OrderStatusResult(
                orders=[
                    base.DistributorOrderStatus(
                        distributor_order_number=purchase_order.po_number,
                        status_code="OPEN",
                        tracking_numbers=[],
                    )
                ]
            )
        except premier_client_exceptions.PremierException as e:
            self._handle_error(e)

        try:
            orders: typing.List[base.DistributorOrderStatus] = []
            for entry in tracking_entries:
                tracking_number = entry.get("trackingNumber")
                orders.append(
                    base.DistributorOrderStatus(
                        distributor_order_number=purchase_order.po_number,
                        status_code="SHIPPED" if tracking_number else "OPEN",
                        tracking_numbers=[tracking_number] if tracking_number else [],
                        carrier=entry.get("carrier"),
                        raw_response=entry,
                    )
                )
            if not orders:
                orders = [
                    base.DistributorOrderStatus(
                        distributor_order_number=purchase_order.po_number, status_code="OPEN", tracking_numbers=[]
                    )
                ]
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected tracking response shape from Premier ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(tracking_entries)[:2000]
                )
            )
        return base.OrderStatusResult(orders=orders)

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "Premier's Order API does not expose a cancel-order endpoint."
        )

    def supports_cancel(self) -> bool:
        return False

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        # Static reference list — Premier's docs give no live "list methods" endpoint, just this
        # fixed catalog of valid shipMethod codes for the Sales Orders request.
        return [base.ShippingMethod(code=code, name=code) for code in _SHIP_METHOD_CODES]
