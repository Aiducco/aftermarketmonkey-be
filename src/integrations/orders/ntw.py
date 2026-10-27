"""
NtwOrderAdapter -- ordering through TBC Corporation's v1 order API for NTW.

Two structural facts about this API shape the whole adapter.

**There is no quote endpoint.** TBC exposes ``POST /v1/order`` and ``GET /v1/order/{orderId}``
and nothing else, so ``get_shipping_quote`` is synthesised from the v2 *inventory* call:
``availableSource`` gives per-warehouse availability, ``productCost`` the line price,
``freightPerTire`` the freight, ``estimatedDelivery``/``estimatedDeliveryDate`` the service
level. That is also exactly the pre-submit availability check we told NTW we perform, and it
inherits the existing 15-minute quote TTL (``PURCHASE_ORDER_QUOTE_TTL_MINUTES``) with submit
hard-blocked against a stale quote.

**``locationId`` is required per line at submit**, and it only exists in the inventory response.
So the quote is not advisory here the way it is for distributors that will pick a warehouse
themselves -- it is where the warehouse decision is made, and a submit without a fresh quote has
no legal value to send. ``submit_order`` reads it back from
``PurchaseOrder.quote_raw_response``, which is where the runner stores our quote verbatim.

Not supported, because TBC exposes no endpoint for it: cancel, an invoice feed, and a shipping
method catalogue. The quote's ship options come from whatever the inventory response said rather
than from a list we can enumerate.

**Blocked on TBC.** Both order endpoints take an HMAC SHA-256 ``signature`` whose
canonicalisation TBC has not documented (see ``api_client.NtwApiClient._signature``). Until they
answer, a submit raises ``NtwApiNotConfiguredError`` rather than sending something unsigned --
Rough Country taught us what an un-acknowledged order channel costs, and TBC publishes no
997-equivalent either.
"""
import datetime
import logging
import typing
from decimal import Decimal, InvalidOperation

from src import constants
from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.ntw import api_client as ntw_api
from src.integrations.clients.ntw import exceptions as ntw_exceptions
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[NTW-ORDER-ADAPTER]"

# TBC's own wording in estimatedDelivery, mapped to a service-level code we can show and send
# back. Their values are delivery *speeds*, not carrier services -- there is no carrier catalogue
# to pick from, so the code is the normalised speed.
_DELIVERY_CODES = {
    "same day": "SAME_DAY",
    "next day": "NEXT_DAY",
    "next truck": "NEXT_TRUCK",
    "2 day": "TWO_DAY",
    "10 days plus": "TEN_DAYS_PLUS",
}

_ORDER_TYPE_PICKUP = "PickUp"
_ORDER_TYPE_SHIP = "Ship"


def _decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _date(value: typing.Any) -> typing.Optional[datetime.date]:
    """TBC writes dates as M/D/YYYY in the inventory response (e.g. "11/06/2026")."""
    text = (str(value) or "").strip() if value is not None else ""
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


class NtwOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.NTW.value
    fulfillment_channel_name = "api"

    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
    ) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider, order_account)
        order_credentials = credentials_helper.get_order_credentials(company_provider, order_account)
        feed_credentials = credentials_helper.get_feed_credentials(company_provider)

        # customer_id may have been entered once on the feed connection (the onboarding card asks
        # for it there, since the dealer gets it from the same rep), so the order account's own
        # value wins but the feed's is accepted. dealer_code and store_number are order-only.
        self._customer_id = (
            order_credentials.get(constants.NTW_CREDENTIALS_CUSTOMER_ID)
            or feed_credentials.get(constants.NTW_CREDENTIALS_CUSTOMER_ID)
            or ""
        ).strip()
        self._dealer_code = (order_credentials.get(constants.NTW_ORDER_CREDENTIALS_DEALER_CODE) or "").strip()
        self._store_number = (order_credentials.get(constants.NTW_ORDER_CREDENTIALS_STORE_NUMBER) or "").strip()

        if not self._customer_id:
            raise ValueError(
                "NTW connection {} has no customer_id; TBC's order API requires it and it is not "
                "the account number in the feed filename.".format(company_provider.id)
            )
        self._client = ntw_api.NtwApiClient()

    # -- helpers -------------------------------------------------------------

    def _ntw_parts_for(
        self, line_items: typing.List[base.OrderLineItemRequest]
    ) -> typing.Dict[int, src_models.NtwParts]:
        """Map each line item id to its feed row, via the composite provider_external_id."""
        wanted: typing.Dict[int, typing.Tuple[int, str]] = {}
        for item in line_items:
            external_id = item.provider_part.provider_external_id or ""
            brand_id, _, part_number = external_id.partition("_")
            if brand_id.isdigit() and part_number:
                wanted[item.line_item_id] = (int(brand_id), part_number)

        if not wanted:
            return {}

        rows = src_models.NtwParts.objects.filter(
            brand_id__in={b for b, _ in wanted.values()},
            part_number__in={pn for _, pn in wanted.values()},
        ).select_related("brand")
        by_key = {(r.brand_id, r.part_number): r for r in rows}
        return {
            line_item_id: by_key[key]
            for line_item_id, key in wanted.items()
            if key in by_key
        }

    @staticmethod
    def _tire_size_for(part: src_models.NtwParts) -> typing.Optional[typing.Dict[str, str]]:
        width = (part.size_component_1 or "").strip()
        ratio = (part.size_component_2 or "").strip()
        diameter = (part.size_component_3 or "").strip()
        if not width or not ratio or not diameter:
            return None
        return {"width": width, "ratio": ratio, "diameter": diameter}

    def _ship_options(self, row: typing.Dict) -> typing.List[base.ShipOption]:
        raw = (row.get("estimatedDelivery") or "").strip()
        if not raw:
            return []
        code = _DELIVERY_CODES.get(raw.lower(), raw.upper().replace(" ", "_"))
        freight = _decimal(row.get("freightPerTire"))
        return [
            base.ShipOption(
                service_level_code=code,
                service_level_name=raw,
                estimated_delivery_date=_date(row.get("estimatedDeliveryDate")),
                cost=freight,
                quote_option_id=code,
            )
        ]

    # -- quote ---------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        parts_by_line = self._ntw_parts_for(line_items)

        criteria: typing.List[typing.Dict] = []
        size_by_line: typing.Dict[int, typing.Dict[str, str]] = {}
        for item in line_items:
            part = parts_by_line.get(item.line_item_id)
            if not part:
                continue
            size = self._tire_size_for(part)
            if not size:
                continue
            size_by_line[item.line_item_id] = size
            if size not in criteria_sizes(criteria):
                # requestedQty is a FILTER on TBC's side, not a quantity -- asking for the
                # buyer's quantity would hide every tire with less stock than that, which is
                # the opposite of what a quote needs to discover. Always 1; the comparison
                # against what the buyer wants happens below.
                criteria.append({"tireSize": size, "requestedQty": ntw_api.STOCK_READ_QTY})

        request_payload = {
            "customerId": self._customer_id,
            "criteria": criteria,
            "isDropShip": not ship_to.is_shop_address,
        }

        if not criteria:
            raise order_exceptions.OrderValidationError(
                "None of these lines has a complete metric tire size, so TBC's size-keyed "
                "inventory API cannot quote them.",
                request_payload=request_payload,
            )

        try:
            rows = self._client.get_tire_inventory(
                self._customer_id,
                criteria,
                store_number=self._store_number or None,
                is_drop_ship=not ship_to.is_shop_address,
                dealer_code=self._dealer_code or None,
            )
        except ntw_exceptions.NtwApiBusinessError as e:
            raise order_exceptions.OrderValidationError(
                str(e), request_payload=request_payload
            ) from e
        except ntw_exceptions.NtwException as e:
            raise order_exceptions.OrderAdapterError(str(e)) from e

        rows_by_part = {}
        for row in rows:
            key = str(row.get("partNo") or "").strip().upper()
            if key:
                rows_by_part.setdefault(key, row)

        lines: typing.List[base.ShippingQuoteLine] = []
        raw_by_line: typing.Dict[str, typing.Dict] = {}
        distributor_total = Decimal("0")
        saw_price = False

        for item in line_items:
            part = parts_by_line.get(item.line_item_id)
            external_id = item.provider_part.provider_external_id or ""
            if not part:
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=item.line_item_id,
                        provider_external_id=external_id,
                        quantity_available=0,
                        quantity_backordered=item.quantity,
                        flags=["blocked"],
                    )
                )
                continue

            row = rows_by_part.get(part.part_number.strip().upper())
            if row is None:
                # TBC listed the size but not this part: genuinely unavailable, not an error.
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=item.line_item_id,
                        provider_external_id=external_id,
                        quantity_available=0,
                        quantity_backordered=item.quantity,
                        flags=["backorder"],
                    )
                )
                continue

            sources = row.get("availableSource") or []
            if isinstance(sources, dict):
                sources = [sources]
            # Pick the single branch that can serve the most of this line. TBC's submit takes one
            # locationId per line, so a split across warehouses is not expressible -- choosing the
            # deepest one maximises what ships now and leaves the rest as a real backorder.
            best_location = None
            best_qty = 0
            for source in sources:
                qty = source.get("quantityAvailable")
                qty = int(qty) if isinstance(qty, (int, float)) else 0
                if qty > best_qty:
                    best_qty = qty
                    best_location = source

            available = min(best_qty, item.quantity)
            unit_price = _decimal((row.get("productCost") or {}).get("amount"))
            line_total = (unit_price * available) if (unit_price is not None and available) else None
            if line_total is not None:
                distributor_total += line_total
                saw_price = True

            location_id = str((best_location or {}).get("locationId") or "") or None
            address = (best_location or {}).get("address") or {}
            warehouse_name = ", ".join(
                [p for p in (address.get("city"), address.get("state")) if p]
            ) or None

            raw_by_line[str(item.line_item_id)] = {
                "partNo": row.get("partNo"),
                "brandName": row.get("brandName"),
                "locationId": location_id,
                "quantityAvailable": best_qty,
                "estimatedDelivery": row.get("estimatedDelivery"),
                "productCost": row.get("productCost"),
            }

            lines.append(
                base.ShippingQuoteLine(
                    line_item_id=item.line_item_id,
                    provider_external_id=external_id,
                    quantity_available=available,
                    quantity_backordered=max(0, item.quantity - available),
                    warehouse_code=location_id,
                    warehouse_name=warehouse_name,
                    ship_options=self._ship_options(row),
                    flags=["backorder"] if available < item.quantity else [],
                    distributor_unit_price=unit_price,
                    distributor_line_total=line_total,
                )
            )

        return base.ShippingQuoteResult(
            lines=lines,
            # Keyed by line item id because submit_order needs the locationId back, and it has
            # no other way to recover which branch this quote chose.
            raw_response={"lines": raw_by_line, "customerId": self._customer_id},
            distributor_total=distributor_total if saw_price else None,
            request_payload=request_payload,
        )

    # -- submit --------------------------------------------------------------

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        quote = purchase_order.quote_raw_response or {}
        quoted_lines = (quote.get("lines") or {}) if isinstance(quote, dict) else {}
        parts_by_line = self._ntw_parts_for(line_items)

        items: typing.List[typing.Dict] = []
        for item in line_items:
            part = parts_by_line.get(item.line_item_id)
            quoted = quoted_lines.get(str(item.line_item_id)) or {}
            location_id = quoted.get("locationId")
            if not part or not location_id:
                # Not a soft failure: TBC rejects a line with no locationId, and we cannot invent
                # one. The quote is where the warehouse is chosen, so a missing one means the
                # quote is stale or never ran -- which the 15-minute TTL should already have
                # caught before we got here.
                raise order_exceptions.OrderValidationError(
                    "Line {} has no quoted NTW warehouse; re-quote before submitting.".format(
                        item.line_item_id
                    ),
                    request_payload={"items": items},
                )
            items.append(
                {
                    "brandName": part.brand.name,
                    "partNo": part.part_number,
                    "locationId": str(location_id),
                    "qty": item.quantity,
                }
            )

        body = {
            "customerId": self._customer_id,
            "poNumber": base.resolve_po_number(purchase_order),
            "isDropShip": not ship_to.is_shop_address,
            "type": _ORDER_TYPE_SHIP,
            "source": "AfterMarketScout",
            "address": {
                "name": ship_to.name,
                "address": ship_to.address1,
                "address2": ship_to.address2 or "",
                "city": ship_to.city,
                "state": ship_to.state,
                "country": ship_to.country or "USA",
                "zipCode": ship_to.postal_code,
            },
            "items": items,
            "placed": purchase_order.submitted_at.isoformat() if purchase_order.submitted_at else None,
        }
        if self._dealer_code:
            body["dealerCode"] = self._dealer_code

        try:
            response = self._client.create_order(body)
        except ntw_exceptions.NtwApiNotConfiguredError as e:
            # The signing secret. Deliberately not retried and not downgraded.
            raise order_exceptions.OrderAdapterNotImplementedError(str(e)) from e
        except ntw_exceptions.NtwApiBusinessError as e:
            raise order_exceptions.OrderValidationError(str(e), request_payload=body) from e
        except ntw_exceptions.NtwException as e:
            raise order_exceptions.OrderAdapterError(str(e)) from e

        # One submit can return SEVERAL TBC order numbers, so the response is an array. Taking
        # [0] would silently orphan the rest.
        entries = response if isinstance(response, list) else [response]
        order_numbers: typing.List[str] = []
        placements: typing.List[base.LineItemPlacement] = []
        part_to_line = {
            (parts_by_line[i.line_item_id].part_number.strip().upper()): i
            for i in line_items
            if i.line_item_id in parts_by_line
        }

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            order_number = str(entry.get("id") or "").strip()
            if not order_number:
                continue
            order_numbers.append(order_number)

            entry_items = entry.get("items") or []
            if isinstance(entry_items, dict):
                entry_items = [entry_items]
            for entry_item in entry_items:
                key = str(entry_item.get("partNo") or "").strip().upper()
                item = part_to_line.get(key)
                if not item:
                    continue
                qty = entry_item.get("qty")
                confirmed = int(qty) if isinstance(qty, (int, float)) else item.quantity
                placements.append(
                    base.LineItemPlacement(
                        line_item_id=item.line_item_id,
                        distributor_order_number=order_number,
                        quantity_confirmed=confirmed,
                        quantity_backordered=max(0, item.quantity - confirmed),
                        warehouse_code=str(entry_item.get("locationId") or "") or None,
                    )
                )

        if not order_numbers:
            raise order_exceptions.OrderValidationError(
                "TBC accepted the request but returned no order id: {}".format(str(response)[:300]),
                request_payload=body,
            )

        return base.DistributorOrderResult(
            distributor_order_numbers=order_numbers,
            line_item_placements=placements,
            raw_response={"orders": entries},
            request_payload=body,
        )

    # -- status --------------------------------------------------------------

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        distributor_orders = list(purchase_order.distributor_orders.all())
        if not distributor_orders:
            return base.OrderStatusResult(orders=[])

        statuses: typing.List[base.DistributorOrderStatus] = []
        for distributor_order in distributor_orders:
            order_number = distributor_order.distributor_order_number
            try:
                payload = self._client.get_order(order_number)
            except ntw_exceptions.NtwApiNotConfiguredError as e:
                raise order_exceptions.OrderAdapterNotImplementedError(str(e)) from e
            except ntw_exceptions.NtwException as e:
                logger.error(
                    "{} Status lookup failed for order {}: {}".format(_LOG_PREFIX, order_number, str(e))
                )
                raise order_exceptions.OrderAdapterError(str(e)) from e

            raw_status = str(payload.get("status") or "").strip()
            tracking = str(payload.get("trackingNumber") or "").strip()
            statuses.append(
                base.DistributorOrderStatus(
                    distributor_order_number=order_number,
                    status_code=raw_status,
                    tracking_numbers=[tracking] if tracking else [],
                    carrier=None,
                    raw_response=payload,
                    estimated_delivery_date=_date(payload.get("estDelivery")),
                    delivery_status=_delivery_status(raw_status),
                )
            )

        return base.OrderStatusResult(orders=statuses)

    # -- capabilities --------------------------------------------------------

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError("TBC exposes no cancel endpoint for NTW orders.")

    def supports_cancel(self) -> bool:
        return False

    def supports_shipping_method_selection(self) -> bool:
        # TBC's items[] takes carrierServiceCode/SCAC, but there is no endpoint to enumerate
        # valid values, so there is nothing to populate a picker with. The quote's own
        # ship_options report whatever speed TBC quoted.
        return False

    def supports_invoices(self) -> bool:
        return False


def criteria_sizes(criteria: typing.List[typing.Dict]) -> typing.List[typing.Dict]:
    """The tire sizes already queued, so two lines of the same size cost one criteria slot --
    which matters because the gateway honours only two per call."""
    return [c.get("tireSize") for c in criteria]


def _delivery_status(raw_status: str) -> typing.Optional[str]:
    """
    Normalise to the vocabulary base.DistributorOrderStatus documents, and only where TBC's own
    wording actually says so -- None rather than a guess, since their status values are
    undocumented and we have never seen a real one.
    """
    text = (raw_status or "").strip().lower()
    if not text:
        return None
    if "cancel" in text:
        return "cancelled"
    if "deliver" in text:
        return "delivered"
    if "transit" in text or "shipped" in text:
        return "in_transit"
    return None
