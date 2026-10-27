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

No request signing. TBC's developer portal shows ``signature`` and ``timestamp`` as query
parameters on these endpoints, and this adapter used to refuse to submit without a secret for
them; the Order Service v1.0 spec states the API has no query parameters at all and
authenticates with the same two headers as the inventory call. The portal page is wrong, or
describes something retired.
"""
import datetime
import logging
import re
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

# Order Service v1.0: "PickUp" (will-call from the warehouse) or "Delivery" (by truck). An
# earlier version sent "Ship", which is not in TBC's enumeration.
_ORDER_TYPE_PICKUP = ntw_api.ORDER_TYPE_PICKUP
_ORDER_TYPE_DELIVERY = ntw_api.ORDER_TYPE_DELIVERY


def _decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _date(value: typing.Any) -> typing.Optional[datetime.date]:
    """
    TBC writes dates in two different shapes depending on which API answered, and an earlier
    version of this function only knew the first:

      inventory  estimatedDeliveryDate  "11/06/2026"            M/D/YYYY
      order      estDelivery            "2023-11-01T13:50Z"     RFC 3339, per the spec's own
                                                                "Date/Time Format" section

    So every estimated delivery date coming back from an order status was silently dropped.
    """
    text = (str(value) or "").strip() if value is not None else ""
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    # RFC 3339. Python's fromisoformat does not accept a trailing "Z" before 3.11, and TBC's
    # samples omit seconds ("13:50Z"), which it also rejects -- so normalise both.
    iso = text.replace("Z", "+00:00")
    for candidate in (iso, re.sub(r"T(\d{2}):(\d{2})([+-])", r"T\1:\2:00\3", iso)):
        try:
            return datetime.datetime.fromisoformat(candidate).date()
        except ValueError:
            continue
    return None


def _ship_to_payload(ship_to: base.ShipToAddress) -> typing.List[typing.Dict]:
    """``ShipToAddress`` in TBC's ``shipTo`` shape (an array of one, with a nested address)."""
    return [{
        "address": {
            "name": ship_to.name or "",
            "address": ship_to.address1 or "",
            "address2": ship_to.address2 or "",
            "city": ship_to.city or "",
            "state": ship_to.state or "",
            "country": ship_to.country or "US",
            "zipCode": ship_to.postal_code or "",
        },
        "email": ship_to.email or "",
        "phoneNumber": ship_to.phone or "",
        "extraInfo": "",
    }]


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

        # Everything the two TBC APIs need lives on the order account: the dealer's own
        # application key/secret and their NTW account number. The product feed holds only an
        # SFTP login -- reading a file needs no account number, so requiring one there would
        # block a feed that works without it. dealerCode and storeNumber are not collected:
        # TBC's own spec says dealerCode is unnecessary once customerId is present.
        self._customer_id = (
            order_credentials.get(constants.NTW_ORDER_CREDENTIALS_CUSTOMER_ID) or ""
        ).strip()
        client_id = (order_credentials.get(constants.NTW_ORDER_CREDENTIALS_CLIENT_ID) or "").strip()
        client_secret = (order_credentials.get(constants.NTW_ORDER_CREDENTIALS_CLIENT_SECRET) or "").strip()

        if not self._customer_id:
            raise ValueError(
                "NTW connection {} has no customer_id; TBC's order API requires the dealer's NTW "
                "account number.".format(company_provider.id)
            )
        if not client_id or not client_secret:
            raise ValueError(
                "NTW connection {} has no TBC API key/secret. Each dealer creates their own "
                "application in TBC's developer portal and subscribes it to the InventoryService "
                "and OrderService plans.".format(company_provider.id)
            )
        self._client = ntw_api.NtwApiClient(client_id=client_id, client_secret=client_secret)

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

        pairs = []
        for item in line_items:
            part = parts_by_line.get(item.line_item_id)
            if part:
                pairs.append((part.brand.name, part.part_number))

        request_payload = {
            "customerId": self._customer_id,
            "searchType": ntw_api.SEARCH_BY_PRODUCT_ID,
            "parts": [{"brandName": b, "partNo": pn} for b, pn in pairs],
            "isDropShip": not ship_to.is_shop_address,
        }

        if not pairs:
            raise order_exceptions.OrderValidationError(
                "None of these lines resolves to an NTW catalog row, so TBC cannot be asked "
                "about them.",
                request_payload=request_payload,
            )

        try:
            # One call for the whole cart: ByProductId has no the two-entry truncation that
            # ByTireSize does, and asks with requestedQty=0 so a line that is out of stock comes
            # back as a real zero instead of vanishing.
            found = self._client.get_inventory_by_parts(
                self._customer_id,
                pairs,
                ship_to=_ship_to_payload(ship_to),
                is_drop_ship=not ship_to.is_shop_address,
            )
        except ntw_exceptions.NtwApiBusinessError as e:
            raise order_exceptions.OrderValidationError(
                str(e), request_payload=request_payload
            ) from e
        except ntw_exceptions.NtwException as e:
            raise order_exceptions.OrderAdapterError(str(e)) from e

        rows_by_part = {part_number.strip().upper(): row
                        for (_brand, part_number), row in found.items()}

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
            "type": _ORDER_TYPE_DELIVERY,
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

        try:
            response = self._client.create_order(body)
        except ntw_exceptions.NtwApiNotConfiguredError as e:
            raise order_exceptions.OrderAdapterNotImplementedError(str(e)) from e
        except ntw_exceptions.NtwApiBusinessError as e:
            # TBC reports a shortfall this way: errorCd 1006 "Quantity can not be fulfilled".
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
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        """
        TBC's published carrier/service enumeration (Order Service v1.0). A catalogue, not a
        priced comparison -- they have no endpoint that costs these, so choosing one filters the
        order rather than shopping it. Only honoured on a drop-ship order: TBC ignores
        carrierServiceCode/SCAC when isDropShip is false.
        """
        return [
            base.ShippingMethod(code=code, name=code.replace("_", " ").title(), carrier_name=scac)
            for scac, code in ntw_api.CARRIER_SERVICE_CODES
        ]

    def supports_invoices(self) -> bool:
        return True

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[base.DistributorInvoice]:
        """
        Synthesized from order status, because TBC exposes no invoice endpoint for NTW.

        What makes this possible is the one word in their status vocabulary: "Delivered" means
        "If the order is Invoiced or Shipped or Delivered" (Order Service v1.0). So an order
        reaching that state HAS been billed, and that is the only billing signal this
        integration can observe. "Processing" means confirmed but not yet billed, and "Rejected"
        means cancelled -- neither produces an invoice.

        Same approach as Keystone, which also has no invoice document and groups its order-history
        rows into one. The difference, and it is the honest caveat here: Keystone at least has a
        real invoice number and billed line prices in that feed, and NTW has neither.

          invoice_number   TBC's ORDER id. There is no invoice number anywhere in their API, and
                           a synthesized one would be a number that exists nowhere on their side
                           and cannot be quoted back to a rep. comments says so explicitly.
          money fields     left None. We hold a quote, not a bill. Filling total_price from our
                           own quoted cost would present an estimate as the amount TBC charged,
                           which is the one thing an invoice must not do.
          invoice_date     left None. estDelivery is a delivery estimate, not a billing date.

        What it does carry is real: the billed line items, the fulfilling warehouse, and the
        tracking number.
        """
        distributor_orders = list(purchase_order.distributor_orders.all())
        if not distributor_orders:
            return []

        invoices: typing.List[base.DistributorInvoice] = []
        for distributor_order in distributor_orders:
            order_number = distributor_order.distributor_order_number
            try:
                payload = self._client.get_order(order_number)
            except ntw_exceptions.NtwApiNotConfiguredError as e:
                raise order_exceptions.OrderAdapterNotImplementedError(str(e)) from e
            except ntw_exceptions.NtwException as e:
                logger.error(
                    "{} Invoice lookup failed for order {}: {}".format(_LOG_PREFIX, order_number, str(e))
                )
                raise order_exceptions.OrderAdapterError(str(e)) from e

            if _delivery_status(str(payload.get("status") or "")) != "delivered":
                # Not billed yet (Processing), or never will be (Rejected).
                continue

            location = payload.get("location") or {}
            warehouse_code = str(location.get("id") or "") or None
            items = payload.get("items") or []
            if isinstance(items, dict):
                items = [items]

            line_items = [
                base.InvoiceLineItem(
                    part_number=str(item.get("partNo") or "") or None,
                    description=str(item.get("brandName") or "") or None,
                    quantity=int(item["qty"]) if isinstance(item.get("qty"), (int, float)) else None,
                    warehouse_code=warehouse_code,
                )
                for item in items
            ]
            tracking_number = str(payload.get("trackingNumber") or "").strip()

            invoices.append(
                base.DistributorInvoice(
                    invoice_number=order_number,
                    distributor_order_number=order_number,
                    tracking=(
                        [base.InvoiceTrackingEntry(tracking_number=tracking_number)]
                        if tracking_number else []
                    ),
                    line_items=line_items,
                    comments=(
                        "Synthesized from NTW order status, not a real invoice document. TBC "
                        "exposes no invoice endpoint, and their status 'Delivered' covers "
                        "invoiced, shipped and delivered alike -- so this records that the order "
                        "has been billed, not what was billed. The invoice number shown is TBC's "
                        "ORDER id: they issue no invoice number through this API. Amounts and "
                        "the invoice date are unavailable and are deliberately left blank rather "
                        "than filled from our own quote."
                    ),
                    raw_response=payload,
                )
            )

        return invoices




# TBC documents exactly three order statuses (Order Service v1.0), and the mapping is not
# one-to-one with ours:
#
#   "Processing"  the order is confirmed and nothing has shipped -> no delivery signal yet
#   "Rejected"    the order was cancelled                        -> cancelled
#   "Delivered"   "If the order is Invoiced or Shipped or Delivered"
#
# That last one is the important caveat: TBC collapses invoiced, shipped and delivered into one
# word, so it means "TBC is done with it", NOT that a tire reached the customer. It is also the
# closest thing this integration gets to an invoice signal, since TBC exposes no invoice
# endpoint -- an order reaching "Delivered" is when it has been billed.
#
# A closed vocabulary rather than substring matching: an unrecognised status yields no signal,
# so if TBC adds a value we show nothing instead of guessing it.
_TBC_DELIVERY_STATUS = {
    "DELIVERED": "delivered",
    "REJECTED": "cancelled",
    "PROCESSING": None,
}


def _delivery_status(raw_status: str) -> typing.Optional[str]:
    """Normalise to base.DistributorOrderStatus's vocabulary, or None when TBC gives no signal."""
    return _TBC_DELIVERY_STATUS.get((raw_status or "").strip().upper())
