"""
MeyerOrderAdapter — DistributorOrderAdapter implementation for Meyer Distributing's v2 REST
Order API. Wraps ``src.integrations.clients.meyer.order_client.MeyerOrderApiClient``.

SAFETY: submit_order() places a REAL order against Meyer, and cancel_order() has real (possibly
irreversible) effect. Both must only ever be invoked from an explicit, user-approved
submission/cancellation — never from exploratory/dev code, automated tests, or ad-hoc scripts.
See ``src/integrations/orders/turn_14.py`` for the reference adapter this mirrors.

Not yet handled: Meyer's "route"/Meyer Truck orders (AddressCode + "Meyer Truck" ship method,
a distinct order-creation path per Meyer's docs) — out of scope until a company actually needs
that fulfillment mode.
"""
import datetime
import decimal
import logging
import re
import typing

from django.conf import settings
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.meyer import exceptions as meyer_client_exceptions
from src.integrations.clients.meyer.order_client import MeyerOrderApiClient
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[MEYER-ORDER-ADAPTER]"

# Cancel error codes that mean "the order has already progressed past a cancellable state," per
# Meyer's docs — cancel_order() must return False for these (per base.py's contract), not raise.
_CANCEL_NOT_CANCELLABLE_CODES = {"60501", "60502"}

# GET /Invoice error codes meaning "nothing has invoiced yet" (40400: no OrderNumber matched,
# 70202: "No invoice found for [OrderNumber]") — get_invoices() must return an empty list for
# these, not raise, since it's a routine/expected state for most of a PO's lifetime before
# anything ships.
_NO_INVOICE_YET_CODES = {"40400", "70202"}

# Meyer's own documented cap on how many item numbers ItemInformation accepts per call.
_ITEM_INFORMATION_CHUNK_SIZE = 100

# Meyer's own docs: DeliveryDate on a quote line is either a literal date ("9/28/2017") or a
# business-day estimate string ("3-5 Business Days" / "5 Business Days") — same field, two
# shapes, confirmed on both ShippingRateQuote and ShippingRateMassQuote.
_MEYER_BUSINESS_DAYS_PATTERN = re.compile(r"^(?:(\d+)\s*-\s*)?(\d+)\s*Business\s*Days?$", re.IGNORECASE)


def _parse_decimal(value: typing.Optional[typing.Any]) -> typing.Optional[decimal.Decimal]:
    if value in (None, ""):
        return None
    try:
        return decimal.Decimal(str(value))
    except decimal.InvalidOperation:
        return None


def _add_business_days(start: datetime.date, business_days: int) -> datetime.date:
    d = start
    remaining = business_days
    while remaining > 0:
        d += datetime.timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            remaining -= 1
    return d


def _parse_meyer_delivery(
    value: typing.Optional[str],
) -> typing.Tuple[typing.Optional[datetime.date], typing.Optional[int]]:
    """(estimated_delivery_date, days_in_transit) from Meyer's DeliveryDate field — mirrors
    keystone.py's _days_in_transit (diffs two Keystone-supplied dates into a day count), except
    Meyer only ever gives us one of two shapes instead of a matching pair of dates:
      - a literal date ("9/28/2017"): diff it against today for days_in_transit, with today
        standing in for the order-placement date (the role Keystone's own OrderDate plays).
      - a business-day estimate ("3-5 Business Days"): Meyer already gives us the day count
        directly (use the upper bound of a range), so derive a concrete date by adding that
        many business days to today, so the FE still has a real date to show.
    """
    if not value:
        return None, None
    value = value.strip()
    today = timezone.now().date()

    m = _MEYER_BUSINESS_DAYS_PATTERN.match(value)
    if m:
        upper = int(m.group(2))
        return _add_business_days(today, upper), upper

    try:
        estimated_delivery_date = datetime.datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError:
        return None, None
    delta = (estimated_delivery_date - today).days
    return estimated_delivery_date, (delta if delta >= 0 else None)


def _parse_meyer_date(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    """Plain M/D/YYYY date parsing for header fields like Invoice.InvoiceDate/OrderDate — unlike
    _parse_meyer_delivery, these are never a business-days estimate string."""
    if not value or not value.strip():
        return None
    try:
        return datetime.datetime.strptime(value.strip(), "%m/%d/%Y").date()
    except ValueError:
        return None


def _summarize_meyer_tracking(
    rows: typing.List[typing.Dict],
) -> typing.Tuple[typing.Optional[datetime.date], typing.Optional[str]]:
    """Collapses GET /SalesTracking's per-tracking-number rows (one order can have several) into
    one (estimated_delivery_date, delivery_status) pair for DistributorOrderStatus. Meyer only
    ever populates DeliveryDate for a delivered Meyer Truck shipment (per its docs) — treated as
    the strongest signal available; otherwise fall back to the earliest parsed DeliveryETA across
    rows (reusing _parse_meyer_delivery, the same ETA-string parsing quote-time already uses) as
    an in-transit estimate. Meyer never gives an actual ship date, only these two."""
    if not rows:
        return None, None
    if any((r.get("DeliveryDate") or "").strip() for r in rows):
        return None, "delivered"
    etas = [d for d, _ in (_parse_meyer_delivery(r.get("DeliveryETA")) for r in rows) if d]
    return (min(etas) if etas else None), ("in_transit" if etas else None)


def _filter_options(
    options: typing.List[base.ShipOption], ship_method: typing.Optional[str]
) -> typing.List[base.ShipOption]:
    if not ship_method:
        return options
    filtered = [o for o in options if o.service_level_code == ship_method]
    return filtered or options


def _load_meyer_warehouse_names() -> typing.Dict[str, str]:
    """{external_id: "City, ST"} from MeyerLocation (populated by fetch_meyer_locations from
    GET /Warehouses), so quote responses can show a real place instead of a bare code like
    "053" — same role turn_14.py's _load_warehouse_names plays for Turn14, except Meyer has no
    separate "name" field, just city/state/country."""
    names = {}
    for row in src_models.MeyerLocation.objects.all().values("external_id", "city", "state"):
        external_id = (row.get("external_id") or "").strip()
        if not external_id:
            continue
        label = ", ".join(part for part in (row.get("city"), row.get("state")) if part)
        if label:
            names[external_id] = label
    return names


class MeyerOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.MEYER.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider)
        environment = getattr(settings, "MEYER_ORDER_ENVIRONMENT", "testing")
        self._client = MeyerOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider),
            environment=environment,
        )

    # -- Request building -----------------------------------------------------------------

    @staticmethod
    def _build_ship_to_fields(ship_to: base.ShipToAddress) -> typing.Dict[str, str]:
        return {
            "ShipToName": ship_to.name,
            "ShipToAddress1": ship_to.address1,
            "ShipToAddress2": ship_to.address2 or "",
            "ShipToCity": ship_to.city,
            "ShipToState": ship_to.state,
            "ShipToZipcode": ship_to.postal_code,
            "ShipToCountry": ship_to.country,
        }

    def _handle_error(self, e: Exception) -> None:
        code = getattr(e, "code", None)
        raise order_exceptions.OrderValidationError(message=str(e), code=str(code) if code else None)

    def _get_item_info(self, item_numbers: typing.List[str]) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
        """
        {item_number: {"price": Decimal|None, "prop_65": bool}} via ItemInformation, so quoted
        line items carry Meyer's own up-to-date price rather than only our (potentially stale)
        catalog-sync price — same role Turn14's inline quote pricing already plays for that
        adapter, and the same gap Keystone's CheckPriceBulk fills for that one. The same response
        also carries Meyer's own Prop 65 flag per item ("Prop 65": "Yes", only present when
        applicable) — folded into ShippingQuoteLine.flags by get_shipping_quote the same way
        Turn14 does from its own quote response (see turn_14.py), no separate API call needed.

        No caching layer here, unlike Keystone: Meyer's docs cap ItemInformation at 100 items
        per call but don't warn of any rate limit or account-suspension risk for calling it
        often, so there's nothing to protect against — every quote can safely fetch live data.
        Best-effort: any failure degrades to no pricing/prop_65 info for the affected item(s)
        rather than failing the shipping quote — this is an enrichment, availability/shipping is
        the core result.
        """
        info: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        for i in range(0, len(item_numbers), _ITEM_INFORMATION_CHUNK_SIZE):
            chunk = item_numbers[i : i + _ITEM_INFORMATION_CHUNK_SIZE]
            try:
                items = self._client.get_item_information(chunk, self._client.customer_number)
            except meyer_client_exceptions.MeyerException:
                logger.exception(
                    "{} ItemInformation failed for item_numbers={}; those line(s) will have no "
                    "distributor-quoted price/Prop 65 info this round.".format(_LOG_PREFIX, chunk)
                )
                continue
            for item in items:
                item_number = (item.get("ItemNumber") or "").strip()
                if not item_number:
                    continue
                info[item_number] = {
                    "price": _parse_decimal(item.get("CustomerPrice")),
                    "prop_65": (item.get("Prop 65") or "").strip().lower() == "yes",
                }
        return info

    # -- DistributorOrderAdapter ------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        data = dict(self._build_ship_to_fields(ship_to))
        data["Items"] = [
            {"ItemNumber": li.provider_part.provider_external_id, "Quantity": li.quantity} for li in line_items
        ]
        try:
            groups = self._client.get_shipping_rate_mass_quote(data)
        except meyer_client_exceptions.MeyerException as e:
            self._handle_error(e)

        logger.info("{} Quote response: {}".format(_LOG_PREFIX, repr(groups)[:4000]))

        item_info = self._get_item_info([li.provider_part.provider_external_id for li in line_items])
        warehouse_names = _load_meyer_warehouse_names()

        try:
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}
            lines: typing.List[base.ShippingQuoteLine] = []
            seen: typing.Set[str] = set()

            for group in groups:
                # Meyer's mass-quote response uses PascalCase group keys ("Warehouse", "Skus",
                # "Quotes") — confirmed against a live response. Reading them lowercase (as this
                # code previously did) silently returned "" / [] for every group, so no SKU ever
                # matched and every line fell through to the "not_returned_in_quote" fallback
                # below instead of getting real availability/shipping data.
                warehouse = group.get("Warehouse", "")
                skus = [s.strip() for s in (group.get("Skus") or "").split(",") if s.strip()]
                raw_options = []
                for q in group.get("Quotes", []):
                    estimated_delivery_date, days_in_transit = _parse_meyer_delivery(q.get("DeliveryDate"))
                    raw_options.append(
                        base.ShipOption(
                            service_level_code=(q.get("ShipMethod") or "").strip(),
                            service_level_name=q.get("ServiceType", ""),
                            estimated_delivery_date=estimated_delivery_date,
                            days_in_transit=days_in_transit,
                            cost=_parse_decimal(q.get("Cost")),
                            # No separate quote-scoped id for Meyer — CreateOrder takes the same
                            # ShipMethod code shown here directly.
                            quote_option_id=(q.get("ShipMethod") or "").strip(),
                        )
                    )
                ship_options = _filter_options(raw_options, ship_method)
                for sku in skus:
                    li = by_external_id.get(sku)
                    seen.add(sku)
                    quantity_available = li.quantity if li else 0
                    info = item_info.get(sku, {})
                    unit_price = info.get("price")
                    lines.append(
                        base.ShippingQuoteLine(
                            line_item_id=li.line_item_id if li else 0,
                            provider_external_id=sku,
                            # Meyer's mass-quote response doesn't carry an explicit per-item
                            # availability/backorder signal at quote time — an item only being
                            # absent from every group's "skus" list is our only stockout signal
                            # (handled below), so a present item is treated as fully available
                            # for the quantity we requested.
                            quantity_available=quantity_available,
                            warehouse_code=warehouse,
                            warehouse_name=warehouse_names.get(warehouse) if warehouse else None,
                            ship_options=ship_options,
                            flags=["prop_65"] if info.get("prop_65") else [],
                            # From ItemInformation (see _get_item_info) — Meyer's shipping-quote
                            # endpoint itself never returns pricing, unlike Turn14's.
                            distributor_unit_price=unit_price,
                            distributor_line_total=(
                                unit_price * quantity_available if unit_price is not None else None
                            ),
                        )
                    )

            # Items never echoed back in any group's skus weren't matched to any warehouse.
            for external_id, li in by_external_id.items():
                if external_id in seen:
                    continue
                flags = ["not_returned_in_quote"]
                if item_info.get(external_id, {}).get("prop_65"):
                    flags.append("prop_65")
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=li.line_item_id,
                        provider_external_id=external_id,
                        quantity_available=0,
                        flags=flags,
                    )
                )

            if not lines:
                raise order_exceptions.OrderValidationError(
                    "Unexpected/empty quote response shape from Meyer. Raw response: {}".format(repr(groups)[:2000])
                )
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected quote response shape from Meyer ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(groups)[:2000]
                )
            )

        return base.ShippingQuoteResult(lines=lines, raw_response={"groups": groups})

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        if not purchase_order.ship_method:
            raise order_exceptions.OrderValidationError(
                "Meyer requires a ship method to be selected before an order can be submitted."
            )

        data = dict(self._build_ship_to_fields(ship_to))
        data.update(
            {
                "ShipMethod": purchase_order.ship_method,
                "ShipToPhone": ship_to.phone or "",
                "CustPO": purchase_order.po_number,
                # We're a marketplace/dropship intermediary with no visibility into whether
                # sales tax was independently collected from the end customer — "No" is the
                # conservative default (Meyer can still add tax on their end rather than risk
                # it going uncollected). No field on PurchaseOrder sources a real answer today.
                "CollectedSalesTax": "No",
                "Items": [
                    {"ItemNumber": li.provider_part.provider_external_id, "Quantity": li.quantity}
                    for li in line_items
                ],
            }
        )

        try:
            response = self._client.create_order(customer_number=self._client.customer_number, data=data)
        except meyer_client_exceptions.MeyerException as e:
            self._handle_error(e)

        return self._parse_submit_response(response, line_items)

    @staticmethod
    def _parse_submit_response(
        response: typing.Dict, line_items: typing.List[base.OrderLineItemRequest]
    ) -> base.DistributorOrderResult:
        try:
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}
            order_numbers: typing.List[str] = []
            placements: typing.List[base.LineItemPlacement] = []

            # Meyer can split one submission into genuinely separate order numbers (one per
            # fulfilling warehouse) — see base.DistributorOrderResult's docstring, which already
            # names this exact shape.
            for order in response.get("Orders", []):
                order_number = order.get("OrderNumber", "")
                if order_number:
                    order_numbers.append(order_number)
                for item in order.get("Items", []):
                    external_id = item.get("ItemNumber", "")
                    li = by_external_id.get(external_id)
                    placements.append(
                        base.LineItemPlacement(
                            line_item_id=li.line_item_id if li else 0,
                            distributor_order_number=order_number,
                            quantity_confirmed=item.get("Quantity", 0),
                        )
                    )

            if not order_numbers:
                raise order_exceptions.OrderValidationError(
                    "Unexpected/empty order response shape from Meyer. Raw response: {}".format(
                        repr(response)[:2000]
                    )
                )
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order response shape from Meyer ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                )
            )

        return base.DistributorOrderResult(
            distributor_order_numbers=order_numbers,
            line_item_placements=placements,
            raw_response=response,
        )

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        try:
            orders = self._client.get_sales_order_detail(
                order_number=purchase_order.po_number, customer_number=self._client.customer_number
            )
        except meyer_client_exceptions.MeyerException as e:
            self._handle_error(e)

        try:
            results: typing.List[base.DistributorOrderStatus] = []
            for order in orders:
                order_number = order.get("OrderNumber", "")
                invoiced = order.get("Invoiced", "")
                tracking_numbers = set(order.get("Tracking") or [])

                # Best-effort: SalesOrderDetail's embedded Tracking[] has no ETA/delivered-date
                # info at all — GET /SalesTracking is Meyer's only source for that (see
                # _summarize_meyer_tracking). A failure here degrades to no date/delivery_status
                # for this order rather than failing the whole status check, same reasoning as
                # _sync_invoices in purchase_order_jobs.py: status/tracking-number data above
                # already succeeded and is the primary result.
                estimated_delivery_date, delivery_status = None, None
                if order_number:
                    try:
                        tracking_rows = self._client.get_sales_tracking(
                            customer_number=self._client.customer_number, order_number=order_number
                        )
                    except meyer_client_exceptions.MeyerException:
                        logger.warning(
                            "{} SalesTracking failed for order_number={}; delivery date/status "
                            "unavailable this round.".format(_LOG_PREFIX, order_number)
                        )
                    else:
                        tracking_numbers.update(
                            t for t in (r.get("TrackingNumber") for r in tracking_rows) if t
                        )
                        estimated_delivery_date, delivery_status = _summarize_meyer_tracking(tracking_rows)

                results.append(
                    base.DistributorOrderStatus(
                        distributor_order_number=order_number,
                        status_code="INVOICED" if invoiced == "Yes" else "OPEN",
                        tracking_numbers=sorted(tracking_numbers),
                        estimated_delivery_date=estimated_delivery_date,
                        delivery_status=delivery_status,
                        raw_response=order,
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order-status response shape from Meyer ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(orders)[:2000]
                )
            )
        return base.OrderStatusResult(orders=results)

    def supports_invoices(self) -> bool:
        return True

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[base.DistributorInvoice]:
        try:
            raw_invoices = self._client.get_invoice(
                order_number=purchase_order.po_number, customer_number=self._client.customer_number
            )
        except meyer_client_exceptions.MeyerOrderValidationError as e:
            if e.code in _NO_INVOICE_YET_CODES:
                return []
            self._handle_error(e)
        except meyer_client_exceptions.MeyerException as e:
            self._handle_error(e)

        try:
            invoices: typing.List[base.DistributorInvoice] = []
            for inv in raw_invoices:
                invoice_number = str(inv.get("InvoiceNumber", "")).strip()
                if not invoice_number:
                    continue
                line_items = [
                    base.InvoiceLineItem(
                        part_number=item.get("ItemNumber"),
                        quantity=item.get("Quantity"),
                        unit_price=_parse_decimal(item.get("UnitPrice")),
                        total_price=_parse_decimal(item.get("ExtendedPrice")),
                    )
                    for item in (inv.get("Items") or [])
                ]
                invoices.append(
                    base.DistributorInvoice(
                        invoice_number=invoice_number,
                        invoice_date=_parse_meyer_date(inv.get("InvoiceDate")),
                        distributor_order_number=inv.get("OrderNumber") or None,
                        # Meyer's Invoice response has no separate "website order number"
                        # concept, no paid_amount/amount_due, and no per-invoice tracking array
                        # (that's SalesTracking/SalesOrderDetail's job) — left None/[] rather
                        # than guessed. total_price already includes Tax/MiscCharge, which have
                        # no dedicated field here; see raw_response for the full breakdown.
                        total_price=_parse_decimal(inv.get("Total")),
                        freight=_parse_decimal(inv.get("Freight")),
                        discount_amount=_parse_decimal(inv.get("TradeDiscount")),
                        line_items=line_items,
                        raw_response=inv,
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected invoice response shape from Meyer ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(raw_invoices)[:2000]
                )
            )
        return invoices

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        distributor_orders = list(purchase_order.distributor_orders.all())
        if not distributor_orders:
            return False

        all_cancelled = True
        for distributor_order in distributor_orders:
            try:
                self._client.cancel_order(
                    order_number=distributor_order.distributor_order_number,
                    customer_number=self._client.customer_number,
                )
            except meyer_client_exceptions.MeyerOrderValidationError as e:
                if e.code in _CANCEL_NOT_CANCELLABLE_CODES:
                    all_cancelled = False
                    continue
                self._handle_error(e)
            except meyer_client_exceptions.MeyerException as e:
                self._handle_error(e)
        return all_cancelled

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        try:
            methods = self._client.get_ship_methods()
        except meyer_client_exceptions.MeyerException as e:
            self._handle_error(e)

        return [
            base.ShippingMethod(
                code=(m.get("ShipMethod") or "").strip(),
                name=m.get("ServiceType", ""),
                carrier_name=m.get("Carrier"),
            )
            for m in methods
        ]
