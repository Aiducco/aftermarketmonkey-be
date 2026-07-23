"""
KeystoneOrderAdapter — DistributorOrderAdapter implementation for Keystone Automotive's SOAP
Electronic Order Web Service. Wraps
``src.integrations.clients.keystone.order_client.KeystoneOrderApiClient``.

SAFETY: submit_order() places a REAL order against Keystone. It must only ever be invoked from
an explicit, user-approved submission — never from exploratory/dev code, automated tests, or
ad-hoc scripts. See ``src/integrations/orders/turn_14.py`` for the reference adapter this
mirrors.

Not yet handled: Keystone kits (``GetKitComponents``) — a kit VCPN submitted through
ShipOrderDropShipMultipleParts is silently exploded server-side into its component line items,
which this adapter does not attempt to reconcile against our own line items. Out of scope until
kit SKUs are actually sold through this path.
"""
import datetime
import decimal
import logging
import typing

from django.core.cache import cache

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.keystone import exceptions as keystone_client_exceptions
from src.integrations.clients.keystone.order_client import KeystoneOrderApiClient, VCPN_REGEX
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[KEYSTONE-ORDER-ADAPTER]"

# Keystone's own "List of Common Shipping Service Levels" reference table — their docs
# explicitly say it "is not exhaustive" and to confirm actual availability via a shipping quote;
# there's no independent "list all methods" endpoint (unlike Turn14's GET /shipping).
_COMMON_SHIPPING_METHODS = [
    base.ShippingMethod(code="U01", name="UPS Ground Commercial", carrier_name="UPS"),
    base.ShippingMethod(code="U11", name="UPS Ground", carrier_name="UPS"),
    base.ShippingMethod(code="F11", name="FedEx 2 Day Economy", carrier_name="FedEx"),
    base.ShippingMethod(code="F06", name="FedEx Standard Overnight", carrier_name="FedEx"),
    base.ShippingMethod(code="LTL", name="LTL Truck", carrier_name="Freight"),
    base.ShippingMethod(code="K06", name="Keystone Truck Run", carrier_name="Keystone"),
    base.ShippingMethod(code="P02", name="Purolator Ground", carrier_name="Purolator"),
    base.ShippingMethod(code="UPM", name="USPS Priority Mail", carrier_name="USPS"),
]

# GetOrderHistory rows whose EKORD# holds one of these means "no matching order", not a real PO.
_ORDER_HISTORY_ERROR_VALUES = {"NoData", "BadDate", "BadInDt"}

# EKSTAT's documented progression (RCV ORD -> ORDER -> PICK -> PACKAGE/XDOCK -> OUT4DLV ->
# INVOICE -> CANCEL) mapped to DistributorOrderStatus.delivery_status's normalized vocabulary.
# Left unmapped (None) for the earlier stages (RCV ORD/ORDER/PICK) — Keystone's own status_code
# already carries the raw stage for those; INVOICE means billed, not confirmed delivered, so it
# stays unmapped too rather than guessed as "delivered".
_EKSTAT_DELIVERY_STATUS = {
    "PACKAGE": "in_transit",
    "XDOCK": "in_transit",
    "OUT4DLV": "in_transit",
    "CANCEL": "cancelled",
}
# EKSTAT values from which EKDATE (that row's transaction date) is a reasonable ship_date proxy
# — Keystone gives no dedicated ship-date field, only the date of each status transition.
_EKSTAT_SHIPPED_STAGES = {"PACKAGE", "XDOCK", "OUT4DLV", "INVOICE"}

# Keystone's own "List of Active Warehouses" reference table — the same warehouse-number
# strings GetShippingOptionsMultiplePartsPerWarehouse's "Warehouse" column and its dynamically
# named "Warehouse_<name>_<number>" rate tables use (see get_shipping_quote below). "City, ST"
# to match the convention Turn14's warehouse_name already uses.
_WAREHOUSE_NAMES = {
    "1": "Exeter, PA",
    "14": "Kansas City, KS",
    "25": "Eastvale, CA",
    "30": "Atlanta, GA",
    "45": "Spokane, WA",
    "50": "Flower Mound, TX",
    "55": "Dallas, TX",
    "60": "Brownstown, MI",
    "70": "Ocoee, FL",
}

# Keystone's sentinel for "no estimate available" (seen on LTL quotes) — .NET's DateTime.MinValue
# serialized as a date, not a real delivery date.
_UNKNOWN_DATE_YEAR = 1

# CheckPriceBulk is rate-limited far more strictly than every other Keystone method (their docs:
# "should be used no more than once per hour" and warn of account suspension for overuse) — cache
# each VCPN's price for exactly that long, keyed per connection since price can vary by account.
# See KeystoneOrderAdapter._get_prices, the only caller of check_price_bulk.
_PRICE_CACHE_KEY = "keystone_price:{}:{}"  # (company_provider_id, vcpn)
_PRICE_CACHE_TTL_SECONDS = 3600
# Keystone's own documented cap on how many VCPNs one CheckPriceBulk call accepts.
_PRICE_BULK_CHUNK_SIZE = 12


def _days_in_transit(order_date: typing.Optional[str], thru_delivery: typing.Optional[str]) -> typing.Optional[int]:
    """Keystone gives an OrderDate (order-placement cutoff) and a ThruDelivery (upper bound of
    the estimated delivery window) per ship option, not a transit-day count directly — derive
    one the same way Turn14's days_in_transit is used (days from placing the order to
    delivery). Returns None when either date is missing or is Keystone's "unknown" sentinel
    (0001-01-01, seen on LTL quotes with no estimate)."""
    order = _parse_keystone_date(order_date)
    thru = _parse_keystone_date(thru_delivery)
    if not order or not thru or order.year == _UNKNOWN_DATE_YEAR or thru.year == _UNKNOWN_DATE_YEAR:
        return None
    delta = (thru - order).days
    return delta if delta >= 0 else None


def _parse_int(value: typing.Optional[str]) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _parse_decimal(value: typing.Optional[str]) -> typing.Optional[decimal.Decimal]:
    if value in (None, ""):
        return None
    try:
        return decimal.Decimal(str(value).strip())
    except decimal.InvalidOperation:
        return None


def _parse_keystone_date(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    if not value:
        return None
    try:
        return datetime.date.fromisoformat(value[:10])
    except ValueError:
        return None


def _parse_keystone_delivery_date(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    """Same as _parse_keystone_date, but treats Keystone's "unknown estimate" sentinel
    (0001-01-01, seen on LTL quotes) as no date at all, rather than leaking a garbage year-1
    date through to the FE."""
    parsed = _parse_keystone_date(value)
    return None if parsed and parsed.year == _UNKNOWN_DATE_YEAR else parsed


def _is_real_ship_option(row: typing.Dict[str, typing.Any]) -> bool:
    """
    Keystone's warehouse rate tables occasionally include a placeholder row alongside the real
    options — e.g. ``{"ShipVia": "3", "Description": "", "ServiceLevel": "U01",
    "TotalFreightCharge": "0"}`` with no ``ThruDelivery``/``FromDelivery``/``OrderDate`` at all
    (confirmed against a live quote). Distinguish this from a legitimate LTL quote, which also
    has no *usable* delivery estimate but represents that with the "unknown date" sentinel
    (``0001-01-01``, handled by ``_parse_keystone_delivery_date``) while still including a real
    ``ThruDelivery`` key — the placeholder is missing the key entirely, not just its value.
    """
    return "ThruDelivery" in row


def _billable_quantity(ship_flag: str, quantity_available: int, quantity_backordered: int) -> int:
    """
    The quantity this warehouse-split should actually be priced/charged for. "Not orderable"
    (ShipFlag X) is a hard cancellation — Keystone will never ship or bill for that quantity —
    unlike a genuine backorder (ShipFlag B), which the distributor still charges for once it
    ships. Without this distinction, ``quantity_available or quantity_backordered`` (the naive
    "whichever is nonzero" pick) would price a cancelled quantity exactly like a backordered
    one, silently inflating po.subtotal/total for units that will never actually be billed.
    """
    if ship_flag == "X":
        return 0
    return quantity_available or quantity_backordered


def _filter_options(
    options: typing.List[base.ShipOption], ship_method: typing.Optional[str]
) -> typing.List[base.ShipOption]:
    if not ship_method:
        return options
    filtered = [o for o in options if o.service_level_code == ship_method]
    return filtered or options


class KeystoneOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.KEYSTONE.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider)
        self._client = KeystoneOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider)
        )

    # -- Request building -----------------------------------------------------------------

    @staticmethod
    def _validate_vcpn(vcpn: str) -> None:
        if not VCPN_REGEX.match(vcpn or ""):
            raise order_exceptions.OrderValidationError(
                message="Invalid Keystone part number (VCPN): {!r}".format(vcpn)
            )

    def _build_part_numbers_qty(self, line_items: typing.List[base.OrderLineItemRequest]) -> str:
        """K,VCPN,QTY|K,VCPN,QTY — the search-type/part/quantity format GetShippingOptions*
        multi-part methods expect ("K" = VCPN search type, per Keystone's docs)."""
        parts = []
        for li in line_items:
            vcpn = li.provider_part.provider_external_id
            self._validate_vcpn(vcpn)
            parts.append("K,{},{}".format(vcpn, li.quantity))
        return "|".join(parts)

    def _build_part_number_quantity(self, line_items: typing.List[base.OrderLineItemRequest]) -> str:
        """VCPN,QTY|VCPN,QTY — ShipOrderDropShipMultipleParts's format (no search-type prefix;
        this function only accepts VCPNs)."""
        parts = []
        for li in line_items:
            vcpn = li.provider_part.provider_external_id
            self._validate_vcpn(vcpn)
            parts.append("{},{}".format(vcpn, li.quantity))
        return "|".join(parts)

    @staticmethod
    def _split_name(full_name: str) -> typing.Tuple[str, str]:
        parts = (full_name or "").split(None, 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        if len(parts) == 1:
            return "", parts[0]
        return "", ""

    def _build_drop_ship(self, ship_to: base.ShipToAddress) -> typing.Dict[str, str]:
        first_name, last_name = self._split_name(ship_to.name)
        return {
            "first_name": first_name,
            "last_name": last_name,
            "company": ship_to.attention or "",
            "address1": ship_to.address1,
            "address2": ship_to.address2 or "",
            "city": ship_to.city,
            "state": (ship_to.state or "").upper(),
            "postal_code": ship_to.postal_code,
            "phone": ship_to.phone or "",
            "country": (ship_to.country or "US").upper(),
            "email": ship_to.email or "",
        }

    def _handle_error(self, e: Exception) -> None:
        code = getattr(e, "code", None)
        raise order_exceptions.OrderValidationError(message=str(e), code=str(code) if code else None)

    def _get_prices(self, vcpns: typing.List[str]) -> typing.Dict[str, decimal.Decimal]:
        """
        {vcpn: CustomerPrice} via CheckPriceBulk, so quoted line items carry Keystone's own
        up-to-date price rather than only our (potentially stale) catalog-sync price — same
        role Turn14's inline quote pricing already plays for that adapter.

        Cached per (company_provider, vcpn) for _PRICE_CACHE_TTL_SECONDS (1 hour) — required,
        not just an optimization: CheckPriceBulk is the one Keystone method whose docs warn of
        account suspension for calling it more than once an hour. A quote is commonly re-run
        many times in that window (cart edits, review, requote), so only VCPNs actually missing
        from cache ever trigger a real call. Best-effort throughout: any failure here (cache or
        the call itself) degrades to no pricing for the affected VCPN(s) rather than failing the
        shipping quote — pricing is an enrichment, availability/shipping is the core result.
        """
        prices: typing.Dict[str, decimal.Decimal] = {}
        missing: typing.List[str] = []
        for vcpn in vcpns:
            cache_key = _PRICE_CACHE_KEY.format(self.company_provider.id, vcpn)
            try:
                cached = cache.get(cache_key)
            except Exception:
                logger.warning(
                    "{} Cache unavailable reading price for vcpn={}; will re-fetch live.".format(
                        _LOG_PREFIX, vcpn
                    )
                )
                cached = None
            if cached is not None:
                prices[vcpn] = cached
            else:
                missing.append(vcpn)

        for i in range(0, len(missing), _PRICE_BULK_CHUNK_SIZE):
            chunk = missing[i : i + _PRICE_BULK_CHUNK_SIZE]
            try:
                tables = self._client.check_price_bulk(",".join(chunk))
            except keystone_client_exceptions.KeystoneException:
                logger.exception(
                    "{} CheckPriceBulk failed for vcpns={}; those line(s) will have no "
                    "distributor-quoted price this round.".format(_LOG_PREFIX, chunk)
                )
                continue

            rows = next(iter(tables.values()), []) if tables else []
            for row in rows:
                vendor = (row.get("Vendor") or "").strip()
                part_number = (row.get("PartNumber") or "").strip()
                vcpn = "{}{}".format(vendor, part_number)
                price = _parse_decimal(row.get("CustomerPrice"))
                if not vcpn or price is None:
                    continue
                prices[vcpn] = price
                try:
                    cache.set(
                        _PRICE_CACHE_KEY.format(self.company_provider.id, vcpn), price, _PRICE_CACHE_TTL_SECONDS
                    )
                except Exception:
                    logger.warning(
                        "{} Cache unavailable writing price for vcpn={}; will re-fetch live "
                        "next time instead of using the cache.".format(_LOG_PREFIX, vcpn)
                    )

        return prices

    # -- DistributorOrderAdapter ------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        part_numbers_qty = self._build_part_numbers_qty(line_items)
        try:
            tables = self._client.get_shipping_options_multiple_parts_per_warehouse(
                part_numbers_qty, to_zip=ship_to.postal_code
            )
        except keystone_client_exceptions.KeystoneException as e:
            self._handle_error(e)

        logger.info("{} Quote tables: {}".format(_LOG_PREFIX, repr(list(tables.keys()))[:2000]))

        prices = self._get_prices([li.provider_part.provider_external_id for li in line_items])

        try:
            by_vcpn = {li.provider_part.provider_external_id: li for li in line_items}

            # Warehouse rate tables are dynamically named (e.g. "Warehouse_Texas_50") — group by
            # the trailing warehouse-number suffix so they can be matched back to
            # PartsQuantityPerWarehouse rows' plain "Warehouse" column.
            warehouse_options: typing.Dict[str, typing.List[base.ShipOption]] = {}
            for table_name, rows in tables.items():
                if not table_name.startswith("Warehouse_"):
                    continue
                wh_number = table_name.rsplit("_", 1)[-1]
                options = [
                    base.ShipOption(
                        service_level_code=row.get("ServiceLevel", ""),
                        service_level_name=row.get("Description", ""),
                        # ThruDelivery, not ToDelivery — that's the actual field name in
                        # Keystone's response (confirmed against a live quote).
                        estimated_delivery_date=_parse_keystone_delivery_date(row.get("ThruDelivery")),
                        days_in_transit=_days_in_transit(row.get("OrderDate"), row.get("ThruDelivery")),
                        cost=_parse_decimal(row.get("TotalFreightCharge")),
                        # No separate quote-scoped id for Keystone — ShipOrderDropShipMultipleParts
                        # takes the same ServiceLevel code shown here directly.
                        quote_option_id=row.get("ServiceLevel", ""),
                    )
                    for row in rows
                    if _is_real_ship_option(row)
                ]
                warehouse_options[wh_number] = _filter_options(options, ship_method)

            lines: typing.List[base.ShippingQuoteLine] = []
            for row in tables.get("PartsQuantityPerWarehouse", []):
                vcpn = row.get("PartID", "")
                li = by_vcpn.get(vcpn)
                ship_flag = row.get("ShipFlag", "")
                warehouse_number = row.get("Warehouse", "")
                flags = {"B": ["backorder"], "X": ["not_orderable"], "T": ["transfer"]}.get(ship_flag, [])
                quantity_available = _parse_int(row.get("QTO")) if ship_flag in ("O", "T") else 0
                # "Not orderable" rows carry their unavailable quantity in "Cancelled", not
                # "Backordered" (confirmed against a live quote: an X row has no "Backordered"
                # key at all) — read the right field per ShipFlag so the FE's "N backordered"
                # badge (driven by quantity_backordered, same as Turn14's out-of-stock shipments)
                # shows the real quantity instead of a silent 0 that looks like nothing happened.
                quantity_backordered = _parse_int(row.get("Cancelled" if ship_flag == "X" else "Backordered"))
                unit_price = prices.get(vcpn)
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=li.line_item_id if li else 0,
                        provider_external_id=vcpn,
                        quantity_available=quantity_available,
                        quantity_backordered=quantity_backordered,
                        warehouse_code=warehouse_number,
                        warehouse_name=_WAREHOUSE_NAMES.get(warehouse_number),
                        ship_options=warehouse_options.get(warehouse_number, []),
                        flags=flags,
                        # From CheckPriceBulk (see _get_prices) — Keystone's shipping-quote
                        # endpoint itself never returns pricing, unlike Turn14's.
                        distributor_unit_price=unit_price,
                        distributor_line_total=(
                            unit_price * _billable_quantity(ship_flag, quantity_available, quantity_backordered)
                            if unit_price is not None
                            else None
                        ),
                    )
                )

            # PartsData carries per-part errors (blocked / insufficient qty / case-qty multiple /
            # not found) — attach as a flag on any matching line(s), or synthesize a
            # zero-availability line so the error isn't silently dropped if no warehouse rows
            # exist for that part at all.
            for row in tables.get("PartsData", []):
                message = row.get("PartMessage", "")
                if not message or message == "OK":
                    continue
                vcpn = row.get("SearchItem", "")
                li = by_vcpn.get(vcpn)
                matched = [line for line in lines if line.provider_external_id == vcpn]
                if matched:
                    for line in matched:
                        line.flags.append(message)
                else:
                    lines.append(
                        base.ShippingQuoteLine(
                            line_item_id=li.line_item_id if li else 0,
                            provider_external_id=vcpn,
                            quantity_available=0,
                            flags=[message],
                        )
                    )

            if not lines:
                raise order_exceptions.OrderValidationError(
                    "Unexpected/empty quote response shape from Keystone. Raw tables: {}".format(
                        repr(tables)[:2000]
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected quote response shape from Keystone ({}: {}). Raw tables: {}".format(
                    type(e).__name__, e, repr(tables)[:2000]
                )
            )

        return base.ShippingQuoteResult(lines=lines, raw_response=tables)

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        part_number_quantity = self._build_part_number_quantity(line_items)
        drop_ship = self._build_drop_ship(ship_to)
        service_level = purchase_order.ship_method or ""

        try:
            tables = self._client.ship_order_dropship_multiple_parts(
                order_process_method=1,
                part_number_quantity=part_number_quantity,
                drop_ship=drop_ship,
                po_number=purchase_order.po_number,
                service_level=service_level,
            )
        except keystone_client_exceptions.KeystoneException as e:
            self._handle_error(e)

        return self._parse_submit_response(tables, purchase_order, line_items)

    @staticmethod
    def _parse_submit_response(
        tables: typing.Dict[str, typing.List[typing.Dict[str, str]]],
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
    ) -> base.DistributorOrderResult:
        try:
            status_rows = tables.get("Status", [])
            status_row = status_rows[0] if status_rows else {}
            overall_status = status_row.get("Status", "")
            overall_message = status_row.get("StatusMessage", "")

            if overall_message != "OK":
                raise order_exceptions.OrderValidationError(
                    message=overall_status or overall_message or "Order rejected by Keystone."
                )

            by_vcpn = {li.provider_part.provider_external_id: li for li in line_items}
            placements: typing.List[base.LineItemPlacement] = []
            for row in tables.get("PartResults", []):
                vcpn = row.get("VCPN", "")
                li = by_vcpn.get(vcpn)
                line_status = row.get("Status", "")
                line_message = row.get("StatusMessage", "")
                qty = _parse_int(row.get("Quantity"))
                placements.append(
                    base.LineItemPlacement(
                        line_item_id=li.line_item_id if li else 0,
                        distributor_order_number=purchase_order.po_number,
                        # A non-empty per-line Status means that line was rejected/removed —
                        # see the "New" errata rows in Keystone's docs (overall order can be OK
                        # while individual lines were dropped for stock/shipping reasons).
                        quantity_confirmed=0 if line_status else qty,
                        status_code=line_status or None,
                        status_message=line_message or None,
                    )
                )
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order response shape from Keystone ({}: {}). Raw tables: {}".format(
                    type(e).__name__, e, repr(tables)[:2000]
                )
            )

        # Keystone doesn't hand back a distinct distributor order number at submit time — order
        # lookups (GetOrderHistory) are keyed off the same po_number we submitted.
        return base.DistributorOrderResult(
            distributor_order_numbers=[purchase_order.po_number],
            line_item_placements=placements,
            raw_response=tables,
        )

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        try:
            tables = self._client.get_order_history(po_number=purchase_order.po_number)
        except keystone_client_exceptions.KeystoneException as e:
            self._handle_error(e)

        try:
            rows = self._find_order_history_rows(tables)
            usable_rows = [r for r in rows if r.get("EKORD#") not in _ORDER_HISTORY_ERROR_VALUES]

            by_po: typing.Dict[str, typing.List[typing.Dict[str, str]]] = {}
            for row in usable_rows:
                po = row.get("EKORD#", "")
                if po:
                    by_po.setdefault(po, []).append(row)

            orders: typing.List[base.DistributorOrderStatus] = []
            for po, po_rows in by_po.items():
                # A single PONumber lookup is sorted by PONumber, not chronologically — sort by
                # date/time ourselves to find the most recent transaction (RCV ORD -> ... -> INVOICE).
                po_rows.sort(key=lambda r: (r.get("EKDATE", ""), r.get("EKTIME", "")))
                latest = po_rows[-1]
                tracking_numbers = sorted({r.get("EKTRCK", "") for r in po_rows if r.get("EKTRCK")})
                latest_stat = latest.get("EKSTAT", "")
                orders.append(
                    base.DistributorOrderStatus(
                        distributor_order_number=po,
                        status_code=latest_stat,
                        tracking_numbers=tracking_numbers,
                        delivery_status=_EKSTAT_DELIVERY_STATUS.get(latest_stat),
                        # EKDATE on the latest transaction row as a ship_date proxy — Keystone
                        # gives no dedicated ship-date field (see _EKSTAT_SHIPPED_STAGES).
                        ship_date=(
                            _parse_keystone_date(latest.get("EKDATE"))
                            if latest_stat in _EKSTAT_SHIPPED_STAGES
                            else None
                        ),
                        raw_response={"rows": po_rows},
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order-status response shape from Keystone ({}: {}). Raw tables: {}".format(
                    type(e).__name__, e, repr(tables)[:2000]
                )
            )
        return base.OrderStatusResult(orders=orders)

    @staticmethod
    def _find_order_history_rows(
        tables: typing.Dict[str, typing.List[typing.Dict[str, str]]]
    ) -> typing.List[typing.Dict[str, str]]:
        """GetOrderHistory's DataSet table name isn't documented explicitly — match structurally
        (a row carrying "EKORD#") instead of by name."""
        for rows in tables.values():
            if rows and "EKORD#" in rows[0]:
                return rows
        if len(tables) == 1:
            return next(iter(tables.values()))
        return []

    def supports_invoices(self) -> bool:
        return True

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[base.DistributorInvoice]:
        """
        Keystone has no dedicated invoice endpoint — GetOrderHistory's own transaction rows
        carry an EKINV# once a line has actually invoiced, so an invoice here is synthesized by
        grouping rows by that field, not fetched as a real invoice document. This means
        invoice_date/paid_amount/amount_due (things GetOrderHistory doesn't report at all) stay
        None — see the comments field on the returned DistributorInvoice for that caveat.
        """
        try:
            tables = self._client.get_order_history(po_number=purchase_order.po_number)
        except keystone_client_exceptions.KeystoneException as e:
            self._handle_error(e)

        try:
            rows = self._find_order_history_rows(tables)
            usable_rows = [r for r in rows if r.get("EKORD#") not in _ORDER_HISTORY_ERROR_VALUES]

            by_invoice: typing.Dict[str, typing.List[typing.Dict[str, str]]] = {}
            for row in usable_rows:
                invoice_number = row.get("EKINV#", "")
                if invoice_number:
                    by_invoice.setdefault(invoice_number, []).append(row)

            invoices: typing.List[base.DistributorInvoice] = []
            for invoice_number, inv_rows in by_invoice.items():
                line_items = [
                    base.InvoiceLineItem(
                        part_number="{}{}".format(
                            (r.get("EKVEND") or "").strip(), (r.get("EKPART") or "").strip()
                        ),
                        quantity=_parse_int(r.get("EKXQTY")),
                        unit_price=_parse_decimal(r.get("EKPRIC")),
                        total_price=_parse_decimal(r.get("EKEXTD")),
                        warehouse_code=r.get("EKSWHS") or None,
                    )
                    for r in inv_rows
                ]
                line_totals = [li.total_price for li in line_items if li.total_price is not None]
                freight_values = [_parse_decimal(r.get("EKFRTD")) for r in inv_rows]
                invoices.append(
                    base.DistributorInvoice(
                        invoice_number=invoice_number,
                        distributor_order_number=inv_rows[0].get("EKORD#") or None,
                        total_price=(sum(line_totals, decimal.Decimal("0")) if line_totals else None),
                        freight=next((f for f in freight_values if f is not None), None),
                        tracking=[
                            base.InvoiceTrackingEntry(tracking_number=t)
                            for t in sorted({r.get("EKTRCK", "") for r in inv_rows if r.get("EKTRCK")})
                        ],
                        line_items=line_items,
                        comments=(
                            "Synthesized from Keystone GetOrderHistory transaction rows, not a "
                            "real invoice document — invoice_date/paid_amount/amount_due aren't "
                            "available in this feed."
                        ),
                        raw_response={"rows": inv_rows},
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order-history response shape from Keystone ({}: {}). Raw tables: {}".format(
                    type(e).__name__, e, repr(tables)[:2000]
                )
            )
        return invoices

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "Keystone's Electronic Order API does not expose a cancel-order endpoint."
        )

    def supports_cancel(self) -> bool:
        return False

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        return list(_COMMON_SHIPPING_METHODS)
