"""
KeystoneOrderHistoryReader -- discovers Keystone orders we did not place, via the existing
``get_order_history(po_number, from_date, to_date)`` SOAP call (already implemented on
KeystoneOrderApiClient; no new client method needed -- see research-be.md's Files-to-change
table). Called with ``po_number=""`` and a date range, which Keystone's own docs document as a
valid "list everything in this window" call (PONumber alone is what narrows it to one order;
blank + a date range is the discovery case this feature needs).

VERIFIED LIVE from the production host on 2026-11-03 (this dev machine's IP is not on
Keystone's allowlist -- it gets the SOAP fault "*** Illegal use of this web service !!! ***").
676 transaction rows across Trident's two accounts, Aug-Nov, confirmed all 16 documented
columns and three things the vendor SDK does not say:

  - DELIVRD is a real EKSTAT value. The SDK's status lists stop at INVOICE/CANCEL.
  - EKSVIA returns values 4 and 5; the SDK documents only 2/3/6/7/8/L.
  - EKTRCK is populated on PACKAGE, OUT4DLV and DELIVRD rows -- never on INVOICE.
"""
import datetime
import decimal
import logging
import typing

from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.keystone import exceptions as keystone_client_exceptions
from src.integrations.clients.keystone.order_client import KeystoneOrderApiClient
from src.integrations.orders import history
from src.integrations.orders import keystone as keystone_orders

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[KEYSTONE-ORDER-HISTORY]"

# Same as orders.keystone._ORDER_HISTORY_ERROR_VALUES -- GetOrderHistory rows whose EKORD# holds
# one of these mean "no matching order", not a real one.
_ORDER_HISTORY_ERROR_VALUES = {"NoData", "BadDate", "BadInDt"}

# EKORD# values that are Keystone's own internal charges rather than a purchase order anybody
# placed. SUBFEE is a recurring subscription fee: measured over Aug-Nov on Trident's two
# accounts it carries 16 rows under four different EKKEY# values, i.e. it is a standing bucket,
# not an order. It reached the purchase-orders screen as "order SUBFEE, total $800.00" before
# this filter existed.
_NON_ORDER_PO_VALUES = {"SUBFEE"}


def _to_decimal(value: typing.Optional[str]) -> typing.Optional[decimal.Decimal]:
    if not value:
        return None
    try:
        return decimal.Decimal(value)
    except decimal.InvalidOperation:
        return None


def _to_int(value: typing.Optional[str]) -> typing.Optional[int]:
    if not value:
        return None
    try:
        return int(decimal.Decimal(value))
    except (decimal.InvalidOperation, ValueError):
        return None


def _parse_ekdate(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    """EKDATE is YYYYMMDD, no separators (same format GetOrderHistory's own FromDate/ToDate
    params take) -- confirmed against orders.keystone._decode_order_history_timestamp, which
    parses the identical field for the write-side adapter's own order-detail view."""
    if not value or len(value) != 8:
        return None
    try:
        return datetime.date(int(value[0:4]), int(value[4:6]), int(value[6:8]))
    except ValueError:
        return None


def _parse_ekdatetime(
    date_value: typing.Optional[str], time_value: typing.Optional[str]
) -> typing.Optional[datetime.datetime]:
    """EKDATE (YYYYMMDD) + EKTIME (HHMMSS) into one naive timestamp. The SDK notes EKTIME loses
    its leading zero before 10 AM, so it is padded back to six digits first. Keystone states no
    timezone anywhere, so this is left naive rather than asserting UTC it did not promise."""
    date_part = _parse_ekdate(date_value)
    if date_part is None:
        return None
    raw_time = (time_value or "").strip().zfill(6)
    try:
        return datetime.datetime(
            date_part.year, date_part.month, date_part.day, int(raw_time[0:2]), int(raw_time[2:4]), int(raw_time[4:6])
        )
    except (ValueError, IndexError):
        return datetime.datetime(date_part.year, date_part.month, date_part.day)


def _normalize_po(value: typing.Optional[str]) -> str:
    """EKORD# comes back with a stray leading "#" on some rows (12 of them across Trident's two
    accounts, Aug-Nov, all for one PO). Stripping it is what lets that order dedup against our
    own PurchaseOrder instead of looking like a separate external one."""
    po = (value or "").strip()
    if po.startswith("#"):
        po = po[1:].strip()
    return po


def _group_rows_by_order(
    rows: typing.List[typing.Dict[str, str]]
) -> typing.List[typing.Tuple[str, str, typing.List[typing.Dict[str, str]]]]:
    """Returns [(distributor_order_number, customer_po_number, rows)] -- one entry per real
    Keystone order.

    GetOrderHistory returns one row per (line item, status transition) pair. Two different
    identifiers repeat across those rows and they are NOT interchangeable:

      - EKORD# is the PONumber that was sent at order placement -- OUR number, not Keystone's.
      - EKKEY# is "KAO internal order number" (vendor SDK p.66), which is Keystone's own.

    Storing EKORD# as the distributor order number made every Keystone row show an Order # equal
    to its own Customer PO, and made dedup compare our PO against our PO -- 19 of Trident's own
    orders were classed external because of it.

    Grouping is by EKORD# first because that is the one identifier present on EVERY row, then
    split by EKKEY# within it: one PO genuinely can become several Keystone orders when the
    lines ship from different warehouses (6 such cases over Aug-Nov, e.g. TRU106930 -> 547167
    and 547120), and each deserves its own row.

    EKKEY# is "0" until Keystone assigns a number, which is exactly the RCV ORD rows. If an
    EKORD# has no assigned key at all it is emitted under its PO number so a just-received order
    is still visible; once ANY key exists the still-unassigned rows are left out rather than
    emitted as a phantom third order -- they are an earlier snapshot of lines the assigned orders
    already carry, and the next poll picks them up under their own key."""
    by_po: typing.Dict[str, typing.List[typing.Dict[str, str]]] = {}
    for row in rows:
        po = _normalize_po(row.get("EKORD#"))
        if not po or po in _ORDER_HISTORY_ERROR_VALUES or po.upper() in _NON_ORDER_PO_VALUES:
            continue
        by_po.setdefault(po, []).append(row)

    groups: typing.List[typing.Tuple[str, str, typing.List[typing.Dict[str, str]]]] = []
    for po, po_rows in by_po.items():
        assigned: typing.Dict[str, typing.List[typing.Dict[str, str]]] = {}
        unassigned: typing.List[typing.Dict[str, str]] = []
        for row in po_rows:
            key = (row.get("EKKEY#") or "").strip()
            if key and key != "0":
                assigned.setdefault(key, []).append(row)
            else:
                unassigned.append(row)
        if assigned:
            groups.extend((key, po, key_rows) for key, key_rows in assigned.items())
        elif unassigned:
            groups.append((po, po, unassigned))
    return groups


def _build_order_result(
    distributor_order_number: str, customer_po_number: str, rows: typing.List[typing.Dict[str, str]]
) -> history.ExternalOrderResult:
    # Most recent row's EKSTAT (last in Keystone's own chronological order -- GetOrderHistory
    # rows for one line item are already in stage-progression order per the vendor's own docs)
    # is this order's current raw status; unlike Turn14/Meyer, Keystone has no single
    # order-level status field, so this is a best-effort "most advanced stage seen" rather than
    # a literal field read.
    # Decoded through the write-side module's own decode_status, so "PACKAGE" reads the same
    # here as it does on an order we placed ourselves.
    status = None
    for row in reversed(rows):
        if row.get("EKSTAT"):
            status = keystone_orders.decode_status(row["EKSTAT"].strip())
            break

    # One row per (line item, status transition), so the SAME line repeats once for every stage
    # it has passed through -- and the vendor SDK warns about exactly this: "PONumber may yield
    # multiple data rows for the same part ... developers should assure that quantities of unique
    # items are merged" (p.66). Summing rows naively counted each line once per transition and
    # inflated every Keystone total by its stage count: TRU107302, one part at $1,267.85 that had
    # reached DELIVRD through 7 stages, was stored as $8,899.95.
    #
    # Collapsed on (part, warehouse) rather than part alone, because a line genuinely can ship
    # from two warehouses as two separate lines. Last occurrence wins: the rows arrive in stage
    # order, so the latest is the most advanced view of that line.
    merged: typing.Dict[typing.Tuple[str, typing.Optional[str]], typing.Dict] = {}
    for row in rows:
        unit_price = _to_decimal(row.get("EKPRIC"))
        line_total = _to_decimal(row.get("EKEXTD"))
        warehouse_code = row.get("EKSWHS") or None
        vend = row.get("EKVEND", "")
        part = row.get("EKPART", "")
        part_number = "{}-{}".format(vend, part) if vend or part else None
        merged[(part_number or "", warehouse_code)] = {
            "part_number": part_number,
            "description": None,
            "quantity": _to_int(row.get("EKXQTY")),
            "unit_price": float(unit_price) if unit_price is not None else None,
            "line_total": float(line_total) if line_total is not None else None,
            "warehouse_code": warehouse_code,
            "warehouse_name": keystone_orders.decode_warehouse((warehouse_code or "").strip() or None),
        }
    line_items = list(merged.values())

    # Invoice numbers repeat on every row of an invoiced line; "0"/blank means not invoiced yet.
    # A list because one order can invoice more than once when its lines ship from two warehouses.
    invoice_numbers = []
    for row in rows:
        inv = str(row.get("EKINV#") or "").strip()
        if inv and inv != "0" and inv not in invoice_numbers:
            invoice_numbers.append(inv)

    # Tracking rides on PACKAGE/OUT4DLV/DELIVRD rows and never on INVOICE (measured live), so
    # this sweeps every row rather than reading the most advanced one.
    tracking = []
    seen_tracking = set()
    for row in rows:
        number = (row.get("EKTRCK") or "").strip()
        if not number or number in seen_tracking:
            continue
        seen_tracking.add(number)
        tracking.append(
            {
                "tracking_number": number,
                "carrier": keystone_orders.decode_ship_method((row.get("EKSVIA") or "").strip() or None),
            }
        )

    # Raw EKSVIA always; the label only when the SDK publishes one for that code. Codes 4 and 5
    # come back live and are documented nowhere, so they stay unlabelled rather than guessed.
    ship_method_code = None
    for row in reversed(rows):
        if (row.get("EKSVIA") or "").strip():
            ship_method_code = row["EKSVIA"].strip()
            break

    # One entry per distinct stage, oldest first. Keystone repeats a stage once per line item, so
    # the same status at the same timestamp collapses to one entry.
    status_history = []
    seen_stage = set()
    for row in sorted(rows, key=lambda r: ((r.get("EKDATE") or ""), (r.get("EKTIME") or "").zfill(6))):
        stage = (row.get("EKSTAT") or "").strip()
        if not stage:
            continue
        occurred_at = _parse_ekdatetime(row.get("EKDATE"), row.get("EKTIME"))
        marker = (stage, occurred_at.isoformat() if occurred_at else None)
        if marker in seen_stage:
            continue
        seen_stage.add(marker)
        status_history.append(
            {
                "status": keystone_orders.decode_status(stage),
                "occurred_at": occurred_at.isoformat() if occurred_at else None,
            }
        )

    line_totals = [li["line_total"] for li in line_items if li["line_total"] is not None]
    subtotal = round(sum(line_totals), 2) if line_totals else None
    # EKFRTD is 0.00 on an order's early rows and only carries a real charge once Keystone has
    # rated the shipment, so the FIRST row is the one row guaranteed not to have it: TRU107302
    # invoiced $25.00 freight and was stored as 0.00. Largest value across the order's rows,
    # which is also right for a split shipment -- the per-transaction figure is the order's
    # freight repeated, not a per-line share to be summed.
    freight_values = [value for value in (_to_decimal(row.get("EKFRTD")) for row in rows) if value is not None]
    freight = max(freight_values) if freight_values else None
    total = (
        decimal.Decimal(str(subtotal)) + freight
        if subtotal is not None and freight is not None
        else (decimal.Decimal(str(subtotal)) if subtotal is not None else None)
    )

    order_date = None
    for row in rows:
        order_date = _parse_ekdate(row.get("EKDATE"))
        if order_date:
            break

    return history.ExternalOrderResult(
        distributor_order_number=distributor_order_number,
        customer_po_number=customer_po_number,
        order_date=order_date,
        status=status,
        # GetOrderHistory carries no name, street, city, postal code or phone, and the Electronic
        # Order Web Service has no order-detail call to go and fetch one (every method in the SDK
        # was reviewed). For a Keystone order we did not place, the recipient is simply not
        # knowable -- the UI says "not reported by this distributor" rather than showing a blank.
        ship_to=None,
        subtotal=decimal.Decimal(str(subtotal)) if subtotal is not None else None,
        freight=freight,
        total=total,
        line_items=line_items,
        raw_response={"rows": rows},
        distributor_invoice_numbers=invoice_numbers,
        tracking=tracking,
        ship_method_code=ship_method_code,
        ship_method=keystone_orders.decode_ship_method(ship_method_code),
        status_history=status_history,
    )


class KeystoneOrderHistoryReader(history.OrderHistoryReader):
    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: src_models.CompanyProviderOrderAccount,
    ) -> None:
        history.OrderHistoryReader.__init__(self, company_provider, order_account)
        self._client = KeystoneOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider, order_account)
        )

    def list_orders(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> typing.List[history.ExternalOrderResult]:
        try:
            tables = self._client.get_order_history(
                po_number="",
                from_date=start_date.strftime("%Y%m%d"),
                to_date=end_date.strftime("%Y%m%d"),
            )
        except keystone_client_exceptions.KeystoneException as e:
            logger.error(
                "{} get_order_history failed for company_provider_id={} order_account_id={}: {}".format(
                    _LOG_PREFIX, self.company_provider.id, self.order_account.id, e
                )
            )
            raise
        # _call_dataset returns {table_name: [rows]} -- GetOrderHistory's rows live under
        # whichever single table name the response's DataSet actually used (see
        # orders.keystone's own "a row carrying EKORD#" lookup for the identical situation).
        rows: typing.List[typing.Dict[str, str]] = []
        for table_rows in tables.values():
            if table_rows and "EKORD#" in table_rows[0]:
                rows = table_rows
                break
        return [
            _build_order_result(distributor_order_number, customer_po_number, order_rows)
            for distributor_order_number, customer_po_number, order_rows in _group_rows_by_order(rows)
        ]
