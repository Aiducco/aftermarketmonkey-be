"""
Hourly bulk order-side sweeps for Turn 14 -- tracking, package details and invoices.

The problem this replaces
-------------------------
``confirmed_purchase_order_sync`` refreshes each confirmed purchase order individually:
``GET /v1/orders/po/{ref}`` plus ``GET /v1/invoices/po/{ref}``, per order, per cycle. Cost grows
with the number of open orders, and it never sees tracking at all except as a side effect of the
order payload.

Turn 14's model instead offers date-range collections. ``GET /v1/tracking`` with no parameters
returns everything that shipped today; ``GET /v1/invoices?start_date=&end_date=`` returns every
invoice raised in a window. So one or two requests per company per hour covers every open order
that company has, however many that is.

This does not replace ``confirmed_purchase_order_sync``. That path still owns the authoritative
per-order status refresh and the freshly-submitted window; this fills in tracking and invoices
for orders that are already out the door, which is where the per-PO polling was most wasteful.

Date-range constraint
---------------------
Turn 14 rejects tracking ranges wider than three days with a 400. :func:`tracking_date_chunks`
exists so a backfill splits correctly rather than discovering that one page at a time.
"""
import datetime
import logging
import typing

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations import rate_limit as rate_limit_base
from src.integrations.clients.turn_14 import exceptions as turn14_exceptions
from src.integrations.clients.turn_14 import order_client as turn14_order_client

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TURN-14-ORDER-SWEEPS]"

# Turn 14 returns 400 for tracking ranges wider than this.
MAX_TRACKING_RANGE_DAYS = 3

# Distributor-order states that still warrant looking for tracking and invoices. A CANCELLED
# slice will never ship, and a SHIPPED one already has everything.
_OPEN_DISTRIBUTOR_STATUSES = (
    src_enums.PurchaseOrderDistributorOrderStatus.SUBMITTED.value,
    src_enums.PurchaseOrderDistributorOrderStatus.CONFIRMED.value,
    src_enums.PurchaseOrderDistributorOrderStatus.PARTIALLY_SHIPPED.value,
)


def tracking_date_chunks(
    start: datetime.date, end: datetime.date
) -> typing.List[typing.Tuple[str, str]]:
    """Split [start, end] into ISO ranges no wider than Turn 14's three-day tracking limit."""
    chunks = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + datetime.timedelta(days=MAX_TRACKING_RANGE_DAYS - 1), end)
        chunks.append((cursor.isoformat(), chunk_end.isoformat()))
        cursor = chunk_end + datetime.timedelta(days=1)
    return chunks


def _open_distributor_orders(
    company_provider: src_models.CompanyProviders,
) -> typing.List[src_models.PurchaseOrderDistributorOrder]:
    return list(
        src_models.PurchaseOrderDistributorOrder.objects.select_related("purchase_order")
        .filter(
            purchase_order__company_provider=company_provider,
            status__in=_OPEN_DISTRIBUTOR_STATUSES,
        )
    )


def _po_reference_index(
    distributor_orders: typing.List[src_models.PurchaseOrderDistributorOrder],
) -> typing.Dict[str, src_models.PurchaseOrderDistributorOrder]:
    """
    Every reference a Turn 14 response might use to name one of our orders, mapped back to it.

    Turn 14 names the same order three different ways depending on which endpoint answered --
    the submit response's ``po_number``, the orders/po lookup's ``purchase_order_number``, and
    the ``website_order_number`` that equals the submit response's ``data.id``. Indexing all of
    them is cheaper and far less brittle than guessing which one a given payload will use.
    """
    index: typing.Dict[str, src_models.PurchaseOrderDistributorOrder] = {}
    for order in distributor_orders:
        for key in (order.po_number, order.distributor_order_number,
                    order.distributor_internal_order_number):
            if key:
                index[str(key)] = order
    return index


def _client_for(company_provider: src_models.CompanyProviders) -> typing.Optional[
    turn14_order_client.Turn14OrderApiClient
]:
    from django.conf import settings

    account = credentials_helper.get_default_order_account(company_provider)
    if not account:
        return None
    credentials = credentials_helper.get_order_credentials(company_provider, account)
    try:
        return turn14_order_client.Turn14OrderApiClient(
            credentials=credentials,
            environment=getattr(settings, "TURN14_ORDER_ENVIRONMENT", "production"),
        )
    except ValueError:
        return None


def sweep_tracking_for_company_provider(
    company_provider: src_models.CompanyProviders,
) -> int:
    """
    Pull today's tracking and attach it to whichever open orders it belongs to.

    One request covers every open order for this company. Returns the number of distributor
    orders whose tracking changed.
    """
    open_orders = _open_distributor_orders(company_provider)
    if not open_orders:
        return 0

    client = _client_for(company_provider)
    if not client:
        logger.info(
            "{} No Turn 14 order credentials for company_provider={}. Skipping.".format(
                _LOG_PREFIX, company_provider.id
            )
        )
        return 0

    index = _po_reference_index(open_orders)

    # No date parameters: Turn 14 returns everything shipped today, which is what an hourly
    # sweep wants and avoids the three-day-range restriction entirely.
    response = client.get_tracking()
    rows = response.get("data") or []
    logger.info(
        "{} company_provider={}: {} tracking row(s) today, {} open order(s).".format(
            _LOG_PREFIX, company_provider.id, len(rows), len(open_orders)
        )
    )

    touched = 0
    for row in rows:
        attributes = row.get("attributes") or {}
        reference = None
        for key in ("purchase_order_number", "po_number", "website_order_number", "order_number"):
            if attributes.get(key):
                reference = str(attributes[key])
                break
        order = index.get(reference) if reference else None
        if order is None:
            continue

        entries = attributes.get("tracking") or attributes.get("tracking_numbers") or []
        normalised = []
        for entry in entries:
            if isinstance(entry, dict):
                number = entry.get("tracking_number")
                method = entry.get("ship_method") or entry.get("method")
            else:
                number, method = entry, None
            if number:
                normalised.append({"tracking_number": str(number), "ship_method": method})
        if not normalised:
            continue

        existing = order.raw_response if isinstance(order.raw_response, dict) else {}
        # Merge rather than replace: a partially-shipped order accumulates tracking numbers
        # across several days, and today's sweep only ever sees today's.
        merged = {t["tracking_number"]: t for t in (existing.get("tracking") or []) if t.get("tracking_number")}
        before = len(merged)
        merged.update({t["tracking_number"]: t for t in normalised})
        if len(merged) == before:
            continue

        existing["tracking"] = list(merged.values())
        order.raw_response = existing
        order.save(update_fields=["raw_response", "updated_at"])
        touched += 1

    logger.info("{} company_provider={}: updated tracking on {} order(s).".format(
        _LOG_PREFIX, company_provider.id, touched
    ))
    return touched


def sweep_invoices_for_company_provider(
    company_provider: src_models.CompanyProviders,
    days_back: int = 1,
) -> int:
    """
    Pull invoices raised in the recent window and attach them to their purchase orders.

    Invoices only come into existence when items actually ship, so an order stays uninvoiced
    for a while after submission and this is what eventually resolves it. ``days_back`` covers
    the boundary where an invoice is raised just before midnight and the sweep runs just after.

    Returns the number of invoices written.
    """
    open_orders = _open_distributor_orders(company_provider)
    if not open_orders:
        return 0

    client = _client_for(company_provider)
    if not client:
        return 0

    index = _po_reference_index(open_orders)
    today = timezone.now().date()
    start = (today - datetime.timedelta(days=days_back)).isoformat()
    end = (today + datetime.timedelta(days=1)).isoformat()

    response = client.get_invoices(start_date=start, end_date=end)
    rows = response.get("data") or []
    logger.info(
        "{} company_provider={}: {} invoice(s) in {}..{}.".format(
            _LOG_PREFIX, company_provider.id, len(rows), start, end
        )
    )

    written = 0
    for row in rows:
        attributes = row.get("attributes") or {}
        reference = None
        for key in ("purchase_order_number", "po_number", "website_order_number"):
            if attributes.get(key):
                reference = str(attributes[key])
                break
        order = index.get(reference) if reference else None
        if order is None:
            continue

        invoice_number = str(row.get("id") or attributes.get("invoice_number") or "")
        if not invoice_number:
            continue

        src_models.PurchaseOrderInvoice.objects.update_or_create(
            purchase_order=order.purchase_order,
            invoice_number=invoice_number,
            defaults={
                "invoice_date": attributes.get("invoice_date"),
                "distributor_order_number": order.distributor_order_number,
                "website_order_number": attributes.get("website_order_number"),
                "total_price": attributes.get("total_price"),
                "freight": attributes.get("freight"),
                "discount_amount": attributes.get("discount_amount"),
                "paid_amount": attributes.get("paid_amount"),
                "amount_due": attributes.get("amount_due"),
                "tracking": attributes.get("tracking") or [],
                "line_items": attributes.get("line_items") or [],
                "comments": attributes.get("comments"),
                "raw_response": row,
            },
        )
        written += 1

    logger.info("{} company_provider={}: wrote {} invoice(s).".format(
        _LOG_PREFIX, company_provider.id, written
    ))
    return written


def run_order_sweeps() -> typing.Dict[str, int]:
    """
    Run the hourly tracking and invoice sweeps for every active Turn 14 connection that has
    open orders.

    A rate-budget stop for one company is not allowed to abort the rest: budgets are per
    credential set, so the next company still has its own allowance. Genuine API errors are
    likewise contained per company -- one customer's expired order credentials must not stop
    everyone else's tracking from updating.
    """
    totals = {"companies": 0, "tracking_updated": 0, "invoices_written": 0, "skipped": 0}

    connections = src_models.CompanyProviders.objects.select_related("company").filter(
        provider__kind=src_enums.BrandProviderKind.TURN_14.value,
        active=True,
    )

    for company_provider in connections:
        totals["companies"] += 1
        try:
            totals["tracking_updated"] += sweep_tracking_for_company_provider(company_provider)
            totals["invoices_written"] += sweep_invoices_for_company_provider(company_provider)
        except rate_limit_base.RateBudgetExhausted as e:
            totals["skipped"] += 1
            logger.warning("{} company_provider={} deferred: {}".format(
                _LOG_PREFIX, company_provider.id, e
            ))
        except turn14_exceptions.Turn14APIException as e:
            totals["skipped"] += 1
            logger.error("{} company_provider={} failed: {}".format(
                _LOG_PREFIX, company_provider.id, e
            ))

    logger.info("{} Sweep complete: {}".format(_LOG_PREFIX, totals))
    return totals
