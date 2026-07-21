"""
Enqueue and run PurchaseOrderJob rows (no Celery) — same claim/run/process shape as
``integration_pricing_sync_jobs.py``, but scoped to a single PurchaseOrder + operation
(quote / submit / status check / cancel) rather than a whole company-provider pricing sync.

Every distributor call is dispatched through ``src.integrations.orders.registry``, so this
module has no distributor-specific logic of its own — it only knows how to translate a
PurchaseOrder + its line items into the adapter's dataclasses and write the result back.

SAFETY: run_purchase_order_job() for a SUBMIT operation places a REAL order with the
distributor. This module does not gate that on its own — the caller (the API layer, or a
developer running the management command by hand) is responsible for only creating SUBMIT
jobs from an explicit, user-approved action. Never invoke this path speculatively.
"""
import concurrent.futures
import decimal
import logging
import time
import typing

from django.core.cache import cache
from django.db import connection, transaction
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.orders import base as order_base
from src.integrations.orders import exceptions as order_exceptions
from src.integrations.orders import registry as order_registry

# The shipping-method catalog (code -> name/carrier) is near-static reference data, not
# something that changes between quotes — cache it instead of calling the distributor's
# method-list endpoint on every single quote just to fill in names.
_SHIPPING_METHOD_NAMES_CACHE_TTL_SECONDS = 3600
_SHIPPING_METHOD_NAMES_CACHE_KEY = "po_shipping_method_names:{}"

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[PURCHASE-ORDER-JOBS]"


def enqueue_submit_job(purchase_order_id: int) -> src_models.PurchaseOrderJob:
    return _enqueue(purchase_order_id, src_enums.PurchaseOrderOperation.SUBMIT)


def enqueue_status_check_job(purchase_order_id: int) -> src_models.PurchaseOrderJob:
    return _enqueue(purchase_order_id, src_enums.PurchaseOrderOperation.STATUS_CHECK)


def enqueue_cancel_job(purchase_order_id: int) -> src_models.PurchaseOrderJob:
    return _enqueue(purchase_order_id, src_enums.PurchaseOrderOperation.CANCEL)


def _enqueue(
    purchase_order_id: int, operation: src_enums.PurchaseOrderOperation
) -> src_models.PurchaseOrderJob:
    job = src_models.PurchaseOrderJob.objects.create(
        purchase_order_id=purchase_order_id,
        operation=operation.value,
        operation_name=operation.name,
        status=src_enums.PurchaseOrderJobStatus.OPEN.value,
        status_name=src_enums.PurchaseOrderJobStatus.OPEN.name,
    )
    logger.info(
        "{} Enqueued {} job id={} for purchase_order_id={}.".format(
            _LOG_PREFIX, operation.name, job.id, purchase_order_id
        )
    )
    return job


def claim_next_open_job(
    allowed_operations: typing.Optional[typing.List[int]] = None,
) -> typing.Optional[src_models.PurchaseOrderJob]:
    """
    Atomically mark one OPEN job as RUNNING. Returns None if none available.

    ``allowed_operations``, when given, restricts which PurchaseOrderOperation values this
    claim will pick up — e.g. cron only ever claims QUOTE/STATUS_CHECK (non-mutating, safe to
    automate); SUBMIT/CANCEL are left OPEN for a human to process on demand while watching.
    """
    with transaction.atomic():
        qs = src_models.PurchaseOrderJob.objects.select_for_update(skip_locked=True).filter(
            status=src_enums.PurchaseOrderJobStatus.OPEN.value
        )
        if allowed_operations is not None:
            qs = qs.filter(operation__in=allowed_operations)
        job = qs.order_by("id").first()
        if not job:
            return None
        job.status = src_enums.PurchaseOrderJobStatus.RUNNING.value
        job.status_name = src_enums.PurchaseOrderJobStatus.RUNNING.name
        job.started_at = timezone.now()
        job.message = None
        job.error_message = None
        job.save(
            update_fields=["status", "status_name", "started_at", "message", "error_message", "updated_at"]
        )
        return job


def _ensure_po_number(po: src_models.PurchaseOrder) -> None:
    if not po.po_number:
        po.po_number = "AMS-{:06d}".format(po.id)
        po.save(update_fields=["po_number", "updated_at"])


def _ship_to_from_purchase_order(po: src_models.PurchaseOrder) -> order_base.ShipToAddress:
    missing = [
        field
        for field in ("ship_to_name", "ship_to_address1", "ship_to_city", "ship_to_state", "ship_to_postal_code", "ship_to_country")
        if not getattr(po, field)
    ]
    if missing:
        raise order_exceptions.OrderValidationError(
            "PurchaseOrder id={} is missing required ship-to field(s): {}. "
            "Review the cart (set a ship-to address) before quoting/submitting.".format(po.id, ", ".join(missing))
        )
    return order_base.ShipToAddress(
        name=po.ship_to_name,
        address1=po.ship_to_address1,
        address2=po.ship_to_address2,
        city=po.ship_to_city,
        state=po.ship_to_state,
        postal_code=po.ship_to_postal_code,
        country=po.ship_to_country,
        attention=po.ship_to_attention,
        phone=po.ship_to_phone,
    )


def _line_item_requests(po: src_models.PurchaseOrder) -> typing.List[order_base.OrderLineItemRequest]:
    line_items = list(po.line_items.select_related("provider_part").all())
    if not line_items:
        raise order_exceptions.OrderValidationError(
            "PurchaseOrder id={} has no line items.".format(po.id)
        )
    return [
        order_base.OrderLineItemRequest(
            line_item_id=li.id,
            provider_part=li.provider_part,
            quantity=li.quantity,
        )
        for li in line_items
    ]


def _record_attempt(
    po: src_models.PurchaseOrder,
    operation: src_enums.PurchaseOrderOperation,
    success: bool,
    response_payload: typing.Optional[typing.Dict] = None,
    error_message: typing.Optional[str] = None,
    duration_ms: typing.Optional[int] = None,
) -> None:
    src_models.PurchaseOrderSubmissionAttempt.objects.create(
        purchase_order=po,
        operation=operation.value,
        operation_name=operation.name,
        success=success,
        response_payload=response_payload,
        error_message=error_message[:4000] if error_message else None,
        duration_ms=duration_ms,
    )


def _get_shipping_method_names(
    adapter: order_base.DistributorOrderAdapter, company_provider_id: int
) -> typing.Dict[str, str]:
    """
    {code: name} lookup for this distributor connection, used to backfill ship_options
    entries whose name came back blank in the quote itself (Turn 14's quote response often
    omits verbose_eta, unlike GET /v1/shipping which always has a name) — so the FE never has
    to show a bare code like "Method 20" or do its own cross-call merge.

    This is a display nicety layered on top of quoting, not part of quoting itself — a broken
    cache backend must never be able to fail an otherwise-successful quote. Every cache access
    is therefore best-effort: on any cache error, skip caching and fall through to a live
    (uncached) name lookup rather than propagating the exception.
    """
    if not adapter.supports_shipping_method_selection():
        return {}

    cache_key = _SHIPPING_METHOD_NAMES_CACHE_KEY.format(company_provider_id)
    try:
        cached = cache.get(cache_key)
    except Exception:
        logger.warning(
            "{} Cache unavailable reading shipping method names for company_provider_id={}; "
            "falling back to a live (uncached) lookup.".format(_LOG_PREFIX, company_provider_id)
        )
        cached = None
    if cached is not None:
        return cached

    try:
        methods = adapter.list_shipping_methods()
    except order_exceptions.OrderAdapterError:
        logger.exception(
            "{} Failed to fetch shipping method catalog for company_provider_id={}; "
            "ship_options names may be blank for unnamed codes this run.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return {}

    names = {m.code: m.name for m in methods if m.name}
    try:
        cache.set(cache_key, names, _SHIPPING_METHOD_NAMES_CACHE_TTL_SECONDS)
    except Exception:
        logger.warning(
            "{} Cache unavailable writing shipping method names for company_provider_id={}; "
            "will re-fetch live next time instead of using the cache.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
    return names


def _run_quote(po: src_models.PurchaseOrder, adapter: order_base.DistributorOrderAdapter) -> None:
    _ensure_po_number(po)
    ship_to = _ship_to_from_purchase_order(po)
    line_items = _line_item_requests(po)

    started = time.monotonic()
    try:
        result = adapter.get_shipping_quote(line_items, ship_to, ship_method=po.ship_method)
    except order_exceptions.OrderAdapterError as e:
        _record_attempt(po, src_enums.PurchaseOrderOperation.QUOTE, False, error_message=str(e))
        raise
    duration_ms = int((time.monotonic() - started) * 1000)
    _record_attempt(
        po, src_enums.PurchaseOrderOperation.QUOTE, True, response_payload=result.raw_response, duration_ms=duration_ms
    )

    method_names = _get_shipping_method_names(adapter, po.company_provider_id)

    by_line_item_id = {li.id: li for li in po.line_items.all()}
    for quote_line in result.lines:
        li = by_line_item_id.get(quote_line.line_item_id)
        if not li:
            continue
        li.quantity_confirmed = quote_line.quantity_available
        li.quantity_backordered = quote_line.quantity_backordered
        li.manufacturer_esd = quote_line.manufacturer_esd
        li.warehouse_code = quote_line.warehouse_code
        li.ship_options = [
            {
                "code": opt.service_level_code,
                # Falls back to the distributor's method-name catalog when the quote itself
                # didn't name this option (e.g. Turn14 often omits verbose_eta) — the FE
                # should never need to show a bare code.
                "name": opt.service_level_name or method_names.get(opt.service_level_code, ""),
                # Stored as float, not Decimal: this JSON blob is for display/selection, not
                # financial calculation (those still go through unit_cost/line_total
                # DecimalFields) — a plain number round-trips through JSON cleanly, whereas
                # DjangoJSONEncoder would otherwise write Decimal out as a string.
                "cost": float(opt.cost) if opt.cost is not None else None,
                "estimated_delivery_date": (
                    opt.estimated_delivery_date.isoformat() if opt.estimated_delivery_date else None
                ),
            }
            for opt in quote_line.ship_options
        ]
        li.save(
            update_fields=[
                "quantity_confirmed",
                "quantity_backordered",
                "manufacturer_esd",
                "warehouse_code",
                "ship_options",
                "updated_at",
            ]
        )

    po.quote_raw_response = result.raw_response
    po.quoted_at = timezone.now()
    po.status = src_enums.PurchaseOrderStatus.QUOTED.value
    po.status_name = src_enums.PurchaseOrderStatus.QUOTED.name
    po.error_message = None
    _compute_totals(po)
    po.save(
        update_fields=[
            "quote_raw_response",
            "quoted_at",
            "status",
            "status_name",
            "error_message",
            "subtotal",
            "estimated_shipping",
            "total",
            "updated_at",
        ]
    )


def _compute_totals(po: src_models.PurchaseOrder) -> None:
    """
    Sets po.subtotal/estimated_shipping/total from the just-quoted line items (mutates po
    in place; caller is responsible for saving). subtotal is our own catalog pricing
    (line_total, frozen at add-to-cart time) — a distributor quote confirms availability and
    shipping, not unit price. estimated_shipping is summed once per shipment, not once per
    line: lines sharing a warehouse_code are the same shipment and quote identical
    ship_options, so summing per line would double/triple-count it. Uses po.ship_method's cost
    for a shipment when set and offered there, else that shipment's cheapest option — the same
    default submit_order() falls back to when no method was explicitly chosen.
    """
    line_items = list(po.line_items.all())
    subtotal = (
        sum((li.line_total for li in line_items if li.line_total is not None), decimal.Decimal("0"))
        if line_items
        else None
    )

    shipment_costs = {}
    for li in line_items:
        if not li.ship_options:
            continue
        shipment_key = li.warehouse_code or "line-{}".format(li.id)
        if shipment_key in shipment_costs:
            continue
        priced_options = [opt for opt in li.ship_options if opt.get("cost") is not None]
        chosen = None
        if po.ship_method:
            chosen = next((opt for opt in priced_options if opt.get("code") == po.ship_method), None)
        if chosen is None and priced_options:
            chosen = min(priced_options, key=lambda opt: opt["cost"])
        if chosen is not None:
            shipment_costs[shipment_key] = decimal.Decimal(str(chosen["cost"]))

    estimated_shipping = sum(shipment_costs.values(), decimal.Decimal("0")) if shipment_costs else None

    po.subtotal = subtotal
    po.estimated_shipping = estimated_shipping
    po.total = (subtotal or decimal.Decimal("0")) + estimated_shipping if estimated_shipping is not None else subtotal


def _run_submit(po: src_models.PurchaseOrder, adapter: order_base.DistributorOrderAdapter) -> None:
    started = time.monotonic()
    try:
        # Pre-flight (po_number/ship_to/line_items) is inside the try, not before it: it can
        # raise OrderValidationError just like the distributor call can, and if it did so
        # outside this block the PO would be left stuck at SUBMITTING with no error_message —
        # silently un-actionable instead of visibly FAILED.
        _ensure_po_number(po)
        ship_to = _ship_to_from_purchase_order(po)
        line_items = _line_item_requests(po)
        result = adapter.submit_order(po, line_items, ship_to)
    except order_exceptions.OrderAdapterError as e:
        _record_attempt(po, src_enums.PurchaseOrderOperation.SUBMIT, False, error_message=str(e))
        po.status = src_enums.PurchaseOrderStatus.FAILED.value
        po.status_name = src_enums.PurchaseOrderStatus.FAILED.name
        po.error_message = str(e)[:4000]
        po.save(update_fields=["status", "status_name", "error_message", "updated_at"])
        raise
    duration_ms = int((time.monotonic() - started) * 1000)
    _record_attempt(
        po, src_enums.PurchaseOrderOperation.SUBMIT, True, response_payload=result.raw_response, duration_ms=duration_ms
    )

    distributor_orders_by_number = {}
    for order_number in result.distributor_order_numbers:
        pdo, _ = src_models.PurchaseOrderDistributorOrder.objects.get_or_create(
            purchase_order=po,
            distributor_order_number=order_number,
            defaults={
                "status": src_enums.PurchaseOrderDistributorOrderStatus.SUBMITTED.value,
                "status_name": src_enums.PurchaseOrderDistributorOrderStatus.SUBMITTED.name,
                "raw_response": result.raw_response,
            },
        )
        distributor_orders_by_number[order_number] = pdo

    by_line_item_id = {li.id: li for li in po.line_items.all()}
    for placement in result.line_item_placements:
        li = by_line_item_id.get(placement.line_item_id)
        if not li:
            continue
        li.distributor_order = distributor_orders_by_number.get(placement.distributor_order_number)
        li.quantity_confirmed = placement.quantity_confirmed
        li.quantity_backordered = placement.quantity_backordered
        li.distributor_line_status_code = placement.status_code
        li.distributor_line_status_message = placement.status_message
        if placement.quantity_confirmed:
            li.status = src_enums.PurchaseOrderLineItemStatus.CONFIRMED.value
            li.status_name = src_enums.PurchaseOrderLineItemStatus.CONFIRMED.name
        elif placement.quantity_backordered:
            li.status = src_enums.PurchaseOrderLineItemStatus.BACKORDERED.value
            li.status_name = src_enums.PurchaseOrderLineItemStatus.BACKORDERED.name
        li.save(
            update_fields=[
                "distributor_order",
                "quantity_confirmed",
                "quantity_backordered",
                "distributor_line_status_code",
                "distributor_line_status_message",
                "status",
                "status_name",
                "updated_at",
            ]
        )

    po.status = (
        src_enums.PurchaseOrderStatus.CONFIRMED.value
        if result.distributor_order_numbers
        else src_enums.PurchaseOrderStatus.SUBMITTED.value
    )
    po.status_name = src_enums.PurchaseOrderStatus.__members__[
        "CONFIRMED" if result.distributor_order_numbers else "SUBMITTED"
    ].name
    po.submitted_at = timezone.now()
    po.error_message = None
    po.save(update_fields=["status", "status_name", "submitted_at", "error_message", "updated_at"])


def _run_status_check(po: src_models.PurchaseOrder, adapter: order_base.DistributorOrderAdapter) -> None:
    started = time.monotonic()
    try:
        result = adapter.get_order_status(po)
    except order_exceptions.OrderAdapterError as e:
        _record_attempt(po, src_enums.PurchaseOrderOperation.STATUS_CHECK, False, error_message=str(e))
        raise
    duration_ms = int((time.monotonic() - started) * 1000)
    _record_attempt(
        po,
        src_enums.PurchaseOrderOperation.STATUS_CHECK,
        True,
        response_payload={"orders": [o.raw_response for o in result.orders]},
        duration_ms=duration_ms,
    )

    for order_status in result.orders:
        src_models.PurchaseOrderDistributorOrder.objects.filter(
            purchase_order=po, distributor_order_number=order_status.distributor_order_number
        ).update(
            tracking_numbers=order_status.tracking_numbers,
            carrier=order_status.carrier,
            raw_response=order_status.raw_response,
            updated_at=timezone.now(),
        )
    # PO-level status rollup (PARTIALLY_FULFILLED / FULFILLED) intentionally left for a later
    # phase until each distributor's exact status vocabulary has been confirmed against a
    # real (user-supervised) test order — see the Purchase Orders plan's verification section.


def _run_cancel(po: src_models.PurchaseOrder, adapter: order_base.DistributorOrderAdapter) -> None:
    if not adapter.supports_cancel():
        raise order_exceptions.OrderNotSupportedError(
            "{} does not support cancelling an order via its API.".format(
                po.company_provider.provider.kind_name
            )
        )
    started = time.monotonic()
    try:
        cancelled = adapter.cancel_order(po)
    except order_exceptions.OrderAdapterError as e:
        _record_attempt(po, src_enums.PurchaseOrderOperation.CANCEL, False, error_message=str(e))
        raise
    duration_ms = int((time.monotonic() - started) * 1000)
    _record_attempt(
        po, src_enums.PurchaseOrderOperation.CANCEL, cancelled, response_payload={"cancelled": cancelled}, duration_ms=duration_ms
    )
    if cancelled:
        po.status = src_enums.PurchaseOrderStatus.CANCELLED.value
        po.status_name = src_enums.PurchaseOrderStatus.CANCELLED.name
        po.save(update_fields=["status", "status_name", "updated_at"])


_RUNNERS = {
    src_enums.PurchaseOrderOperation.QUOTE.value: _run_quote,
    src_enums.PurchaseOrderOperation.SUBMIT.value: _run_submit,
    src_enums.PurchaseOrderOperation.STATUS_CHECK.value: _run_status_check,
    src_enums.PurchaseOrderOperation.CANCEL.value: _run_cancel,
}

# Only operations that are safe to silently retry unattended: read-only (STATUS_CHECK) or
# non-mutating on the distributor's side (QUOTE). SUBMIT/CANCEL are deliberately excluded —
# see the comment at the re-queue site in run_purchase_order_job.
_AUTO_RETRYABLE_OPERATIONS = {
    src_enums.PurchaseOrderOperation.QUOTE.value,
    src_enums.PurchaseOrderOperation.STATUS_CHECK.value,
}


def _resolve_po_and_adapter(
    purchase_order_id: int,
) -> typing.Tuple[typing.Optional[src_models.PurchaseOrder], typing.Optional[order_base.DistributorOrderAdapter], typing.Optional[str]]:
    """Returns (po, adapter, error_message). error_message is set (and the other two may be
    None/partial) when resolution failed — same lookup used by both the job runner and the
    synchronous quote path so they fail the same way."""
    po = (
        src_models.PurchaseOrder.objects.select_related("company_provider__provider")
        .filter(id=purchase_order_id)
        .first()
    )
    if not po:
        return None, None, "PurchaseOrder no longer exists."

    adapter = order_registry.get_adapter(po.company_provider)
    if adapter is None:
        return po, None, order_registry.get_adapter_unavailable_reason(po.company_provider)

    return po, adapter, None


def run_quote_synchronously(purchase_order_id: int) -> src_models.PurchaseOrder:
    """
    Quoting is non-mutating on the distributor's side (Turn 14's own docs: a quote "cannot be
    viewed from the Turn 14 website until promoted to an order"), so unlike submit/cancel it
    doesn't need the job queue's deliberate-gating — it's called directly in the request/
    response cycle for fast feedback. Never raises: on failure the returned PO has
    status=FAILED and error_message set, so a multi-distributor review can keep quoting the
    other POs in the group instead of aborting the whole request.
    """
    po, adapter, error_message = _resolve_po_and_adapter(purchase_order_id)
    if error_message:
        if po:
            po.status = src_enums.PurchaseOrderStatus.FAILED.value
            po.status_name = src_enums.PurchaseOrderStatus.FAILED.name
            po.error_message = error_message
            po.save(update_fields=["status", "status_name", "error_message", "updated_at"])
        return po

    try:
        _run_quote(po, adapter)
    except Exception as e:
        logger.exception("{} Synchronous quote failed for purchase_order_id={}.".format(_LOG_PREFIX, purchase_order_id))
        po.status = src_enums.PurchaseOrderStatus.FAILED.value
        po.status_name = src_enums.PurchaseOrderStatus.FAILED.name
        po.error_message = str(e)[:4000]
        po.save(update_fields=["status", "status_name", "error_message", "updated_at"])
    return po


def run_purchase_order_job(job: src_models.PurchaseOrderJob) -> None:
    po, adapter, error_message = _resolve_po_and_adapter(job.purchase_order_id)
    if error_message:
        job.status = src_enums.PurchaseOrderJobStatus.FAILED.value
        job.status_name = src_enums.PurchaseOrderJobStatus.FAILED.name
        job.error_message = error_message
        job.completed_at = timezone.now()
        job.save(update_fields=["status", "status_name", "error_message", "completed_at", "updated_at"])
        return

    runner = _RUNNERS.get(job.operation)
    try:
        runner(po, adapter)
    except Exception as e:
        logger.exception("{} Job id={} ({}) failed.".format(_LOG_PREFIX, job.id, job.operation_name))
        job.attempt_count += 1
        job.error_message = str(e)[:4000]
        job.status = src_enums.PurchaseOrderJobStatus.FAILED.value
        job.status_name = src_enums.PurchaseOrderJobStatus.FAILED.name
        job.completed_at = timezone.now()
        job.save(
            update_fields=["status", "status_name", "attempt_count", "error_message", "completed_at", "updated_at"]
        )
        if job.operation in _AUTO_RETRYABLE_OPERATIONS and job.attempt_count < job.max_attempts:
            # Re-queue as a fresh OPEN job rather than retrying in place, so
            # claim_next_open_job's FIFO ordering keeps working and other jobs aren't
            # starved by a stuck retry. SUBMIT/CANCEL are deliberately excluded from
            # _AUTO_RETRYABLE_OPERATIONS: neither Turn 14 nor most distributors document an
            # idempotency guarantee on our po_number, so a failure that happened *after* the
            # distributor actually created/cancelled the order (e.g. a timeout on the response
            # leg) could silently place or cancel a real order a second time if auto-retried.
            # A failed SUBMIT/CANCEL job stays FAILED — a human decides the next step (for
            # SUBMIT that's normally requote_purchase_order for a fresh quote_id, then submit
            # again, never blindly resubmitting the same one).
            src_models.PurchaseOrderJob.objects.create(
                purchase_order_id=job.purchase_order_id,
                operation=job.operation,
                operation_name=job.operation_name,
                status=src_enums.PurchaseOrderJobStatus.OPEN.value,
                status_name=src_enums.PurchaseOrderJobStatus.OPEN.name,
                attempt_count=job.attempt_count,
                max_attempts=job.max_attempts,
            )
        return

    job.status = src_enums.PurchaseOrderJobStatus.COMPLETED.value
    job.status_name = src_enums.PurchaseOrderJobStatus.COMPLETED.name
    job.message = "OK"
    job.completed_at = timezone.now()
    job.save(update_fields=["status", "status_name", "message", "completed_at", "updated_at"])


def process_purchase_order_jobs(
    limit: int = 10,
    workers: int = 1,
    allowed_operations: typing.Optional[typing.List[int]] = None,
) -> int:
    """
    Claim and run up to ``limit`` OPEN PurchaseOrderJob rows. Mirrors
    integration_pricing_sync_jobs.process_pricing_sync_jobs's claim/run loop and
    ThreadPoolExecutor fan-out.

    ``allowed_operations``: see claim_next_open_job — pass e.g. [QUOTE, STATUS_CHECK] to run
    this safely on an unattended cron schedule without ever touching SUBMIT/CANCEL jobs.
    Leave None only when running by hand under supervision (this is what the management
    command's default requires an explicit --operations flag to avoid).
    """
    if workers <= 1:
        processed = 0
        while processed < limit:
            job = claim_next_open_job(allowed_operations=allowed_operations)
            if not job:
                break
            run_purchase_order_job(job)
            connection.close()
            processed += 1
        return processed

    processed_count = 0
    lock = __import__("threading").Lock()

    def _worker() -> int:
        local_count = 0
        while True:
            with lock:
                nonlocal processed_count
                if processed_count >= limit:
                    break
                processed_count += 1
            job = claim_next_open_job(allowed_operations=allowed_operations)
            if not job:
                with lock:
                    processed_count -= 1
                break
            try:
                run_purchase_order_job(job)
            finally:
                connection.close()
            local_count += 1
        return local_count

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="po_job"
    ) as executor:
        futs = [executor.submit(_worker) for _ in range(workers)]
        total = sum(f.result() for f in concurrent.futures.as_completed(futs))
    return total
