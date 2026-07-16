"""
Business logic for the Purchase Orders feature: the per-distributor "Add to PO" cart (a DRAFT
PurchaseOrder doubles as the cart — see src.models.PurchaseOrder), cart review/checkout, and
PO/job status reads. Distributor calls themselves are never made synchronously here — every
quote/submit/status/cancel operation is enqueued as a PurchaseOrderJob and processed by the
``process_purchase_order_jobs`` management command; see
``src.integrations.services.purchase_order_jobs``.
"""
import datetime
import decimal
import logging
import typing

from django.conf import settings
from django.db import IntegrityError, transaction
from django.utils import timezone as django_timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.orders import registry as order_registry
from src.integrations.services import purchase_order_jobs

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[PURCHASE-ORDERS-API]"

_REQUIRED_SHIP_TO_FIELDS = ("name", "address1", "city", "state", "postal_code", "country")


class PurchaseOrderServiceError(Exception):
    """Raised for any client-correctable error (bad input, not found, wrong state)."""
    pass


# -- Serialization ------------------------------------------------------------------------


def _decimal_to_float(value: typing.Optional[decimal.Decimal]) -> typing.Optional[float]:
    return float(value) if value is not None else None


def _serialize_line_item(li: src_models.PurchaseOrderLineItem) -> typing.Dict:
    master_part = li.provider_part.master_part
    return {
        "id": li.id,
        "provider_part_id": li.provider_part_id,
        "part_number": master_part.part_number if master_part else None,
        "brand_name": master_part.brand.name if master_part and master_part.brand_id else None,
        "description": master_part.description if master_part else None,
        "image_url": master_part.image_url if master_part else None,
        "quantity": li.quantity,
        "unit_cost": _decimal_to_float(li.unit_cost),
        "line_total": _decimal_to_float(li.line_total),
        "status": li.status,
        "status_name": li.status_name,
        "distributor_line_status_code": li.distributor_line_status_code,
        "distributor_line_status_message": li.distributor_line_status_message,
        "quantity_confirmed": li.quantity_confirmed,
        "quantity_backordered": li.quantity_backordered,
        "manufacturer_esd": li.manufacturer_esd.isoformat() if li.manufacturer_esd else None,
        "warehouse_code": li.warehouse_code,
        "ship_options": li.ship_options,
        "distributor_order_id": li.distributor_order_id,
    }


def _serialize_distributor_order(pdo: src_models.PurchaseOrderDistributorOrder) -> typing.Dict:
    return {
        "id": pdo.id,
        "distributor_order_number": pdo.distributor_order_number,
        "warehouse_code": pdo.warehouse_code,
        "status": pdo.status,
        "status_name": pdo.status_name,
        "tracking_numbers": pdo.tracking_numbers,
        "carrier": pdo.carrier,
    }


def _serialize_purchase_order(po: src_models.PurchaseOrder, include_line_items: bool = True) -> typing.Dict:
    provider = po.company_provider.provider
    result = {
        "id": po.id,
        "po_number": po.po_number,
        "status": po.status,
        "status_name": po.status_name,
        "source": po.source,
        "source_name": po.source_name,
        "company_provider_id": po.company_provider_id,
        "provider_kind_name": provider.kind_name,
        "provider_name": provider.name,
        "group_id": po.group_id,
        "ship_to": {
            "name": po.ship_to_name,
            "attention": po.ship_to_attention,
            "address1": po.ship_to_address1,
            "address2": po.ship_to_address2,
            "city": po.ship_to_city,
            "state": po.ship_to_state,
            "postal_code": po.ship_to_postal_code,
            "country": po.ship_to_country,
            "phone": po.ship_to_phone,
        }
        if po.ship_to_address1
        else None,
        "ship_method": po.ship_method,
        "subtotal": _decimal_to_float(po.subtotal),
        "estimated_shipping": _decimal_to_float(po.estimated_shipping),
        "total": _decimal_to_float(po.total),
        "error_message": po.error_message,
        "notes": po.notes,
        "quoted_at": po.quoted_at.isoformat() if po.quoted_at else None,
        "quote_is_stale": (
            _quote_is_stale(po) if po.status == src_enums.PurchaseOrderStatus.QUOTED.value else None
        ),
        "submitted_at": po.submitted_at.isoformat() if po.submitted_at else None,
        "created_at": po.created_at.isoformat(),
        "updated_at": po.updated_at.isoformat(),
        "distributor_orders": [_serialize_distributor_order(pdo) for pdo in po.distributor_orders.all()],
    }
    if include_line_items:
        result["line_items"] = [_serialize_line_item(li) for li in po.line_items.select_related(
            "provider_part__master_part__brand"
        ).all()]
        result["item_count"] = sum(li["quantity"] for li in result["line_items"])
    return result


# -- Cart -----------------------------------------------------------------------------------


def _resolve_user_profile_id(user_id: typing.Optional[int]) -> typing.Optional[int]:
    if not user_id:
        return None
    profile = src_models.UserProfile.objects.filter(user_id=user_id).first()
    return profile.id if profile else None


def _get_company_provider(company_id: int, provider_id: int) -> src_models.CompanyProviders:
    """
    Resolve this company's connection (CompanyProviders) to a distributor, from the
    distributor's public catalog id (Providers.id — what part-detail responses expose as
    provider_id, and what the FE naturally has on hand). Raises if not connected, or if the
    distributor doesn't support in-app ordering.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(company_id=company_id, provider_id=provider_id)
        .select_related("provider")
        .first()
    )
    if not cp:
        raise PurchaseOrderServiceError("This distributor isn't connected for your company.")
    if not order_registry.supports_ordering(cp.provider.kind):
        raise PurchaseOrderServiceError(
            "{} does not support in-app ordering yet.".format(cp.provider.name)
        )
    return cp


def _get_or_create_draft(
    company_id: int, user_id: typing.Optional[int], cp: src_models.CompanyProviders
) -> src_models.PurchaseOrder:
    created_by_id = _resolve_user_profile_id(user_id)
    try:
        with transaction.atomic():
            po, _created = src_models.PurchaseOrder.objects.get_or_create(
                company_id=company_id,
                company_provider=cp,
                status=src_enums.PurchaseOrderStatus.DRAFT.value,
                defaults={
                    "status_name": src_enums.PurchaseOrderStatus.DRAFT.name,
                    "source": src_enums.PurchaseOrderSource.STAFF_MANUAL.value,
                    "source_name": src_enums.PurchaseOrderSource.STAFF_MANUAL.name,
                    "created_by_id": created_by_id,
                },
            )
    except IntegrityError:
        po = src_models.PurchaseOrder.objects.get(
            company_id=company_id, company_provider=cp, status=src_enums.PurchaseOrderStatus.DRAFT.value
        )
    return po


def add_cart_item(
    company_id: int,
    user_id: typing.Optional[int],
    provider_id: int,
    master_part_id: int,
    quantity: int,
) -> typing.Dict:
    """
    ``provider_id`` and ``master_part_id`` are exactly what GET /parts/<id>/ already exposes
    per provider row (providers[].provider_id and the part's own id) — the FE never needs to
    know about internal CompanyProviders/ProviderPart row ids to add something to a cart.
    """
    if not provider_id:
        raise PurchaseOrderServiceError("provider_id is required.")
    if not master_part_id:
        raise PurchaseOrderServiceError("master_part_id is required.")
    if not isinstance(quantity, int) or quantity <= 0:
        raise PurchaseOrderServiceError("Quantity must be a positive integer.")

    cp = _get_company_provider(company_id, provider_id)

    provider_part = src_models.ProviderPart.objects.filter(
        master_part_id=master_part_id, provider_id=provider_id
    ).first()
    if not provider_part:
        raise PurchaseOrderServiceError("Part not found for this distributor.")

    po = _get_or_create_draft(company_id, user_id, cp)

    pricing = src_models.ProviderPartCompanyPricing.objects.filter(
        provider_part=provider_part, company_id=company_id
    ).first()
    unit_cost = pricing.cost if pricing else None

    with transaction.atomic():
        li = (
            src_models.PurchaseOrderLineItem.objects.select_for_update()
            .filter(purchase_order=po, provider_part=provider_part)
            .first()
        )
        if li:
            li.quantity += quantity
            if unit_cost is not None:
                li.unit_cost = unit_cost
                li.line_total = unit_cost * li.quantity
            li.save(update_fields=["quantity", "unit_cost", "line_total", "updated_at"])
        else:
            src_models.PurchaseOrderLineItem.objects.create(
                purchase_order=po,
                provider_part=provider_part,
                quantity=quantity,
                unit_cost=unit_cost,
                line_total=(unit_cost * quantity) if unit_cost is not None else None,
                status=src_enums.PurchaseOrderLineItemStatus.PENDING.value,
                status_name=src_enums.PurchaseOrderLineItemStatus.PENDING.name,
            )

    return get_cart(company_id)


def update_cart_item(company_id: int, line_item_id: int, quantity: int) -> typing.Dict:
    if not isinstance(quantity, int) or quantity <= 0:
        raise PurchaseOrderServiceError("Quantity must be a positive integer.")

    li = src_models.PurchaseOrderLineItem.objects.filter(
        id=line_item_id,
        purchase_order__company_id=company_id,
        purchase_order__status=src_enums.PurchaseOrderStatus.DRAFT.value,
    ).first()
    if not li:
        raise PurchaseOrderServiceError("Cart item not found.")

    li.quantity = quantity
    if li.unit_cost is not None:
        li.line_total = li.unit_cost * quantity
    li.save(update_fields=["quantity", "line_total", "updated_at"])
    return get_cart(company_id)


def remove_cart_item(company_id: int, line_item_id: int) -> typing.Dict:
    deleted, _ = src_models.PurchaseOrderLineItem.objects.filter(
        id=line_item_id,
        purchase_order__company_id=company_id,
        purchase_order__status=src_enums.PurchaseOrderStatus.DRAFT.value,
    ).delete()
    if not deleted:
        raise PurchaseOrderServiceError("Cart item not found.")
    return get_cart(company_id)


def get_cart(company_id: int) -> typing.Dict:
    drafts = (
        src_models.PurchaseOrder.objects.filter(
            company_id=company_id, status=src_enums.PurchaseOrderStatus.DRAFT.value
        )
        .select_related("company_provider__provider")
        .prefetch_related("line_items__provider_part__master_part__brand", "distributor_orders")
        .order_by("company_provider__provider__name")
    )
    serialized = [_serialize_purchase_order(po) for po in drafts]
    # Only carts that actually have line items count toward the badge/review flow — an empty
    # DRAFT can exist momentarily (e.g. after removing the last item) without being "active".
    active = [po for po in serialized if po["line_items"]]
    return {
        "purchase_orders": active,
        "item_count": sum(po["item_count"] for po in active),
    }


# -- Review / checkout ------------------------------------------------------------------------


def review_cart(
    company_id: int,
    user_id: typing.Optional[int],
    ship_to: typing.Dict,
    purchase_order_ids: typing.Optional[typing.List[int]] = None,
    purchase_order_id: typing.Optional[int] = None,
    group_reference: typing.Optional[str] = None,
    ship_methods: typing.Optional[typing.Dict[str, str]] = None,
    shipping_method_id: typing.Optional[str] = None,
) -> typing.Dict:
    """
    ``ship_methods``: optional {purchase_order_id (as string): shipping_method_code}. Method
    codes are distributor-specific (see GET .../shipping-methods/?company_provider_id=), so
    this is keyed per-PO rather than a single value on ``ship_to`` — a cross-distributor
    checkout can legitimately want UPS Ground from one distributor and a different carrier's
    equivalent from another. Omit a PO's entry (or the whole map) to use that distributor's
    own default/cheapest pick.

    A few aliases are accepted for convenience since they're reasonable shapes a caller might
    reach for: ``ship_to.address`` for ``ship_to.address1`` (single-line address forms are
    common), ``purchase_order_id`` (singular) for a one-element ``purchase_order_ids``, and
    ``shipping_method_id`` (singular) for a one-entry ``ship_methods`` map — the last one only
    applies when exactly one PO is being reviewed, since a single method id can't sensibly
    apply across distributors with different method-code namespaces.
    """
    ship_to = dict(ship_to or {})
    if not ship_to.get("address1") and ship_to.get("address"):
        ship_to["address1"] = ship_to["address"]

    missing = [f for f in _REQUIRED_SHIP_TO_FIELDS if not ship_to.get(f)]
    if missing:
        raise PurchaseOrderServiceError("Missing required ship-to field(s): {}.".format(", ".join(missing)))

    if not purchase_order_ids and purchase_order_id:
        purchase_order_ids = [purchase_order_id]

    ship_methods = dict(ship_methods or {})
    if shipping_method_id and not ship_methods and purchase_order_ids and len(purchase_order_ids) == 1:
        ship_methods[str(purchase_order_ids[0])] = shipping_method_id

    qs = src_models.PurchaseOrder.objects.filter(
        company_id=company_id, status=src_enums.PurchaseOrderStatus.DRAFT.value
    ).filter(line_items__isnull=False)
    if purchase_order_ids:
        qs = qs.filter(id__in=purchase_order_ids)
    purchase_orders = list(qs.distinct())
    if not purchase_orders:
        raise PurchaseOrderServiceError("No cart items to review.")

    created_by_id = _resolve_user_profile_id(user_id)
    group = src_models.PurchaseOrderGroup.objects.create(
        company_id=company_id, created_by_id=created_by_id, reference=group_reference
    )

    # Quoting is called synchronously (see run_quote_synchronously's docstring for why this is
    # safe to do inline, unlike submit) — for a single-distributor cart this is one real HTTP
    # round-trip to that distributor; for a multi-distributor group it's currently sequential
    # (one distributor at a time). Revisit with a thread pool if/when that latency matters —
    # not worth the complexity while Turn14 is the only live adapter.
    results = []
    for po in purchase_orders:
        po.group = group
        po.ship_to_name = ship_to["name"]
        po.ship_to_attention = ship_to.get("attention")
        po.ship_to_address1 = ship_to["address1"]
        po.ship_to_address2 = ship_to.get("address2")
        po.ship_to_city = ship_to["city"]
        po.ship_to_state = ship_to["state"]
        po.ship_to_postal_code = ship_to["postal_code"]
        po.ship_to_country = ship_to["country"]
        po.ship_to_phone = ship_to.get("phone")
        po.ship_method = ship_methods.get(str(po.id))
        po.save(
            update_fields=[
                "group",
                "ship_to_name",
                "ship_to_attention",
                "ship_to_address1",
                "ship_to_address2",
                "ship_to_city",
                "ship_to_state",
                "ship_to_postal_code",
                "ship_to_country",
                "ship_to_phone",
                "ship_method",
                "updated_at",
            ]
        )
        quoted_po = purchase_order_jobs.run_quote_synchronously(po.id)
        results.append(_serialize_purchase_order(quoted_po))

    return {"group_id": group.id, "purchase_orders": results}


# -- PO / group reads -----------------------------------------------------------------------


def get_purchase_order_detail(company_id: int, purchase_order_id: int) -> typing.Dict:
    po = (
        src_models.PurchaseOrder.objects.filter(id=purchase_order_id, company_id=company_id)
        .select_related("company_provider__provider")
        .prefetch_related("line_items__provider_part__master_part__brand", "distributor_orders")
        .first()
    )
    if not po:
        raise PurchaseOrderServiceError("Purchase order not found.")
    return _serialize_purchase_order(po)


def _parse_status_filter(value: typing.Optional[typing.Union[int, str]]) -> typing.Optional[int]:
    """
    Accepts either the numeric status code or the status name (e.g. "CONFIRMED", matching the
    status_name field every PO/line-item/job response already returns) — case-insensitive.
    Every serialized response exposes status_name, so filtering by that same string is the
    natural thing for a caller to do; this must not 500 just because it isn't an int.
    """
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.lstrip("-").isdigit():
        return int(text)
    try:
        return src_enums.PurchaseOrderStatus[text.upper()].value
    except KeyError:
        valid = ", ".join(m.name for m in src_enums.PurchaseOrderStatus)
        raise PurchaseOrderServiceError(
            "Unknown status '{}'. Valid values: {} (or their numeric codes).".format(value, valid)
        )


def list_purchase_orders(
    company_id: int,
    status: typing.Optional[typing.Union[int, str]] = None,
    company_provider_id: typing.Optional[int] = None,
) -> typing.List[typing.Dict]:
    status_value = _parse_status_filter(status)
    qs = (
        src_models.PurchaseOrder.objects.filter(company_id=company_id)
        .exclude(status=src_enums.PurchaseOrderStatus.DRAFT.value)
        .select_related("company_provider__provider")
        .prefetch_related("distributor_orders")
        .order_by("-created_at")
    )
    if status_value is not None:
        qs = qs.filter(status=status_value)
    if company_provider_id is not None:
        qs = qs.filter(company_provider_id=company_provider_id)
    return [_serialize_purchase_order(po, include_line_items=False) for po in qs[:200]]


def get_purchase_order_group_detail(company_id: int, group_id: int) -> typing.Dict:
    group = src_models.PurchaseOrderGroup.objects.filter(id=group_id, company_id=company_id).first()
    if not group:
        raise PurchaseOrderServiceError("Purchase order group not found.")
    pos = (
        group.purchase_orders.select_related("company_provider__provider")
        .prefetch_related("line_items__provider_part__master_part__brand", "distributor_orders")
        .all()
    )
    return {
        "id": group.id,
        "reference": group.reference,
        "created_at": group.created_at.isoformat(),
        "purchase_orders": [_serialize_purchase_order(po) for po in pos],
    }


# -- Submit / cancel / status -----------------------------------------------------------------


def _quote_is_stale(po: src_models.PurchaseOrder) -> bool:
    if not po.quoted_at:
        return True
    ttl = datetime.timedelta(minutes=settings.PURCHASE_ORDER_QUOTE_TTL_MINUTES)
    return django_timezone.now() - po.quoted_at > ttl


def submit_purchase_order(
    company_id: int, purchase_order_id: int, ship_method: typing.Optional[str] = None
) -> typing.Dict:
    """
    ``ship_method``, when given, is applied to the PO right before submitting — this is how
    the user's shipping choice, made *after* seeing the quote's real priced options (a quote
    call returns every available method's live price/ETA, not just one), reaches the
    distributor. Leave unset to keep whatever was set at review time (or the distributor's
    default/cheapest, if nothing was ever set).
    """
    po = src_models.PurchaseOrder.objects.filter(id=purchase_order_id, company_id=company_id).first()
    if not po:
        raise PurchaseOrderServiceError("Purchase order not found.")
    if ship_method is not None:
        po.ship_method = ship_method
        po.save(update_fields=["ship_method", "updated_at"])
    if po.status != src_enums.PurchaseOrderStatus.QUOTED.value:
        raise PurchaseOrderServiceError(
            "Purchase order must be quoted before it can be submitted (current status: {}).".format(
                po.status_name
            )
        )
    if _quote_is_stale(po):
        raise PurchaseOrderServiceError(
            "This quote is more than {} minute(s) old and may no longer reflect current "
            "price/availability. Re-quote before submitting.".format(
                settings.PURCHASE_ORDER_QUOTE_TTL_MINUTES
            )
        )
    po.status = src_enums.PurchaseOrderStatus.SUBMITTING.value
    po.status_name = src_enums.PurchaseOrderStatus.SUBMITTING.name
    po.save(update_fields=["status", "status_name", "updated_at"])
    job = purchase_order_jobs.enqueue_submit_job(po.id)
    return {"purchase_order_id": po.id, "job_id": job.id}


def requote_purchase_order(company_id: int, purchase_order_id: int) -> typing.Dict:
    """Re-runs the quote for an already-reviewed PO (stale QUOTED, or a FAILED quote attempt)
    without rebuilding the cart — reuses the ship-to/ship-method already stored on it."""
    po = src_models.PurchaseOrder.objects.filter(id=purchase_order_id, company_id=company_id).first()
    if not po:
        raise PurchaseOrderServiceError("Purchase order not found.")
    requotable = {
        src_enums.PurchaseOrderStatus.QUOTED.value,
        src_enums.PurchaseOrderStatus.FAILED.value,
    }
    if po.status not in requotable:
        raise PurchaseOrderServiceError(
            "Purchase order can only be re-quoted from QUOTED or FAILED (current status: {}).".format(
                po.status_name
            )
        )
    if not po.ship_to_address1:
        raise PurchaseOrderServiceError("Purchase order has no ship-to address on file yet — review the cart first.")
    quoted_po = purchase_order_jobs.run_quote_synchronously(po.id)
    return _serialize_purchase_order(quoted_po)


def submit_purchase_order_group(
    company_id: int, group_id: int, ship_methods: typing.Optional[typing.Dict[str, str]] = None
) -> typing.Dict:
    """``ship_methods``: optional {purchase_order_id (as string): shipping_method_code} —
    same per-PO keying as review_cart, since each distributor has its own method-code
    namespace."""
    group = src_models.PurchaseOrderGroup.objects.filter(id=group_id, company_id=company_id).first()
    if not group:
        raise PurchaseOrderServiceError("Purchase order group not found.")
    quoted_ids = list(
        group.purchase_orders.filter(status=src_enums.PurchaseOrderStatus.QUOTED.value).values_list("id", flat=True)
    )
    if not quoted_ids:
        raise PurchaseOrderServiceError("No quoted purchase orders in this group to submit.")
    ship_methods = ship_methods or {}
    results = [
        submit_purchase_order(company_id, po_id, ship_method=ship_methods.get(str(po_id)))
        for po_id in quoted_ids
    ]
    return {"group_id": group_id, "purchase_orders": results}


def get_job_status(company_id: int, purchase_order_id: int, job_id: int) -> typing.Dict:
    job = src_models.PurchaseOrderJob.objects.filter(
        id=job_id, purchase_order_id=purchase_order_id, purchase_order__company_id=company_id
    ).first()
    if not job:
        raise PurchaseOrderServiceError("Job not found.")
    return {
        "id": job.id,
        "purchase_order_id": job.purchase_order_id,
        "operation": job.operation,
        "operation_name": job.operation_name,
        "status": job.status,
        "status_name": job.status_name,
        "message": job.message,
        "error_message": job.error_message,
        "attempt_count": job.attempt_count,
        "created_at": job.created_at.isoformat(),
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


def cancel_purchase_order(company_id: int, purchase_order_id: int) -> typing.Dict:
    po = src_models.PurchaseOrder.objects.filter(id=purchase_order_id, company_id=company_id).first()
    if not po:
        raise PurchaseOrderServiceError("Purchase order not found.")
    terminal = {
        src_enums.PurchaseOrderStatus.CANCELLED.value,
        src_enums.PurchaseOrderStatus.FAILED.value,
        src_enums.PurchaseOrderStatus.FULFILLED.value,
    }
    if po.status in terminal:
        raise PurchaseOrderServiceError("Purchase order is already {}.".format(po.status_name.lower()))
    job = purchase_order_jobs.enqueue_cancel_job(po.id)
    return {"purchase_order_id": po.id, "job_id": job.id}


def refresh_purchase_order_status(company_id: int, purchase_order_id: int) -> typing.Dict:
    po = src_models.PurchaseOrder.objects.filter(id=purchase_order_id, company_id=company_id).first()
    if not po:
        raise PurchaseOrderServiceError("Purchase order not found.")
    job = purchase_order_jobs.enqueue_status_check_job(po.id)
    return {"purchase_order_id": po.id, "job_id": job.id}


# -- Capabilities ---------------------------------------------------------------------------


def get_order_capabilities(company_id: int) -> typing.List[typing.Dict]:
    company_providers = src_models.CompanyProviders.objects.filter(
        company_id=company_id, active=True
    ).select_related("provider")
    results = []
    for cp in company_providers:
        can_order = order_registry.supports_ordering(cp.provider.kind)
        supports_shipping_selection = False
        supports_cancel = False
        if can_order:
            adapter = order_registry.get_adapter(cp)
            supports_shipping_selection = bool(adapter and adapter.supports_shipping_method_selection())
            supports_cancel = bool(adapter and adapter.supports_cancel())
        results.append(
            {
                "company_provider_id": cp.id,
                "provider_id": cp.provider_id,
                "provider_kind_name": cp.provider.kind_name,
                "provider_name": cp.provider.name,
                "can_order_in_app": can_order,
                "supports_shipping_method_selection": supports_shipping_selection,
                "supports_cancel": supports_cancel,
            }
        )
    return results


def get_shipping_methods(company_id: int, company_provider_id: int) -> typing.List[typing.Dict]:
    cp = (
        src_models.CompanyProviders.objects.filter(id=company_provider_id, company_id=company_id)
        .select_related("provider")
        .first()
    )
    if not cp:
        raise PurchaseOrderServiceError("Distributor connection not found for this company.")
    adapter = order_registry.get_adapter(cp)
    if not adapter or not adapter.supports_shipping_method_selection():
        raise PurchaseOrderServiceError("{} does not support choosing a shipping method.".format(cp.provider.name))
    return [
        {"code": m.code, "name": m.name, "carrier_name": m.carrier_name}
        for m in adapter.list_shipping_methods()
    ]
