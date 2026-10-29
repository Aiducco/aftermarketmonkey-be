"""
Applies Rough Country's inbound EDI documents to PurchaseOrder rows.

This is the half of the integration that has no equivalent in any API distributor. Turn14 or
Keystone answer ``submit_order`` with confirmed quantities, so ``purchase_order_jobs._run_submit``
writes them on the spot. Rough Country answers hours later, in three separate documents, in a
directory we have to poll — so everything _run_submit does synchronously happens here instead.

Four constraints shape the whole module:

**Delivery is at-least-once.** A document is archived only after this work commits, so a crash
re-presents the same file on the next sweep. Every apply is therefore idempotent and the first
thing :func:`record_inbound` does is dedupe on the interchange control number.

**There is no line number to match on.** Neither the 856's ``LIN`` nor the 810's ``IT1`` carries
one — ``LIN01`` is a Rough Country internal id joinable to nothing of ours. So matching is by
part number, and one part number can legitimately appear on two lines of a PO: ``ProviderPart``
is unique on ``(master_part, provider)`` and NOT on ``(provider, provider_external_id)``, so two
duplicate master parts each get their own ProviderPart pointing at the same Rough Country SKU.
(Kit expansion, which the plan doc blamed for this, cannot cause it — PurchaseOrderLineItem is
unique on ``(purchase_order, provider_part)``.) Quantities are therefore aggregated by part
number and distributed across matching lines rather than assumed one-to-one.

**The 855 only ever accepts.** Alluvia confirmed on 21 Oct 2026 that they send one acknowledgment
and it acknowledges the order. So EDI never tells us a line was short-shipped: that is only
visible by comparing the 856's ``SN1`` quantities against what we ordered, which
:func:`_apply_856` does.

**A document that matches no PO must never be dropped.** It is stored with status UNMATCHED and
reported. The common cause is an acknowledgment arriving for a PO number we no longer recognise,
which is a real problem to investigate, not a file to delete.
"""
import decimal
import logging
import typing

from django.db import IntegrityError, transaction
from django.db.models import Q

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.rough_country import ack_997
from src.integrations.clients.rough_country import edi as rough_country_edi
from src.integrations.clients.rough_country import edi_config, inbound
from src.integrations.edi import control_numbers
from src.integrations.edi import exceptions as edi_exceptions
from src.integrations.edi import x12

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ROUGH-COUNTRY-EDI-APPLY]"

_TWO_PLACES = decimal.Decimal("0.01")

# PO statuses an inbound document is allowed to advance. A PO that a human has cancelled, or
# that is already fulfilled, is never moved backwards by a late-arriving document.
_ADVANCEABLE_STATUSES = {
    src_enums.PurchaseOrderStatus.QUOTED.value,
    src_enums.PurchaseOrderStatus.SUBMITTING.value,
    src_enums.PurchaseOrderStatus.SUBMITTED.value,
    src_enums.PurchaseOrderStatus.CONFIRMED.value,
    src_enums.PurchaseOrderStatus.PARTIALLY_FULFILLED.value,
}


# -- recording -------------------------------------------------------------------------------


def record_inbound(
    company_provider: src_models.CompanyProviders,
    doc_type: str,
    payload: str,
    filename: typing.Optional[str] = None,
) -> typing.Tuple[typing.Optional[src_models.EdiDocument], bool]:
    """
    Store one received interchange, returning ``(document, is_new)``.

    ``is_new`` False means we have seen this interchange control number from this company
    before and the caller must not apply it again — the dedup that makes at-least-once delivery
    safe. An unparseable payload is still stored, with status ERROR, so it reaches a human
    instead of vanishing; that returns ``is_new`` True with no parsed payload.
    """
    try:
        interchange = x12.parse_interchange(payload)
    except edi_exceptions.EdiParseError as e:
        logger.error("%s %s did not parse: %s", _LOG_PREFIX, filename, e)
        document = src_models.EdiDocument.objects.create(
            direction=src_enums.EdiDirection.INBOUND.value,
            direction_name=src_enums.EdiDirection.INBOUND.name,
            company_provider=company_provider,
            partner_id="",
            doc_type=doc_type,
            # No ISA to read a control number from; the filename is the only handle we have.
            interchange_control_number="unparseable:{}".format(filename or "")[:32],
            filename=filename,
            raw_payload=payload,
            status=src_enums.EdiDocumentStatus.ERROR.value,
            status_name=src_enums.EdiDocumentStatus.ERROR.name,
            error_message=str(e)[:4000],
        )
        return document, True

    transactions = interchange.transactions()
    first = transactions[0] if transactions else None
    try:
        with transaction.atomic():
            document = src_models.EdiDocument.objects.create(
                direction=src_enums.EdiDirection.INBOUND.value,
                direction_name=src_enums.EdiDirection.INBOUND.name,
                company_provider=company_provider,
                partner_id=interchange.sender_id,
                doc_type=(first.transaction_set_code if first else doc_type),
                interchange_control_number=interchange.control_number,
                group_control_number=(interchange.groups[0].control_number if interchange.groups else None),
                transaction_control_number=(first.control_number if first else None),
                filename=filename,
                raw_payload=payload,
                status=src_enums.EdiDocumentStatus.RECEIVED.value,
                status_name=src_enums.EdiDocumentStatus.RECEIVED.name,
            )
    except IntegrityError:
        existing = src_models.EdiDocument.objects.filter(
            company_provider=company_provider,
            direction=src_enums.EdiDirection.INBOUND.value,
            interchange_control_number=interchange.control_number,
        ).first()
        logger.info(
            "%s Already have interchange %s for company_provider_id=%s; skipping.",
            _LOG_PREFIX,
            interchange.control_number,
            company_provider.id,
        )
        return existing, False

    return document, True


def match_purchase_order(
    company_provider: src_models.CompanyProviders, po_number: str
) -> typing.Optional[src_models.PurchaseOrder]:
    """
    Find the PO a document names, scoped to this company.

    Matched on ``po_name`` as well as ``po_number`` because ``base.resolve_po_number`` sends
    whichever of the two the shop supplied, and that is the value Rough Country echoes back.
    Scoped by company so one shop's acknowledgment can never land on another's order.
    """
    reference = (po_number or "").strip()
    if not reference:
        return None
    return (
        src_models.PurchaseOrder.objects.filter(company_provider=company_provider)
        .filter(Q(po_name=reference) | Q(po_number=reference))
        .order_by("-id")
        .first()
    )


# -- applying --------------------------------------------------------------------------------


def apply_document(document: src_models.EdiDocument) -> bool:
    """
    Parse and apply one stored inbound document. Returns True when it is safe to archive the
    remote file — which includes the UNMATCHED case: re-collecting a document we cannot match
    forever would mean re-alerting forever, and the payload is already stored.

    Returns False only when the document could not be parsed or applied at all, so the file is
    left in /Outbound for the next sweep.
    """
    if document is None:
        return False
    if document.status == src_enums.EdiDocumentStatus.ERROR.value:
        return False

    try:
        interchange = x12.parse_interchange(document.raw_payload)
        transaction_sets = interchange.transactions()
    except edi_exceptions.EdiParseError as e:
        _mark(document, src_enums.EdiDocumentStatus.ERROR, str(e))
        return False
    if not transaction_sets:
        _mark(document, src_enums.EdiDocumentStatus.ERROR, "Interchange carries no transaction sets.")
        return False

    handlers = {
        "855": _apply_855,
        "856": _apply_856,
        "810": _apply_810,
    }
    unknown = sorted({t.transaction_set_code for t in transaction_sets if t.transaction_set_code not in handlers})
    if unknown:
        _mark(
            document,
            src_enums.EdiDocumentStatus.ERROR,
            "No handler for transaction set(s) {}.".format(", ".join(unknown)),
        )
        return False

    # EVERY transaction set, not just the first. Rough Country batches acknowledgments: on
    # 29 Oct 2026 one interchange carried two 855s (GE*2), one per order, and reading only
    # transactions()[0] silently discarded the second -- PO 165's acknowledgment arrived and was
    # thrown away, leaving a real customer order unconfirmed with nothing to indicate why.
    #
    # One atomic block for the whole interchange: either all of it applies or none does, so a
    # failure half way through cannot leave a document marked applied with work missing. The
    # file is re-collected and re-applied, which is safe because each apply is idempotent.
    # Each handler returns (matched PurchaseOrder or None, parsed payload, the PO number it named).
    results: typing.List[typing.Tuple[typing.Optional[src_models.PurchaseOrder], typing.Dict, str]] = []
    try:
        with transaction.atomic():
            for transaction_set in transaction_sets:
                results.append(handlers[transaction_set.transaction_set_code](document, transaction_set))
    except Exception as e:
        logger.exception("%s Failed applying document id=%s.", _LOG_PREFIX, document.id)
        _mark(document, src_enums.EdiDocumentStatus.ERROR, str(e))
        return False

    payloads = [parsed for _po, parsed, _ref in results]
    document.parsed_payload = payloads[0] if len(payloads) == 1 else payloads
    # The FK holds one PO; an interchange can name several. First match keeps the common
    # single-transaction case useful, and parsed_payload carries every reference either way.
    document.purchase_order = next((po for po, _p, _ref in results if po is not None), None)
    document.save(update_fields=["parsed_payload", "purchase_order", "updated_at"])

    unmatched_refs = [ref for po, _p, ref in results if po is None]
    if unmatched_refs:
        logger.error(
            "%s Document id=%s named %s purchase order(s) we do not have: %s. Stored as UNMATCHED.",
            _LOG_PREFIX,
            document.id,
            len(unmatched_refs),
            ", ".join(repr(r) for r in unmatched_refs),
        )
        _mark(
            document,
            src_enums.EdiDocumentStatus.UNMATCHED,
            "No purchase order matches {}.".format(", ".join(repr(r) for r in unmatched_refs)),
        )
    else:
        _mark(document, src_enums.EdiDocumentStatus.APPLIED)
    return True


def _mark(
    document: src_models.EdiDocument,
    status: src_enums.EdiDocumentStatus,
    error_message: typing.Optional[str] = None,
) -> None:
    document.status = status.value
    document.status_name = status.name
    if error_message is not None:
        document.error_message = error_message[:4000]
    document.save(update_fields=["status", "status_name", "error_message", "updated_at"])


def _distributor_order(
    purchase_order: src_models.PurchaseOrder, order_number: str
) -> src_models.PurchaseOrderDistributorOrder:
    row, _ = src_models.PurchaseOrderDistributorOrder.objects.get_or_create(
        purchase_order=purchase_order,
        distributor_order_number=order_number,
        defaults={
            "status": src_enums.PurchaseOrderDistributorOrderStatus.SUBMITTED.value,
            "status_name": src_enums.PurchaseOrderDistributorOrderStatus.SUBMITTED.name,
        },
    )
    return row


def _lines_by_part_number(
    purchase_order: src_models.PurchaseOrder,
) -> typing.Dict[str, typing.List[src_models.PurchaseOrderLineItem]]:
    """
    PO line items grouped by the Rough Country part number they were ordered under.

    A list per part number, not one line, because nothing inbound carries a line number and two
    duplicate master parts can each own a ProviderPart for the same Rough Country SKU (see the
    module docstring). Ordered by id so distribution across them is deterministic.
    """
    grouped: typing.Dict[str, typing.List[src_models.PurchaseOrderLineItem]] = {}
    queryset = purchase_order.line_items.select_related("provider_part").order_by("id")
    for line_item in queryset:
        if line_item.provider_part_id is None:
            continue
        # Keyed on Rough Country's own SKU, because that is what their 855/856/810 echo back.
        # provider_external_id is "{rc_brand_id}_{sku}" and would never match.
        key = rough_country_edi.rough_country_sku(line_item.provider_part).upper()
        grouped.setdefault(key, []).append(line_item)
    return grouped


def _distribute(quantity: int, line_items: typing.List[src_models.PurchaseOrderLineItem]):
    """
    Spread an aggregated quantity across the PO lines carrying that part number, filling each
    line up to what it ordered before moving to the next.

    Yields ``(line_item, allocated)``. This is the only honest way to apply a document that
    reports a part number and a quantity but no line reference: for the common one-line-per-SKU
    case it is exactly the reported quantity, and for a duplicated SKU it at least never
    credits one line with another's goods.
    """
    remaining = quantity
    for line_item in line_items:
        if remaining <= 0:
            yield line_item, 0
            continue
        allocated = min(remaining, line_item.quantity)
        remaining -= allocated
        yield line_item, allocated


def _set_line_status(
    line_item: src_models.PurchaseOrderLineItem, status: src_enums.PurchaseOrderLineItemStatus
) -> None:
    line_item.status = status.value
    line_item.status_name = status.name


def _advance_po(purchase_order: src_models.PurchaseOrder, status: src_enums.PurchaseOrderStatus) -> None:
    """Move a PO forward, never backwards, and never out of a terminal state a human set."""
    if purchase_order.status not in _ADVANCEABLE_STATUSES:
        logger.info(
            "%s PO %s is %s; leaving it rather than moving it to %s.",
            _LOG_PREFIX,
            purchase_order.id,
            purchase_order.status_name,
            status.name,
        )
        return
    purchase_order.status = status.value
    purchase_order.status_name = status.name
    purchase_order.save(update_fields=["status", "status_name", "updated_at"])


# -- 855 -------------------------------------------------------------------------------------


def _apply_855(
    document: src_models.EdiDocument, transaction_set: x12.Transaction
) -> typing.Tuple[typing.Optional[src_models.PurchaseOrder], typing.Dict, str]:
    ack = inbound.parse_855(transaction_set)
    purchase_order = match_purchase_order(document.company_provider, ack.po_number)
    parsed = {
        "po_number": ack.po_number,
        "ack_type_code": ack.ack_type_code,
        "ack_date": ack.ack_date.isoformat() if ack.ack_date else None,
        "lines": [
            {
                "line_number": line.line_number,
                "rough_country_part_number": line.rough_country_part_number,
                "our_part_number": line.our_part_number,
                "quantity_ordered": line.quantity_ordered,
                "quantity_acknowledged": line.quantity_acknowledged,
                "ack_code": line.ack_code,
            }
            for line in ack.lines
        ],
    }
    if purchase_order is None:
        return None, parsed, ack.po_number

    distributor_order = _distributor_order(purchase_order, ack.po_number)
    grouped = _lines_by_part_number(purchase_order)

    accepted_total = 0
    rejected_total = 0
    for ack_line in ack.lines:
        key = (ack_line.rough_country_part_number or "").strip().upper()
        line_items = grouped.get(key)
        if not line_items:
            logger.warning(
                "%s 855 for PO %s acknowledges part %r, which is not on the order.",
                _LOG_PREFIX,
                purchase_order.id,
                ack_line.rough_country_part_number,
            )
            continue
        quantity = ack_line.quantity_acknowledged or 0
        for line_item, allocated in _distribute(quantity, line_items):
            line_item.distributor_order = distributor_order
            line_item.distributor_line_status_code = ack_line.ack_code or None
            line_item.quantity_confirmed = allocated
            # Everything ordered but not acknowledged is backordered as far as we can tell:
            # their 855 never reports a short, so the shortfall is all we have to go on.
            line_item.quantity_backordered = max(line_item.quantity - allocated, 0)
            if ack_line.is_rejected:
                _set_line_status(line_item, src_enums.PurchaseOrderLineItemStatus.REJECTED)
                line_item.distributor_line_status_message = "Rejected by Rough Country."
                rejected_total += line_item.quantity
            elif allocated <= 0:
                _set_line_status(line_item, src_enums.PurchaseOrderLineItemStatus.BACKORDERED)
            elif allocated < line_item.quantity:
                _set_line_status(line_item, src_enums.PurchaseOrderLineItemStatus.BACKORDERED)
                accepted_total += allocated
            else:
                _set_line_status(line_item, src_enums.PurchaseOrderLineItemStatus.CONFIRMED)
                accepted_total += allocated
            line_item.save(
                update_fields=[
                    "distributor_order",
                    "distributor_line_status_code",
                    "distributor_line_status_message",
                    "quantity_confirmed",
                    "quantity_backordered",
                    "status",
                    "status_name",
                    "updated_at",
                ]
            )

    distributor_order.status = src_enums.PurchaseOrderDistributorOrderStatus.CONFIRMED.value
    distributor_order.status_name = src_enums.PurchaseOrderDistributorOrderStatus.CONFIRMED.name
    distributor_order.raw_response = parsed
    distributor_order.save(update_fields=["status", "status_name", "raw_response", "updated_at"])

    if accepted_total or not rejected_total:
        _advance_po(purchase_order, src_enums.PurchaseOrderStatus.CONFIRMED)
    else:
        # Every line rejected. FAILED rather than CONFIRMED so it surfaces as needing attention.
        purchase_order.status = src_enums.PurchaseOrderStatus.FAILED.value
        purchase_order.status_name = src_enums.PurchaseOrderStatus.FAILED.name
        purchase_order.error_message = "Rough Country rejected every line on this order."
        purchase_order.save(update_fields=["status", "status_name", "error_message", "updated_at"])
    return purchase_order, parsed, ack.po_number


# -- 856 -------------------------------------------------------------------------------------


def _apply_856(
    document: src_models.EdiDocument, transaction_set: x12.Transaction
) -> typing.Tuple[typing.Optional[src_models.PurchaseOrder], typing.Dict, str]:
    asn = inbound.parse_856(transaction_set)
    purchase_order = match_purchase_order(document.company_provider, asn.po_number)
    parsed = {
        "shipment_id": asn.shipment_id,
        "po_number": asn.po_number,
        "ship_date": asn.ship_date.isoformat() if asn.ship_date else None,
        "carrier_code": asn.carrier_code,
        "warehouse_code": asn.warehouse_code,
        "warehouse_name": asn.warehouse_name,
        "tracking_numbers": list(asn.tracking_numbers),
        "items": [
            {
                "rough_country_part_number": item.rough_country_part_number,
                "our_part_number": item.our_part_number,
                "quantity_shipped": item.quantity_shipped,
                "tracking_numbers": list(item.tracking_numbers),
            }
            for item in asn.items
        ],
    }
    if purchase_order is None:
        return None, parsed, asn.po_number

    distributor_order = _distributor_order(purchase_order, asn.po_number)
    # Union rather than replace: a multi-shipment order produces several ASNs and each carries
    # only its own packages, so overwriting would lose the earlier shipment's tracking.
    existing_tracking = list(distributor_order.tracking_numbers or [])
    for tracking in asn.tracking_numbers:
        if tracking not in existing_tracking:
            existing_tracking.append(tracking)
    distributor_order.tracking_numbers = existing_tracking
    # Rough Country's own code (their test ASN said "99", which is on none of the lists they
    # have published). Stored verbatim, never mapped to a closed set.
    distributor_order.carrier = asn.carrier_code or distributor_order.carrier
    distributor_order.ship_date = asn.ship_date or distributor_order.ship_date
    distributor_order.warehouse_code = asn.warehouse_code or distributor_order.warehouse_code
    distributor_order.distributor_internal_order_number = (
        asn.shipment_id or distributor_order.distributor_internal_order_number
    )
    distributor_order.delivery_status = "in_transit"

    grouped = _lines_by_part_number(purchase_order)
    shipped_by_part: typing.Dict[str, int] = {}
    for item in asn.items:
        key = (item.rough_country_part_number or "").strip().upper()
        shipped_by_part[key] = shipped_by_part.get(key, 0) + (item.quantity_shipped or 0)

    for key, quantity in shipped_by_part.items():
        line_items = grouped.get(key)
        if not line_items:
            logger.warning(
                "%s 856 for PO %s ships part %r, which is not on the order.",
                _LOG_PREFIX,
                purchase_order.id,
                key,
            )
            continue
        for line_item, allocated in _distribute(quantity, line_items):
            if allocated <= 0:
                continue
            line_item.distributor_order = distributor_order
            if allocated >= line_item.quantity:
                _set_line_status(line_item, src_enums.PurchaseOrderLineItemStatus.SHIPPED)
            else:
                _set_line_status(line_item, src_enums.PurchaseOrderLineItemStatus.PARTIALLY_SHIPPED)
            line_item.save(update_fields=["distributor_order", "status", "status_name", "updated_at"])

    all_line_items = list(purchase_order.line_items.all())
    fully_shipped = all_line_items and all(
        li.status == src_enums.PurchaseOrderLineItemStatus.SHIPPED.value for li in all_line_items
    )
    distributor_order.status = (
        src_enums.PurchaseOrderDistributorOrderStatus.SHIPPED.value
        if fully_shipped
        else src_enums.PurchaseOrderDistributorOrderStatus.PARTIALLY_SHIPPED.value
    )
    distributor_order.status_name = src_enums.PurchaseOrderDistributorOrderStatus(distributor_order.status).name
    distributor_order.save()

    _advance_po(
        purchase_order,
        src_enums.PurchaseOrderStatus.FULFILLED if fully_shipped else src_enums.PurchaseOrderStatus.PARTIALLY_FULFILLED,
    )
    return purchase_order, parsed, asn.po_number


# -- 810 -------------------------------------------------------------------------------------


def _apply_810(
    document: src_models.EdiDocument, transaction_set: x12.Transaction
) -> typing.Tuple[typing.Optional[src_models.PurchaseOrder], typing.Dict, str]:
    invoice = inbound.parse_810(transaction_set)
    purchase_order = match_purchase_order(document.company_provider, invoice.po_number)
    parsed = {
        "invoice_number": invoice.invoice_number,
        "invoice_date": invoice.invoice_date.isoformat() if invoice.invoice_date else None,
        "po_number": invoice.po_number,
        "net_days": invoice.net_days,
        "carrier_code": invoice.carrier_code,
        "carrier_name": invoice.carrier_name,
        "total_amount": str(invoice.total_amount) if invoice.total_amount is not None else None,
        "freight": str(invoice.freight) if invoice.freight is not None else None,
        "line_subtotal": str(invoice.line_subtotal),
        "lines": [_invoice_line_json(line) for line in invoice.lines],
    }
    if purchase_order is None:
        return None, parsed, invoice.po_number

    if not invoice.invoice_number:
        raise ValueError("810 for {} carries no invoice number (BIG02).".format(invoice.po_number))

    # Keyed on (purchase_order, invoice_number) so a replayed 810 updates rather than duplicates.
    row, created = src_models.PurchaseOrderInvoice.objects.get_or_create(
        purchase_order=purchase_order,
        invoice_number=invoice.invoice_number,
        defaults={"distributor_order_number": invoice.po_number},
    )
    row.invoice_date = invoice.invoice_date
    row.distributor_order_number = invoice.po_number
    row.total_price = _money(invoice.total_amount)
    row.freight = _money(invoice.freight)
    # amount_due is deliberately left alone. TDS01 is defined as the total including charges,
    # but the one real invoice we have excluded its freight entirely and carried none, so
    # whether total + freight or total alone is owed is genuinely unknown. Showing a confident
    # wrong number on a bill is worse than showing none.
    row.line_items = parsed["lines"]
    row.raw_response = parsed
    row.save()

    logger.info(
        "%s %s invoice %s on PO %s (total %s, freight %s).",
        _LOG_PREFIX,
        "Created" if created else "Updated",
        invoice.invoice_number,
        purchase_order.id,
        invoice.total_amount,
        invoice.freight,
    )
    return purchase_order, parsed, invoice.po_number


def _invoice_line_json(line: inbound.Invoice810Line) -> typing.Dict:
    """Unit price kept as a STRING. Rough Country invoices to four decimal places (33.2168) and
    PurchaseOrderInvoice's own columns are 2dp, so the full-precision figure survives only
    here; coercing it to float would also reintroduce the rounding we are trying to preserve."""
    total = None
    if line.quantity is not None and line.unit_price is not None:
        total = str((decimal.Decimal(line.quantity) * line.unit_price))
    return {
        "part_number": line.rough_country_part_number,
        "our_part_number": line.our_part_number,
        "description": line.description,
        "quantity": line.quantity,
        "unit_price": str(line.unit_price) if line.unit_price is not None else None,
        "total_price": total,
    }


def _money(value: typing.Optional[decimal.Decimal]) -> typing.Optional[decimal.Decimal]:
    """For the 2dp DecimalFields on PurchaseOrderInvoice. Only totals go through here — never a
    unit price, which keeps its four places in the JSON above."""
    if value is None:
        return None
    return value.quantize(_TWO_PLACES, rounding=decimal.ROUND_HALF_UP)


# -- sweep -----------------------------------------------------------------------------------


def process_account(
    account: src_models.CompanyProviderOrderAccount,
    document_types: typing.Optional[typing.Sequence[str]] = None,
    limit: typing.Optional[int] = None,
    send_acknowledgments: bool = True,
) -> typing.List[typing.Dict]:
    """
    Collect, apply and acknowledge every document waiting in one company's mailbox.

    The remote file is archived only when :func:`apply_document` reports success, so a failure
    leaves it for the next run. 997s are sent after the document has been applied, never
    before: acknowledging a document we then failed to process would tell Rough Country we had
    it handled.
    """
    client = rough_country_edi.RoughCountryEdiClient.for_account(account.company_provider, account)
    company_provider = account.company_provider

    def handler(doc_type: str, filename: str, payload: str) -> bool:
        document, is_new = record_inbound(company_provider, doc_type, payload, filename)
        if not is_new:
            # Seen before: safe to archive, nothing to apply.
            return True
        if not apply_document(document):
            return False
        if send_acknowledgments:
            _send_997(client, document)
        return True

    return client.process_waiting(handler, document_types=document_types, limit=limit)


def _send_997(client: rough_country_edi.RoughCountryEdiClient, document: src_models.EdiDocument) -> None:
    """
    Acknowledge one received interchange. Best-effort by design: a 997 is a courtesy to Rough
    Country, and failing to send one must never undo an 855 we have already applied or cause
    the document to be collected again.
    """
    try:
        interchange = x12.parse_interchange(document.raw_payload)
        group = interchange.groups[0]
        sender = client.config.sender_id
        payload = ack_997.build_997(
            group_functional_id_code=group.functional_id_code,
            group_control_number=group.control_number,
            transactions=[(t.transaction_set_code, t.control_number) for t in group.transactions],
            sender_id=sender,
            receiver_id=client.config.receiver_id,
            interchange_control_number=control_numbers.next_interchange_control_number(sender),
            our_group_control_number=control_numbers.next_group_control_number(sender),
            transaction_control_number=control_numbers.next_transaction_control_number(sender),
            usage_indicator=client.config.usage_indicator,
        )
        client.send_document("997", ack_997.filename_for(interchange.control_number), payload)
    except Exception:
        logger.exception(
            "%s Could not send a 997 for document id=%s. The document itself is applied; only "
            "the acknowledgment is missing.",
            _LOG_PREFIX,
            document.id,
        )


def process_all(
    document_types: typing.Optional[typing.Sequence[str]] = None,
    limit: typing.Optional[int] = None,
    send_acknowledgments: bool = True,
) -> typing.Dict[int, typing.List[typing.Dict]]:
    """Every configured Rough Country mailbox, keyed by company_provider_id. One company's
    failure never stops the sweep."""
    results: typing.Dict[int, typing.List[typing.Dict]] = {}
    for account in edi_config.configured_accounts():
        try:
            results[account.company_provider_id] = process_account(
                account,
                document_types=document_types,
                limit=limit,
                send_acknowledgments=send_acknowledgments,
            )
        except Exception as e:
            logger.exception(
                "%s Sweep failed for company_provider_id=%s.",
                _LOG_PREFIX,
                account.company_provider_id,
            )
            results[account.company_provider_id] = [{"error": str(e)}]
    return results
