"""
RoughCountryEdiOrderAdapter — the first order adapter whose transport is files in a directory
rather than an HTTP API.

Three things make it shaped differently from Turn14/Keystone/Meyer/Premier/WheelPros, and all
three are properties of EDI rather than choices:

**Submitting does not confirm.** Writing an 850 into Rough Country's pickup folder means the
file was delivered, not that the order was accepted. Confirmation is the 855, which arrives
minutes to hours later in a different directory. So ``submit_order`` returns
``distributor_confirmed=False`` and the PO stays SUBMITTED until the inbound poller applies the
855 — the same escape hatch EmailOrderAdapter uses, for the same reason.

**There is no live availability or freight.** No 846 inventory document, no 832 price/catalog,
no synchronous stock check anywhere in the set. ``get_shipping_quote`` therefore returns no
quote lines at all (``PurchaseOrderReviewPage.vue`` already renders "Shipping calculated by
{provider} at submit time" for exactly this case) rather than synthesizing availability we do
not have. The shipping *method* picker still works, because that is a static catalogue of Rough
Country's own codes, not a priced quote — which is the distinction worth keeping: a shop can
choose FedEx Ground over NDA, and nobody learns the freight cost until the 810 arrives, after
the goods have already shipped.

**Status and invoices are reads of our own tables.** ``get_order_status`` and ``get_invoices``
cannot call anything; they report what the poller has already applied from the 856 and 810. An
empty result means "nothing has come back yet", never "nothing happened".

Credentials and the trading-partner envelope are per company — Alluvia issues every shop its own
FTP login and its own ISA/GS id — so the adapter is constructed from the order account like
every other, and raises ValueError when that account is not configured (the contract
``orders.registry.get_adapter`` relies on).

SAFETY: ``submit_order`` writes a real file into a real partner mailbox. Against a company whose
``edi_environment`` is "production" that file IS a purchase order and Rough Country will ship
goods to the address in it. It must only ever run from an explicit, user-approved submission —
never from exploratory code, tests or ad-hoc scripts. See keystone.py's matching note.
"""
import logging
import typing

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.rough_country import edi as rough_country_edi
from src.integrations.clients.rough_country import edi_config, po_850
from src.integrations.edi import control_numbers
from src.integrations.edi import exceptions as edi_exceptions
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ROUGH-COUNTRY-EDI-ORDER]"


class RoughCountryEdiOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.ROUGH_COUNTRY.value

    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
    ) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider, order_account)
        # Raises ValueError when this account has no usable EDI credentials, which
        # orders.registry.get_adapter catches and reports as "not configured yet".
        self.config = edi_config.for_account(company_provider, order_account)
        self.client = rough_country_edi.RoughCountryEdiClient(self.config)

    # -- capabilities -------------------------------------------------------------------------

    fulfillment_channel_name = "edi"

    def supports_cancel(self) -> bool:
        """No cancel path exists in the document set Alluvia gave us — no 860, and no
        confirmation that an 850 with BEG01=01 would be honoured. Until that is answered, a
        shop must phone Rough Country rather than be shown a button that does nothing."""
        return False

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        """Rough Country's own TD5-05 codes, supplied by Alluvia on 21 Oct 2026. A static
        catalogue, not a priced quote: choosing one filters how it ships, and the cost is not
        known until the invoice."""
        return [
            base.ShippingMethod(code=code, name=name, carrier_name="FedEx") for code, name in po_850.SHIP_METHOD_CODES
        ]

    def supports_invoices(self) -> bool:
        return True

    # -- quote --------------------------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        """
        Intentionally empty ``lines``. There is nothing to ask: no inventory document, no
        pricing document, no synchronous endpoint. Synthesizing one line per item would put a
        fabricated ``quantity_available`` in front of a shop and let them believe stock was
        checked, which is worse than showing nothing. ``po.subtotal`` falls back to our own
        frozen catalog pricing, the same fallback every adapter uses before it has quoted
        per-item prices — and Rough Country prices from their own system regardless (we sent
        33.22, they invoiced 33.2168).
        """
        selected = (ship_method or "").strip()
        return base.ShippingQuoteResult(
            lines=[],
            raw_response={
                "channel": "edi",
                "environment": self.config.environment,
                "ship_method": selected or None,
                "ship_method_name": po_850.SHIP_METHOD_NAMES.get(selected),
                "note": "Rough Country has no live availability, pricing or freight document. "
                "Availability is confirmed by their 855 and freight is only known from the 810, "
                "after the goods ship.",
            },
            request_payload={
                "line_items": [
                    {
                        "line_item_id": item.line_item_id,
                        "part_number": item.provider_part.provider_external_id,
                        "quantity": item.quantity,
                    }
                    for item in line_items
                ],
                "ship_method": selected or None,
            },
        )

    # -- submit -------------------------------------------------------------------------------

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        po_number = base.resolve_po_number(purchase_order)
        request = self._build_850_request(purchase_order, line_items, ship_to)
        request_payload = _describe_request(request, self.config)

        # Allocated FIRST, and deliberately not inside a transaction of ours: a control number
        # must never be recycled, even by a send that then fails. Gaps are harmless; a reused
        # ISA13 is rejected by their translator and, with no 997 coming back for our 850s, that
        # rejection is silent. See edi/control_numbers.py.
        sender = self.config.sender_id
        interchange_control_number = control_numbers.next_interchange_control_number(sender)
        group_control_number = control_numbers.next_group_control_number(sender)
        transaction_control_number = control_numbers.next_transaction_control_number(sender)

        try:
            payload = po_850.build_850(
                request=request,
                sender_id=sender,
                receiver_id=self.config.receiver_id,
                interchange_control_number=interchange_control_number,
                group_control_number=group_control_number,
                transaction_control_number=transaction_control_number,
                usage_indicator=self.config.usage_indicator,
            )
        except edi_exceptions.EdiBuildError as e:
            # A document we cannot legally encode is a business-rule failure (a PO number that
            # will not fit, a delimiter in an address), not a transport one.
            raise order_exceptions.OrderValidationError(str(e), request_payload=request_payload)

        filename = po_850.filename_for(po_number)
        document = self._record_outbound(
            purchase_order=purchase_order,
            doc_type="850",
            interchange_control_number=interchange_control_number,
            group_control_number=group_control_number,
            transaction_control_number=transaction_control_number,
            filename=filename,
            payload=payload,
            parsed_payload=request_payload,
        )

        logger.info("%s Sending 850 for PO %s (%s).", _LOG_PREFIX, po_number, self.config.describe())
        try:
            remote_path = self.client.send_document("850", filename, payload)
        except (edi_exceptions.EdiTransportError, ValueError) as e:
            document.status = src_enums.EdiDocumentStatus.ERROR.value
            document.status_name = src_enums.EdiDocumentStatus.ERROR.name
            document.error_message = str(e)[:4000]
            document.save(update_fields=["status", "status_name", "error_message", "updated_at"])
            raise order_exceptions.OrderAdapterError(
                "Could not deliver the 850 for PO {} to Rough Country: {}".format(po_number, e)
            )

        document.status = src_enums.EdiDocumentStatus.SENT.value
        document.status_name = src_enums.EdiDocumentStatus.SENT.name
        document.save(update_fields=["status", "status_name", "updated_at"])

        return base.DistributorOrderResult(
            # Rough Country's 855 carries no order number of their own — BAK03 is our own PO
            # number — so this is the only reference either side shares. The 856's BSN02 and the
            # 810's invoice number are the only RC-side ids we ever learn, and both arrive later.
            distributor_order_numbers=[po_number],
            # Nothing is confirmed yet, so no per-line placements: letting _run_submit write
            # quantity_confirmed here would show a shop confirmed stock nobody has checked.
            line_item_placements=[],
            raw_response={
                "channel": "edi",
                "environment": self.config.environment,
                "sender_id": sender,
                "receiver_id": self.config.receiver_id,
                "usage_indicator": self.config.usage_indicator,
                "interchange_control_number": interchange_control_number,
                "remote_path": remote_path,
                "filename": filename,
                "edi_document_id": document.id,
                "payload": payload,
            },
            request_payload=request_payload,
            # The 850 was delivered, not accepted. Keeps the PO at SUBMITTED until the 855.
            distributor_confirmed=False,
        )

    def _build_850_request(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> po_850.Po850Request:
        lines = []
        for position, item in enumerate(line_items, start=1):
            provider_part = item.provider_part
            master_part = provider_part.master_part if provider_part else None
            lines.append(
                po_850.Po850Line(
                    line_number=position,
                    quantity=item.quantity,
                    rough_country_part_number=provider_part.provider_external_id,
                    # Our own item code, the SK qualifier. Same precedent as the PO PDF: the
                    # master part number where we have one, else the distributor's own id.
                    our_part_number=(master_part.part_number if master_part else provider_part.provider_external_id),
                    unit_price=_line_unit_price(purchase_order, item),
                    description=(master_part.description or "") if master_part else "",
                )
            )

        ship_method = (purchase_order.ship_method or "").strip()
        if ship_method not in po_850.SHIP_METHOD_NAMES:
            # An unknown or unset method would otherwise go out in TD5-05 verbatim. Rough
            # Country's list is closed for what we SEND, so fall back rather than invent.
            if ship_method:
                logger.warning(
                    "%s PO %s requested ship method %r, which is not one of Rough Country's "
                    "codes; sending %s instead.",
                    _LOG_PREFIX,
                    purchase_order.id,
                    ship_method,
                    po_850.DEFAULT_SHIP_METHOD_CODE,
                )
            ship_method = po_850.DEFAULT_SHIP_METHOD_CODE

        return po_850.Po850Request(
            po_number=base.resolve_po_number(purchase_order),
            lines=lines,
            ship_to=po_850.Po850ShipTo(
                name=ship_to.attention or ship_to.name,
                address1=ship_to.address1,
                address2=ship_to.address2 or "",
                city=ship_to.city,
                state=ship_to.state,
                postal_code=ship_to.postal_code,
                country=ship_to.country or "US",
            ),
            contact=_order_contact(purchase_order, ship_to),
            ship_method_code=ship_method,
        )

    def _record_outbound(
        self,
        purchase_order: src_models.PurchaseOrder,
        doc_type: str,
        interchange_control_number: str,
        group_control_number: str,
        transaction_control_number: str,
        filename: str,
        payload: str,
        parsed_payload: typing.Optional[typing.Dict] = None,
    ) -> src_models.EdiDocument:
        """
        Written BEFORE the upload on purpose. The unique constraint on (company_provider,
        direction, interchange_control_number) then catches a reused control number here,
        while nothing has been sent, instead of letting the partner reject it silently later.
        """
        return src_models.EdiDocument.objects.create(
            direction=src_enums.EdiDirection.OUTBOUND.value,
            direction_name=src_enums.EdiDirection.OUTBOUND.name,
            company_provider=self.company_provider,
            partner_id=self.config.receiver_id,
            doc_type=doc_type,
            interchange_control_number=interchange_control_number,
            group_control_number=group_control_number,
            transaction_control_number=transaction_control_number,
            filename=filename,
            raw_payload=payload,
            parsed_payload=parsed_payload,
            purchase_order=purchase_order,
            status=src_enums.EdiDocumentStatus.PENDING.value,
            status_name=src_enums.EdiDocumentStatus.PENDING.name,
        )

    # -- status / invoices --------------------------------------------------------------------

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        """
        A read of what the inbound poller has already applied from the 855/856, not a live call
        — there is nothing to call. An empty tracking list means no ASN has arrived yet.
        """
        orders = []
        for row in purchase_order.distributor_orders.all():
            orders.append(
                base.DistributorOrderStatus(
                    distributor_order_number=row.distributor_order_number,
                    status_code=row.distributor_order_status_name or row.status_name,
                    tracking_numbers=list(row.tracking_numbers or []),
                    carrier=row.carrier,
                    ship_date=row.ship_date,
                    estimated_delivery_date=row.estimated_delivery_date,
                    delivery_status=row.delivery_status,
                    raw_response=row.raw_response,
                )
            )
        return base.OrderStatusResult(orders=orders)

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[base.DistributorInvoice]:
        """Invoices applied from Rough Country's 810s. Empty until one arrives, which does not
        happen until the goods have shipped."""
        invoices = []
        for row in purchase_order.invoices.all():
            invoices.append(
                base.DistributorInvoice(
                    invoice_number=row.invoice_number,
                    invoice_date=row.invoice_date,
                    distributor_order_number=row.distributor_order_number,
                    total_price=row.total_price,
                    freight=row.freight,
                    amount_due=row.amount_due,
                    line_items=[
                        base.InvoiceLineItem(
                            part_number=line.get("part_number"),
                            description=line.get("description"),
                            quantity=line.get("quantity"),
                            unit_price=line.get("unit_price"),
                            total_price=line.get("total_price"),
                        )
                        for line in (row.line_items or [])
                    ],
                    raw_response=row.raw_response,
                )
            )
        return invoices

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "Rough Country has no EDI cancellation document. Contact Rough Country customer "
            "service to cancel this order."
        )


def _line_unit_price(
    purchase_order: src_models.PurchaseOrder, item: base.OrderLineItemRequest
) -> typing.Optional[typing.Any]:
    """
    Our own frozen catalog cost for this line, for PO1-04.

    Informational: Rough Country prices from their own system and ignores what we send (we sent
    33.22, their invoice said 33.2168). Sent anyway because it is what the sample does and it
    makes a rejected order readable by a human, but nothing downstream should treat it as the
    price that will be billed.
    """
    for line_item in purchase_order.line_items.all():
        if line_item.id == item.line_item_id:
            return line_item.unit_cost
    return None


def _order_contact(
    purchase_order: src_models.PurchaseOrder, ship_to: base.ShipToAddress
) -> typing.Optional[po_850.Po850Contact]:
    """
    PER*OC is the buyer-side order contact — who to call about the order — which is a different
    person from the N1*ST consignee. Prefers whoever submitted the PO, falling back to the
    delivery contact so the segment is populated rather than omitted.
    """
    name = ""
    email = ""
    profile = purchase_order.created_by if purchase_order.created_by_id else None
    if profile is not None and profile.user_id:
        user = profile.user
        name = (user.get_full_name() or user.username or "").strip()
        email = (user.email or "").strip()
    if not name:
        name = (ship_to.attention or ship_to.name or "").strip()
    if not email:
        email = (ship_to.email or "").strip()
    if not name:
        return None
    return po_850.Po850Contact(name=name, phone=(ship_to.phone or "").strip(), email=email)


def _describe_request(request: po_850.Po850Request, config: edi_config.RoughCountryEdiConfig) -> typing.Dict:
    """The logical request, for PurchaseOrderSubmissionAttempt.request_payload. Never includes
    credentials — the password is transport-level and must not reach an audit row."""
    return {
        "po_number": request.po_number,
        "sender_id": config.sender_id,
        "receiver_id": config.receiver_id,
        "environment": config.environment,
        "usage_indicator": config.usage_indicator,
        "ship_method_code": request.ship_method_code,
        "ship_method_name": po_850.SHIP_METHOD_NAMES.get(request.ship_method_code),
        "order_type_code": request.order_type_code,
        "ship_to": {
            "name": request.ship_to.name,
            "city": request.ship_to.city,
            "state": request.ship_to.state,
            "postal_code": request.ship_to.postal_code,
            "country": request.ship_to.country,
        },
        "lines": [
            {
                "line_number": line.line_number,
                "rough_country_part_number": line.rough_country_part_number,
                "our_part_number": line.our_part_number,
                "quantity": line.quantity,
                "unit_price": str(line.unit_price) if line.unit_price is not None else None,
            }
            for line in request.lines
        ],
    }
