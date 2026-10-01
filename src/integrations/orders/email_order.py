"""
EmailOrderAdapter — a generic DistributorOrderAdapter that doesn't call any distributor API at
all. Instead, submit_order() renders the PO to PDF and emails it to a distributor rep, CC'ing an
internal address — the same manual workflow some distributors already require, automated.

Used for EVERY distributor kind, not registered per-kind in registry.py's _ADAPTERS: unlike
Turn14/Keystone/Meyer/Premier/WheelPros (one bespoke class per distributor API),
orders.registry.get_adapter() constructs this directly whenever the resolved order account's
``order_method`` is EMAIL (src.enums.OrderMethod), regardless of provider kind — see that
function's docstring for the channel-first dispatch. That's also why this class has no
``provider_kind`` class attribute: nothing in the shared submit/quote pipeline needs one for
this adapter (the two isinstance() checks in purchase_order_jobs._run_submit are for Turn14 and
Premier's distributor-assigned PO number extraction, which doesn't apply here — see base.
resolve_po_number instead).

SAFETY: submit_order() sends a REAL email to a REAL distributor rep. It must only ever be
invoked from an explicit, user-approved submission — never from exploratory/dev code, automated
tests, or ad-hoc scripts. See src/integrations/orders/keystone.py's matching note.

Credential fields (PROVIDER_CATALOG's email_order_connection_required_fields /
..._optional_fields, same list for every provider): ``rep_email`` (required — the one thing that
blocks construction, see __init__), ``cc_email``, ``reply_to_email`` and ``account_number``
(all optional). ``account_number`` is shown on the PO PDF so the distributor knows which of the
company's accounts to bill — it has nothing to do with which email account places the order.
``cc_email`` defaults to the submitting user's own email when left blank; ``reply_to_email`` then
defaults to whichever CC address results (explicit or defaulted) when it's also left blank — see
_effective_cc_and_reply_to, computed at submit time since both can depend on the PurchaseOrder's
creator. ``reply_to_email`` was renamed from ``reply_to`` (old key still read for backward
compatibility) to match the "_email" suffix every other field here uses.
"""
import datetime
import decimal
import logging
import typing

from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions
from src.integrations.services import notifications
from src.integrations.services import purchase_order_pdf

logger = logging.getLogger(__name__)


def _decimal_to_str(value: typing.Optional[decimal.Decimal]) -> typing.Optional[str]:
    """String, not float, in the PDF-rendering context — keeps the exact decimal the PO was
    quoted at (see PurchaseOrderLineItem.unit_cost/line_total, both DecimalFields) rather than
    letting float rounding drift into a document a rep will actually read."""
    return str(value) if value is not None else None


class EmailOrderAdapter(base.DistributorOrderAdapter):
    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: typing.Optional[src_models.CompanyProviderOrderAccount] = None,
    ) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider, order_account)
        creds = credentials_helper.get_order_credentials(company_provider, order_account)
        self.rep_email = (creds.get("rep_email") or "").strip()
        # Raw, un-defaulted values from the stored credentials -- CC/reply-to defaulting (CC to
        # the submitting user's own email, reply-to to the resolved CC) depends on the
        # PurchaseOrder being submitted, which isn't known yet at construction time, so it's
        # computed in submit_order() instead. See _effective_cc_and_reply_to.
        self._cc_email_raw = (creds.get("cc_email") or "").strip() or None
        # reply_to_email is the current field name (renamed from "reply_to" so it matches the
        # "_email" suffix every other email-channel field uses -- the FE's field-type inference
        # keyed off that suffix, and "reply_to" alone was rendering as a password input). Still
        # read the old key for any account saved before the rename.
        self._reply_to_raw = (
            (creds.get("reply_to_email") or creds.get("reply_to") or "").strip() or None
        )
        # Optional -- shown on the PO PDF/email so the distributor knows which of the company's
        # accounts to bill, independent of which email account placed the order.
        self.account_number = (creds.get("account_number") or "").strip() or None
        if not self.rep_email:
            raise ValueError("rep_email is required for email-based ordering.")

    @staticmethod
    def _effective_cc_and_reply_to(
        cc_email_raw: typing.Optional[str],
        reply_to_raw: typing.Optional[str],
        purchase_order: src_models.PurchaseOrder,
    ) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
        """
        CC defaults to the submitting user's own email when left blank; reply-to then defaults to
        whichever CC address results (explicit or defaulted) when IT is left blank -- matching
        the Integrations page's documented field behavior for these two fields. Both stay
        user-overridable: an explicit value in credentials always wins over the default.
        """
        cc_email = cc_email_raw
        if not cc_email and purchase_order.created_by_id and purchase_order.created_by.user_id:
            cc_email = (purchase_order.created_by.user.email or "").strip() or None
        reply_to = reply_to_raw or cc_email
        return cc_email, reply_to

    # -- Quote ------------------------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        """
        No live distributor API to ask — an intentionally empty ``lines`` list, not one
        synthesized line per item. purchase_order_jobs._run_quote already handles this
        gracefully (po.shipments ends up []), and the FE's PurchaseOrderReviewPage.vue already
        renders "Shipping calculated by {provider} at submit time" for exactly this case.
        po.subtotal falls back to our own frozen catalog unit_cost/line_total, the same fallback
        every other adapter uses before it has per-item quoted pricing.
        """
        return base.ShippingQuoteResult(
            lines=[],
            raw_response={
                "channel": "email",
                "note": "No live distributor quote available for email-based ordering; "
                "pricing/availability is confirmed by the distributor after the PO is emailed.",
            },
            distributor_total=None,
            fees=[],
            request_payload={
                "channel": "email",
                "line_items": [
                    {
                        "line_item_id": li.line_item_id,
                        "provider_external_id": li.provider_part.provider_external_id,
                        "quantity": li.quantity,
                    }
                    for li in line_items
                ],
            },
        )

    # -- Submit -----------------------------------------------------------------------------

    def _build_pdf_context(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        cc_email: typing.Optional[str],
    ) -> typing.Dict:
        po_reference = base.resolve_po_number(purchase_order)
        pdf_line_items = []
        for li in line_items:
            master_part = li.provider_part.master_part if li.provider_part else None
            pdf_line_items.append(
                {
                    "part_number": master_part.part_number if master_part else li.provider_part.provider_external_id,
                    "brand_name": master_part.brand.name if master_part and master_part.brand_id else None,
                    "description": master_part.description if master_part else None,
                    "quantity": li.quantity,
                    "unit_cost": None,
                    "line_total": None,
                }
            )
        # Line pricing comes from the PurchaseOrderLineItem rows (our own frozen catalog
        # snapshot — there's no distributor quote to prefer here, unlike a real adapter's
        # post-quote effective_line_total), matched back up by line_item_id.
        by_line_item_id = {li.id: li for li in purchase_order.line_items.all()}
        for entry, request_li in zip(pdf_line_items, line_items):
            po_li = by_line_item_id.get(request_li.line_item_id)
            if po_li is not None:
                entry["unit_cost"] = _decimal_to_str(po_li.unit_cost)
                entry["line_total"] = _decimal_to_str(po_li.line_total)

        subtotal = sum(
            (po_li.line_total for po_li in by_line_item_id.values() if po_li.line_total is not None),
            decimal.Decimal("0"),
        )

        return {
            "po_reference": po_reference,
            "company_name": self.company_provider.company.name,
            "provider_name": self.company_provider.provider.name,
            "account_number": self.account_number,
            "order_date": datetime.date.today().isoformat(),
            "ship_to": {
                "name": ship_to.name,
                "attention": ship_to.attention,
                "address1": ship_to.address1,
                "address2": ship_to.address2,
                "city": ship_to.city,
                "state": ship_to.state,
                "postal_code": ship_to.postal_code,
                "country": ship_to.country,
                "phone": ship_to.phone,
            },
            "line_items": pdf_line_items,
            "subtotal": _decimal_to_str(subtotal) if by_line_item_id else None,
            "notes": purchase_order.notes,
            "rep_email": self.rep_email,
            "cc_email": cc_email,
        }

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        po_reference = base.resolve_po_number(purchase_order)
        cc_email, reply_to = self._effective_cc_and_reply_to(
            self._cc_email_raw, self._reply_to_raw, purchase_order
        )
        pdf_context = self._build_pdf_context(purchase_order, line_items, ship_to, cc_email)

        try:
            pdf_bytes = purchase_order_pdf.render_purchase_order_pdf(pdf_context)
        except purchase_order_pdf.PurchaseOrderPdfError as e:
            raise order_exceptions.OrderValidationError(
                "Failed to render the purchase order PDF: {}".format(e),
                request_payload=pdf_context,
            )

        sent_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:
            notifications.send_purchase_order_email(
                company_provider=self.company_provider,
                purchase_order=purchase_order,
                to_email=self.rep_email,
                cc_email=cc_email,
                reply_to=reply_to,
                pdf_bytes=pdf_bytes,
                pdf_filename="PO-{}.pdf".format(po_reference),
            )
        except order_exceptions.OrderAdapterError as e:
            # notifications.send_purchase_order_email already raises OrderValidationError with
            # its own message; re-attach request_payload so a failed SUBMIT attempt still
            # records what the PDF would have contained (matches every other adapter's
            # OrderValidationError(..., request_payload=...) convention on the failure path).
            if isinstance(e, order_exceptions.OrderValidationError) and e.request_payload is None:
                e.request_payload = pdf_context
            raise

        return base.DistributorOrderResult(
            distributor_order_numbers=[po_reference],
            line_item_placements=[
                base.LineItemPlacement(
                    line_item_id=li.line_item_id,
                    distributor_order_number=po_reference,
                    quantity_confirmed=0,
                    quantity_backordered=0,
                    status_code="EMAILED",
                    status_message="Emailed to {} — awaiting confirmation.".format(self.rep_email),
                )
                for li in line_items
            ],
            raw_response={
                "channel": "email",
                "to": self.rep_email,
                "cc": cc_email,
                "reply_to": reply_to,
                "account_number": self.account_number,
                "sent_at": sent_at,
            },
            request_payload=pdf_context,
            distributor_confirmed=False,
        )

    # -- Status / cancel ----------------------------------------------------------------------

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        """Nothing to poll — there's no distributor API. Status only moves once someone
        manually updates it (or a future reply-parsing feature lands)."""
        return base.OrderStatusResult(orders=[])

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        raise order_exceptions.OrderNotSupportedError(
            "{} orders are placed by email and can't be cancelled automatically here — "
            "contact {} directly.".format(self.company_provider.provider.name, self.rep_email)
        )

    def supports_cancel(self) -> bool:
        return False

    def fulfillment_channel(self) -> str:
        return "email"
