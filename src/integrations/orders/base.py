"""
Common interface every distributor order adapter implements (Turn14, Keystone, Premier,
Meyer — the four distributors whose APIs support placing an order). Distributors without an
adapter registered in ``registry.py`` are treated as "quote-only via redirect link"; nothing
here is invoked for them.

Kept separate from ``src/integrations/clients/`` and ``src/integrations/services/`` (the
existing read-only catalog-sync layer) because order APIs use entirely different transports
per distributor (SOAP vs REST, different auth schemes) and are a distinct, write-capable
capability layered on top.
"""
import abc
import dataclasses
import datetime
import decimal
import typing

from src import models as src_models


@dataclasses.dataclass
class ShipToAddress:
    name: str
    address1: str
    city: str
    state: str
    postal_code: str
    country: str
    address2: typing.Optional[str] = None
    attention: typing.Optional[str] = None
    phone: typing.Optional[str] = None
    email: typing.Optional[str] = None


@dataclasses.dataclass
class OrderLineItemRequest:
    """One line item to quote/order, keyed to our own PurchaseOrderLineItem."""
    line_item_id: int
    provider_part: src_models.ProviderPart
    quantity: int
    additional_info: typing.Optional[str] = None


@dataclasses.dataclass
class ShipOption:
    service_level_code: str
    service_level_name: str
    estimated_delivery_date: typing.Optional[datetime.date] = None
    cost: typing.Optional[decimal.Decimal] = None
    # Number of business days in transit (distinct from estimated_delivery_date — some
    # distributors, Turn14 included, give only one or the other per option).
    days_in_transit: typing.Optional[int] = None
    # Distributor's own marketing/eligibility blurb for this specific option (e.g. Turn14's
    # "Preferred Access $9.75 Flat Rate ... For Powersports" text) — display-only, and NOT a
    # substitute for service_level_name: most options don't carry one at all, and where present
    # it describes eligibility/terms, not a clean carrier/method name.
    verbose_eta: typing.Optional[str] = None
    # The identifier to send back when SELECTING this exact priced option — e.g. Turn14's
    # shipping_quote_id, which is distinct from and more specific than service_level_code (one
    # code can recur across many shipments/quotes; this id is unique to this one priced
    # quote-line and is what submit_order must send to actually choose it). Distributors with
    # no separate quote-scoped id (Keystone/Meyer/Premier — their create-order calls take one
    # plain method code for the whole order) just set this equal to service_level_code.
    quote_option_id: typing.Optional[str] = None


@dataclasses.dataclass
class LinePromotion:
    """A distributor-applied discount on a quoted line (e.g. Turn14's per-item pricing
    promos) — already netted into the distributor's own unit_price/line_total in the raw
    response. Subtracted from ShippingQuoteLine.distributor_line_total to produce
    PurchaseOrderLineItem.distributor_net_line_total, which IS what feeds po.subtotal (see
    compute_totals) — this never touches unit_cost/line_total themselves, which stay our
    frozen catalog pricing regardless."""
    description: str
    amount: typing.Optional[decimal.Decimal] = None


@dataclasses.dataclass
class LineFee:
    """A distributor-applied fee that isn't tied to any specific line item (e.g. Turn14's
    per-shipment dropship fee) — display-only, surfaced separately from per-line pricing and
    never fed into our own subtotal/total math (see ShippingQuoteResult.fees)."""
    fee_type: str
    description: str
    amount: typing.Optional[decimal.Decimal] = None


@dataclasses.dataclass
class ShippingQuoteLine:
    line_item_id: int
    provider_external_id: str
    quantity_available: int
    quantity_backordered: int = 0
    warehouse_code: typing.Optional[str] = None
    # Human-readable label for warehouse_code (e.g. "Hatfield, PA"), when the distributor
    # exposes a location catalog we can decode it against — null for distributors/adapters that
    # don't have one. Display-only; warehouse_code remains the identifier for selection/dedup.
    warehouse_name: typing.Optional[str] = None
    manufacturer_esd: typing.Optional[datetime.date] = None
    ship_options: typing.List[ShipOption] = dataclasses.field(default_factory=list)
    # Normalized flags, e.g. "kit", "backorder", "must_order_case_qty", "blocked".
    flags: typing.List[str] = dataclasses.field(default_factory=list)
    # Per-item promos active on this quote (see LinePromotion) — same list on every shipment
    # line for a given item_id, since the promo applies to the item as a whole, not per-shipment.
    promotions: typing.List[LinePromotion] = dataclasses.field(default_factory=list)
    # Distributor's own quoted price for THIS shipment-split (gross, before promotions).
    # Summed across every split for this item_id and netted against promotions to produce
    # PurchaseOrderLineItem.distributor_net_line_total, which drives po.subtotal when present
    # (see compute_totals) — this is the authoritative billing price once a quote has
    # returned one; it never overwrites unit_cost/line_total themselves, which stay our frozen
    # catalog pricing (used as a fallback only for distributors that don't quote per-item
    # pricing). unit_price is normally stable across every split for the same item_id;
    # line_total is per-split (unit_price * this split's quantity), not the item's overall total.
    distributor_unit_price: typing.Optional[decimal.Decimal] = None
    distributor_line_total: typing.Optional[decimal.Decimal] = None


@dataclasses.dataclass
class ShippingQuoteResult:
    lines: typing.List[ShippingQuoteLine]
    # Opaque distributor-specific token(s) needed later to submit against this exact quote
    # (e.g. Turn14's quote_id + per-location shipping_quote_id). Stored verbatim on
    # PurchaseOrder.quote_raw_response and handed back to submit_order() unchanged.
    raw_response: typing.Dict
    # Distributor's own quoted grand total across every line (gross, before any shipping
    # method is selected) — display-only, never fed into PurchaseOrder.total (our own frozen
    # catalog pricing stays authoritative for billing).
    distributor_total: typing.Optional[decimal.Decimal] = None
    # Fees that apply to the order as a whole rather than any specific line (e.g. a dropship
    # fee) — see LineFee. Display-only, same reasoning as distributor_total.
    fees: typing.List[LineFee] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class LineItemPlacement:
    line_item_id: int
    distributor_order_number: str
    quantity_confirmed: int
    quantity_backordered: int = 0
    warehouse_code: typing.Optional[str] = None
    status_code: typing.Optional[str] = None
    status_message: typing.Optional[str] = None


@dataclasses.dataclass
class DistributorOrderResult:
    """
    One submit_order() call can yield more than one distributor-side order number — either
    genuinely separate orders (Meyer's ``Orders`` array) or one order with several fulfilling
    warehouses (Keystone, Turn14's single-order/multi-shipment shape). Both are represented
    the same way here: one or more distinct ``distributor_order_number`` values, each with
    its own PurchaseOrderDistributorOrder row.
    """
    distributor_order_numbers: typing.List[str]
    line_item_placements: typing.List[LineItemPlacement]
    raw_response: typing.Dict


@dataclasses.dataclass
class DistributorOrderStatus:
    distributor_order_number: str
    status_code: str
    tracking_numbers: typing.List[str] = dataclasses.field(default_factory=list)
    carrier: typing.Optional[str] = None
    raw_response: typing.Optional[typing.Dict] = None


@dataclasses.dataclass
class OrderStatusResult:
    orders: typing.List[DistributorOrderStatus]


@dataclasses.dataclass
class InvoiceTrackingEntry:
    """One tracking number on an invoice, paired with the ship method it actually went out on
    (an invoice can carry more than one package/tracking number — e.g. Turn14's own invoice
    dashboard lists each separately)."""
    ship_method: typing.Optional[str] = None
    tracking_number: typing.Optional[str] = None


@dataclasses.dataclass
class DistributorInvoice:
    """
    A distributor-issued invoice for (part of) a PurchaseOrder — created once items actually
    ship, not at order-placement time, so a single PO commonly accumulates more than one of
    these over its lifetime (e.g. an immediate shipment plus a later backorder release). Only
    returned by adapters where supports_invoices() is True (currently Turn14 — see
    GET /v1/invoices/po/{purchase_order_number}).
    """
    invoice_number: str
    invoice_date: typing.Optional[datetime.date] = None
    # The distributor's own order id this invoice was issued against, when the response states
    # one — informational only, not used to look anything up (see PurchaseOrderInvoice).
    distributor_order_number: typing.Optional[str] = None
    # The distributor's customer-facing "website" order number, when distinct from its
    # internal order id (Turn14's `website_order_number`) — the number a customer support
    # rep on the distributor's side would actually recognize.
    website_order_number: typing.Optional[str] = None
    total_price: typing.Optional[decimal.Decimal] = None
    freight: typing.Optional[decimal.Decimal] = None
    discount_amount: typing.Optional[decimal.Decimal] = None
    paid_amount: typing.Optional[decimal.Decimal] = None
    amount_due: typing.Optional[decimal.Decimal] = None
    tracking: typing.List[InvoiceTrackingEntry] = dataclasses.field(default_factory=list)
    comments: typing.Optional[str] = None
    raw_response: typing.Optional[typing.Dict] = None


@dataclasses.dataclass
class ShippingMethod:
    """One entry in a distributor's catalog of selectable shipping methods (e.g. from
    Turn14's GET /v1/shipping). This is a name/carrier catalog, not a priced quote — most
    distributor APIs (Turn14 included) only compute an actual price for ONE method per
    quote call, so picking a method here is "filter the next quote to this carrier," not
    "compare live prices across methods in one call"."""
    code: str
    name: str
    carrier_name: typing.Optional[str] = None


class DistributorOrderAdapter(abc.ABC):
    """
    One instance per CompanyProviders connection (holds that connection's credentials).
    Only implemented for distributors whose order API is registered in ``registry.py``.
    """

    provider_kind: int  # src.enums.BrandProviderKind value, set by each subclass

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        self.company_provider = company_provider

    @abc.abstractmethod
    def get_shipping_quote(
        self,
        line_items: typing.List[OrderLineItemRequest],
        ship_to: ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> ShippingQuoteResult:
        """
        Non-committal availability/pricing/shipping lookup. Safe to call repeatedly.

        ``ship_method``, when given, is a code from list_shipping_methods() — implementations
        should use it to FILTER the quote to that carrier/method (some distributors, Turn14
        included, only compute a price for one method per call; there is no live
        cross-method comparison within a single quote). Leave None to get the distributor's
        own default/cheapest pick.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[OrderLineItemRequest],
        ship_to: ShipToAddress,
    ) -> DistributorOrderResult:
        """
        Places the order for real. Where the distributor supports a dedup key (e.g. Meyer's
        CustPO), implementations should treat resubmission with the same
        ``purchase_order.po_number`` as idempotent. Where it does not (Keystone), the caller
        (the PurchaseOrderJob runner) is responsible for at-most-once execution via job
        state — this method does not attempt its own dedup in that case.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> OrderStatusResult:
        """Polls current status/tracking for every distributor order linked to this PO."""
        raise NotImplementedError

    @abc.abstractmethod
    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        """
        Best-effort cancel. Returns False (does not raise) when the distributor's order has
        already progressed past a cancellable state. Only called when supports_cancel() is
        True; callers should surface OrderNotSupportedError to the user otherwise.
        """
        raise NotImplementedError

    def supports_cancel(self) -> bool:
        """Override to return False for distributors with no cancel endpoint at all."""
        return True

    def supports_shipping_method_selection(self) -> bool:
        """Override to return True for distributors whose adapter implements
        list_shipping_methods(). False by default — the UI should hide the picker and rely
        on the distributor's own default/cheapest pick."""
        return False

    def list_shipping_methods(self) -> typing.List[ShippingMethod]:
        """
        Catalog of selectable shipping method codes/names for this connection. Only called
        when supports_shipping_method_selection() is True. Not priced — see
        get_shipping_quote's ``ship_method`` param for how a selection actually affects cost.
        """
        raise NotImplementedError

    def supports_invoices(self) -> bool:
        """Override to return True for distributors whose adapter implements get_invoices()."""
        return False

    def get_invoices(self, purchase_order: src_models.PurchaseOrder) -> typing.List[DistributorInvoice]:
        """
        Fetch every invoice issued against this PO so far. Only called when
        supports_invoices() is True — invoices are created once items actually ship (not at
        order-placement time), so this can return an empty list for a while after submit, and
        can grow over the PO's lifetime as backordered items eventually ship separately.
        """
        raise NotImplementedError
