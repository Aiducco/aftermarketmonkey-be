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


@dataclasses.dataclass
class LinePromotion:
    """A distributor-applied discount on a quoted line (e.g. Turn14's per-item pricing
    promos) — informational for the buyer, already netted into the distributor's own
    unit_price/line_total in the raw response, so this is display-only and never feeds back
    into our own subtotal math (that's still our frozen catalog pricing)."""
    description: str
    amount: typing.Optional[decimal.Decimal] = None


@dataclasses.dataclass
class ShippingQuoteLine:
    line_item_id: int
    provider_external_id: str
    quantity_available: int
    quantity_backordered: int = 0
    warehouse_code: typing.Optional[str] = None
    manufacturer_esd: typing.Optional[datetime.date] = None
    ship_options: typing.List[ShipOption] = dataclasses.field(default_factory=list)
    # Normalized flags, e.g. "kit", "backorder", "must_order_case_qty", "blocked".
    flags: typing.List[str] = dataclasses.field(default_factory=list)
    # Per-item promos active on this quote (see LinePromotion) — same list on every shipment
    # line for a given item_id, since the promo applies to the item as a whole, not per-shipment.
    promotions: typing.List[LinePromotion] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ShippingQuoteResult:
    lines: typing.List[ShippingQuoteLine]
    # Opaque distributor-specific token(s) needed later to submit against this exact quote
    # (e.g. Turn14's quote_id + per-location shipping_quote_id). Stored verbatim on
    # PurchaseOrder.quote_raw_response and handed back to submit_order() unchanged.
    raw_response: typing.Dict


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
