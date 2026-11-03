"""
Read-only order-discovery reader interface -- a new, deliberately narrow hierarchy for listing a
distributor's own order history (used to discover orders this app did not place), kept entirely
separate from ``src.integrations.orders.base.DistributorOrderAdapter``. That hierarchy is
write-capable by design (abstract ``submit_order``/``cancel_order``) and this feature must never
be able to reach either call, even by accident -- see
docs/tasks/external-orders/research-be.md's "Adapters" section for the full reasoning. Readers
are registered separately too, in ``history_registry.py``, which does not import (and is not
imported by) ``registry.py``.
"""
import abc
import dataclasses
import datetime
import decimal
import typing

from src import models as src_models


@dataclasses.dataclass
class ExternalOrderResult:
    """
    One distributor-side order, normalized just enough for
    ``src.integrations.services.external_order_sync`` to dedup/upsert it. NOT the same shape as
    ``base.DistributorOrderResult`` -- that one describes an order THIS app just placed; this
    one describes an order discovered by polling history that may or may not be ours.

    ``ship_to``, when not None, is a partial dict with any subset of name/address1/address2/
    city/state/postal_code/country/phone -- a reader fills in only what its distributor's
    response actually gives it (see e.g. history_readers/turn_14.py, which only ever has
    address1, since Turn14's bulk listing gives one opaque pre-formatted string).
    """

    distributor_order_number: str
    customer_po_number: typing.Optional[str]
    order_date: typing.Optional[datetime.date]
    # The distributor's own raw status text, verbatim -- never translated into our own
    # PurchaseOrderStatus vocabulary (see contract.md's ExternalOrder.distributor_status).
    status: typing.Optional[str]
    ship_to: typing.Optional[typing.Dict[str, typing.Optional[str]]]
    subtotal: typing.Optional[decimal.Decimal]
    freight: typing.Optional[decimal.Decimal]
    total: typing.Optional[decimal.Decimal]
    # [{part_number, description, quantity, unit_price, line_total, warehouse_code,
    #   warehouse_name}]
    line_items: typing.List[typing.Dict]
    raw_response: typing.Dict
    # Meyer-only: "API Order" == "Yes"/"No" as a bool -- authoritative on whether we placed it.
    # None for every other distributor (no such flag exists on their listing responses). See
    # src.enums.ExternalOrderOriginSignal / external_order_sync._apply_results.
    api_order_flag: typing.Optional[bool] = None

    # -- Detail parity with an app-placed order (see ExternalPurchaseOrder's own field comments
    # and _serialize_confirmed_distributor_order, the shape these mirror). Every one defaults to
    # empty so a reader that has no such data simply does not set it.

    # Plain invoice-number strings; a list because one order can invoice more than once when its
    # lines ship from different warehouses.
    distributor_invoice_numbers: typing.List[str] = dataclasses.field(default_factory=list)
    # [{tracking_number, carrier}] -- carrier None where only a number is given.
    tracking: typing.List[typing.Dict] = dataclasses.field(default_factory=list)
    # Raw code always; label only where the distributor publishes one for that code.
    ship_method_code: typing.Optional[str] = None
    ship_method: typing.Optional[str] = None
    # [{status, occurred_at}] oldest first, in the distributor's own vocabulary.
    status_history: typing.List[typing.Dict] = dataclasses.field(default_factory=list)
    # Distributor's own estimate of arrival, where it gives one.
    estimated_delivery_date: typing.Optional[datetime.date] = None


class OrderHistoryReader(abc.ABC):
    """
    One instance per (company_provider, order_account) pair -- read-only, constructed the same
    way every ``DistributorOrderAdapter`` is (credentials resolved via
    ``src.integrations.credentials.get_order_credentials``) but deliberately NOT a
    ``base.DistributorOrderAdapter`` subclass, so there is no path from a reader instance to
    ``submit_order``/``cancel_order``, even by accident.
    """

    def __init__(
        self,
        company_provider: src_models.CompanyProviders,
        order_account: src_models.CompanyProviderOrderAccount,
    ) -> None:
        self.company_provider = company_provider
        self.order_account = order_account

    @abc.abstractmethod
    def list_orders(self, start_date: datetime.date, end_date: datetime.date) -> typing.List[ExternalOrderResult]:
        """
        Every order in [start_date, end_date] (inclusive), per this distributor's own
        order-history API. Raises on a real transport/auth failure -- callers
        (``external_order_sync``) are responsible for catching that and recording it against
        ``ExternalOrderSyncWatermark``.
        """
        raise NotImplementedError

    def list_current_orders(self) -> typing.List[ExternalOrderResult]:
        """
        Override only for a distributor whose "current/open orders" listing takes no date range
        at all (Meyer's SalesCurrent) -- every other reader's default here just calls
        ``list_orders`` with today's date on both ends, which is never actually called (the
        sync service only invokes this for readers that need it; see
        ``external_order_sync._collect_results``), kept only so the interface has a sane default
        rather than requiring every non-Meyer reader to implement a method nothing calls.
        """
        today = datetime.date.today()
        return self.list_orders(today, today)

    def enrich_new_order(self, result: ExternalOrderResult) -> ExternalOrderResult:
        """
        Called only for a genuinely NEW external order (never for a refresh of one already
        stored -- see contract.md's "Only for NEWLY discovered orders, never on every poll").
        Default is a no-op; Premier overrides this to make one follow-up
        ``get_sales_order()`` call, since its bulk listing carries no ship-to address or line
        pricing at all (tracking-specific fields only).
        """
        return result
