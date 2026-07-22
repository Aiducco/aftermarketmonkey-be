"""
MeyerOrderAdapter — DistributorOrderAdapter implementation for Meyer Distributing's v2 REST
Order API. Wraps ``src.integrations.clients.meyer.order_client.MeyerOrderApiClient``.

SAFETY: submit_order() places a REAL order against Meyer, and cancel_order() has real (possibly
irreversible) effect. Both must only ever be invoked from an explicit, user-approved
submission/cancellation — never from exploratory/dev code, automated tests, or ad-hoc scripts.
See ``src/integrations/orders/turn_14.py`` for the reference adapter this mirrors.

Not yet handled: Meyer's "route"/Meyer Truck orders (AddressCode + "Meyer Truck" ship method,
a distinct order-creation path per Meyer's docs) — out of scope until a company actually needs
that fulfillment mode.
"""
import datetime
import decimal
import logging
import typing

from django.conf import settings

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.meyer import exceptions as meyer_client_exceptions
from src.integrations.clients.meyer.order_client import MeyerOrderApiClient
from src.integrations.orders import base
from src.integrations.orders import exceptions as order_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[MEYER-ORDER-ADAPTER]"

# Cancel error codes that mean "the order has already progressed past a cancellable state," per
# Meyer's docs — cancel_order() must return False for these (per base.py's contract), not raise.
_CANCEL_NOT_CANCELLABLE_CODES = {"60501", "60502"}


def _parse_decimal(value: typing.Optional[typing.Any]) -> typing.Optional[decimal.Decimal]:
    if value in (None, ""):
        return None
    try:
        return decimal.Decimal(str(value))
    except decimal.InvalidOperation:
        return None


def _parse_meyer_date(value: typing.Optional[str]) -> typing.Optional[datetime.date]:
    if not value:
        return None
    try:
        return datetime.datetime.strptime(value.strip(), "%m/%d/%Y").date()
    except ValueError:
        return None


def _filter_options(
    options: typing.List[base.ShipOption], ship_method: typing.Optional[str]
) -> typing.List[base.ShipOption]:
    if not ship_method:
        return options
    filtered = [o for o in options if o.service_level_code == ship_method]
    return filtered or options


class MeyerOrderAdapter(base.DistributorOrderAdapter):
    provider_kind = src_enums.BrandProviderKind.MEYER.value

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        base.DistributorOrderAdapter.__init__(self, company_provider)
        environment = getattr(settings, "MEYER_ORDER_ENVIRONMENT", "testing")
        self._client = MeyerOrderApiClient(
            credentials=credentials_helper.get_order_credentials(company_provider),
            environment=environment,
        )

    # -- Request building -----------------------------------------------------------------

    @staticmethod
    def _build_ship_to_fields(ship_to: base.ShipToAddress) -> typing.Dict[str, str]:
        return {
            "ShipToName": ship_to.name,
            "ShipToAddress1": ship_to.address1,
            "ShipToAddress2": ship_to.address2 or "",
            "ShipToCity": ship_to.city,
            "ShipToState": ship_to.state,
            "ShipToZipcode": ship_to.postal_code,
            "ShipToCountry": ship_to.country,
        }

    def _handle_error(self, e: Exception) -> None:
        code = getattr(e, "code", None)
        raise order_exceptions.OrderValidationError(message=str(e), code=str(code) if code else None)

    # -- DistributorOrderAdapter ------------------------------------------------------------

    def get_shipping_quote(
        self,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
        ship_method: typing.Optional[str] = None,
    ) -> base.ShippingQuoteResult:
        data = dict(self._build_ship_to_fields(ship_to))
        data["Items"] = [
            {"ItemNumber": li.provider_part.provider_external_id, "Quantity": li.quantity} for li in line_items
        ]
        try:
            groups = self._client.get_shipping_rate_mass_quote(data)
        except meyer_client_exceptions.MeyerException as e:
            self._handle_error(e)

        logger.info("{} Quote response: {}".format(_LOG_PREFIX, repr(groups)[:4000]))

        try:
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}
            lines: typing.List[base.ShippingQuoteLine] = []
            seen: typing.Set[str] = set()

            for group in groups:
                warehouse = group.get("warehouse", "")
                skus = [s.strip() for s in (group.get("skus") or "").split(",") if s.strip()]
                ship_options = _filter_options(
                    [
                        base.ShipOption(
                            service_level_code=(q.get("ShipMethod") or "").strip(),
                            service_level_name=q.get("ServiceType", ""),
                            estimated_delivery_date=_parse_meyer_date(q.get("DeliveryDate")),
                            cost=_parse_decimal(q.get("Cost")),
                            # No separate quote-scoped id for Meyer — CreateOrder takes the same
                            # ShipMethod code shown here directly.
                            quote_option_id=(q.get("ShipMethod") or "").strip(),
                        )
                        for q in group.get("quotes", [])
                    ],
                    ship_method,
                )
                for sku in skus:
                    li = by_external_id.get(sku)
                    seen.add(sku)
                    lines.append(
                        base.ShippingQuoteLine(
                            line_item_id=li.line_item_id if li else 0,
                            provider_external_id=sku,
                            # Meyer's mass-quote response doesn't carry an explicit per-item
                            # availability/backorder signal at quote time — an item only being
                            # absent from every group's "skus" list is our only stockout signal
                            # (handled below), so a present item is treated as fully available
                            # for the quantity we requested.
                            quantity_available=li.quantity if li else 0,
                            warehouse_code=warehouse,
                            ship_options=ship_options,
                        )
                    )

            # Items never echoed back in any group's skus weren't matched to any warehouse.
            for external_id, li in by_external_id.items():
                if external_id in seen:
                    continue
                lines.append(
                    base.ShippingQuoteLine(
                        line_item_id=li.line_item_id,
                        provider_external_id=external_id,
                        quantity_available=0,
                        flags=["not_returned_in_quote"],
                    )
                )

            if not lines:
                raise order_exceptions.OrderValidationError(
                    "Unexpected/empty quote response shape from Meyer. Raw response: {}".format(repr(groups)[:2000])
                )
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected quote response shape from Meyer ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(groups)[:2000]
                )
            )

        return base.ShippingQuoteResult(lines=lines, raw_response={"groups": groups})

    def submit_order(
        self,
        purchase_order: src_models.PurchaseOrder,
        line_items: typing.List[base.OrderLineItemRequest],
        ship_to: base.ShipToAddress,
    ) -> base.DistributorOrderResult:
        if not purchase_order.ship_method:
            raise order_exceptions.OrderValidationError(
                "Meyer requires a ship method to be selected before an order can be submitted."
            )

        data = dict(self._build_ship_to_fields(ship_to))
        data.update(
            {
                "ShipMethod": purchase_order.ship_method,
                "ShipToPhone": ship_to.phone or "",
                "CustPO": purchase_order.po_number,
                # We're a marketplace/dropship intermediary with no visibility into whether
                # sales tax was independently collected from the end customer — "No" is the
                # conservative default (Meyer can still add tax on their end rather than risk
                # it going uncollected). No field on PurchaseOrder sources a real answer today.
                "CollectedSalesTax": "No",
                "Items": [
                    {"ItemNumber": li.provider_part.provider_external_id, "Quantity": li.quantity}
                    for li in line_items
                ],
            }
        )

        try:
            response = self._client.create_order(customer_number=self._client.customer_number, data=data)
        except meyer_client_exceptions.MeyerException as e:
            self._handle_error(e)

        return self._parse_submit_response(response, line_items)

    @staticmethod
    def _parse_submit_response(
        response: typing.Dict, line_items: typing.List[base.OrderLineItemRequest]
    ) -> base.DistributorOrderResult:
        try:
            by_external_id = {li.provider_part.provider_external_id: li for li in line_items}
            order_numbers: typing.List[str] = []
            placements: typing.List[base.LineItemPlacement] = []

            # Meyer can split one submission into genuinely separate order numbers (one per
            # fulfilling warehouse) — see base.DistributorOrderResult's docstring, which already
            # names this exact shape.
            for order in response.get("Orders", []):
                order_number = order.get("OrderNumber", "")
                if order_number:
                    order_numbers.append(order_number)
                for item in order.get("Items", []):
                    external_id = item.get("ItemNumber", "")
                    li = by_external_id.get(external_id)
                    placements.append(
                        base.LineItemPlacement(
                            line_item_id=li.line_item_id if li else 0,
                            distributor_order_number=order_number,
                            quantity_confirmed=item.get("Quantity", 0),
                        )
                    )

            if not order_numbers:
                raise order_exceptions.OrderValidationError(
                    "Unexpected/empty order response shape from Meyer. Raw response: {}".format(
                        repr(response)[:2000]
                    )
                )
        except order_exceptions.OrderValidationError:
            raise
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order response shape from Meyer ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(response)[:2000]
                )
            )

        return base.DistributorOrderResult(
            distributor_order_numbers=order_numbers,
            line_item_placements=placements,
            raw_response=response,
        )

    def get_order_status(self, purchase_order: src_models.PurchaseOrder) -> base.OrderStatusResult:
        try:
            orders = self._client.get_sales_order_detail(
                order_number=purchase_order.po_number, customer_number=self._client.customer_number
            )
        except meyer_client_exceptions.MeyerException as e:
            self._handle_error(e)

        try:
            results: typing.List[base.DistributorOrderStatus] = []
            for order in orders:
                order_number = order.get("OrderNumber", "")
                invoiced = order.get("Invoiced", "")
                tracking_numbers = list(order.get("Tracking") or [])
                results.append(
                    base.DistributorOrderStatus(
                        distributor_order_number=order_number,
                        status_code="INVOICED" if invoiced == "Yes" else "OPEN",
                        tracking_numbers=tracking_numbers,
                        raw_response=order,
                    )
                )
        except (AttributeError, TypeError, KeyError, IndexError) as e:
            raise order_exceptions.OrderValidationError(
                "Unexpected order-status response shape from Meyer ({}: {}). Raw response: {}".format(
                    type(e).__name__, e, repr(orders)[:2000]
                )
            )
        return base.OrderStatusResult(orders=results)

    def cancel_order(self, purchase_order: src_models.PurchaseOrder) -> bool:
        distributor_orders = list(purchase_order.distributor_orders.all())
        if not distributor_orders:
            return False

        all_cancelled = True
        for distributor_order in distributor_orders:
            try:
                self._client.cancel_order(
                    order_number=distributor_order.distributor_order_number,
                    customer_number=self._client.customer_number,
                )
            except meyer_client_exceptions.MeyerOrderValidationError as e:
                if e.code in _CANCEL_NOT_CANCELLABLE_CODES:
                    all_cancelled = False
                    continue
                self._handle_error(e)
            except meyer_client_exceptions.MeyerException as e:
                self._handle_error(e)
        return all_cancelled

    def supports_shipping_method_selection(self) -> bool:
        return True

    def list_shipping_methods(self) -> typing.List[base.ShippingMethod]:
        try:
            methods = self._client.get_ship_methods()
        except meyer_client_exceptions.MeyerException as e:
            self._handle_error(e)

        return [
            base.ShippingMethod(
                code=(m.get("ShipMethod") or "").strip(),
                name=m.get("ServiceType", ""),
                carrier_name=m.get("Carrier"),
            )
            for m in methods
        ]
