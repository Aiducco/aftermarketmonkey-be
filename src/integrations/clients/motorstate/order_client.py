"""
Motor State's ordering API -- stock/price inquiry, shipping estimate, validation, order
placement and order status. Separate from ``client.py`` (brands) and from the FTP feed, and
authenticated with the dealer's own API key rather than the feed's FTP login.

Verified live against api.motorstate.com on 2026-10-28 with a real dealer key. Spec read from
https://api.motorstate.com/swagger/docs/v1 -- note the Swagger UI's own link, /v3/api-docs and
the other usual paths all 404; only /swagger/docs/v1 serves it.

Four behaviours that shape every method here, all measured rather than documented:

**One request body drives four endpoints.** OrderInquiry, ShippingEstimate, OrderValidation and
Order all take the same ``PurchaseOrder``. That is the point of :meth:`_purchase_order`: a quote
that differs from the submitted order prices something nobody buys.

**A payment method is mandatory, and omitting it fails in two different ways.** Without
``PaymentMethodCode`` an inquiry comes back ``HasErrors: true`` with "Payment Method has not been
selected" and an EMPTY rate list, while ShippingEstimate returns a bare HTTP 500. Neither reads
as "you forgot a field", so :meth:`_purchase_order` refuses to build a body without one.

**OrderInquiry already returns the freight options.** Its ``AvailableShippingMethodRates`` is the
same list ShippingEstimate returns, so a quote needs ONE call, not two. ShippingEstimate stays
for the rates-only case (re-pricing a cart on a shipping-method change).

**A rate can be absent rather than zero.** Methods the cart cannot use come back with no
``Rate`` key at all (USPS Priority Mail on a heavy cart, for example). Reading a missing rate as
0.00 would offer free shipping on a method that cannot ship it.

Errors arrive as HTTP 422 with the business reason in ``HeaderErrors`` (for example code 17,
"COD payment is not available for drop shipment orders", and code 20, FedEx having retired COD),
so the status code alone is not enough to tell a validation problem from a transport one.
"""
import logging
import typing
import uuid

import requests
from django.conf import settings

from src.integrations.clients.motorstate import exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[MOTOR-STATE-ORDER-CLIENT]"

REQUEST_TIMEOUT_SECONDS = 60

# Payment codes seen on real accounts, best first. Terms accounts get ON ACCOUNT; others a card.
# COD is deliberately last: it is refused outright for drop shipments, and FedEx retired every
# COD service in July 2023, so defaulting to it would fail most carts.
_PAYMENT_PREFERENCE = ("ON ACCOUNT", "ONACCOUNT", "CREDITCARD", "CHECK")
_COD_CODES = frozenset({"CHECK", "COD"})


def quote_reference(prefix: str = "AMS-QUOTE") -> str:
    """
    A throwaway PurchaseOrderNumber for the non-committal calls.

    OrderInquiry and ShippingEstimate both demand one ("Purchase Order Number has not been
    entered") even though neither creates an order, so quoting has to supply something. It is
    deliberately unique per call and obviously not a real PO, so it can never be mistaken for
    one in Motor State's order history.
    """
    return "{}-{}".format(prefix, uuid.uuid4().hex[:12].upper())


class MotorStateOrderClient:
    def __init__(self, credentials: typing.Dict, base_url: typing.Optional[str] = None) -> None:
        self.api_key = str((credentials or {}).get("api_key") or "").strip()
        if not self.api_key:
            raise ValueError("Invalid Motor State order credentials -- missing api_key.")
        self.base_url = str(
            base_url or getattr(settings, "MOTOR_STATE_BASE_URL", "https://api.motorstate.com")
        ).rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({"accept": "application/json", "apiKey": self.api_key})
        self._order_options: typing.Optional[typing.Dict] = None

    # -- transport -----------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs: typing.Any) -> typing.Any:
        url = "{}{}".format(self.base_url, path)
        try:
            response = self._session.request(method, url, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs)
        except requests.exceptions.RequestException as e:
            raise exceptions.MotorStateAPIConnectionError(
                "{} {} failed: {}".format(method, url, str(e))
            )

        if response.status_code in (401, 403):
            raise exceptions.MotorStatePermissionError(
                message=(
                    "Motor State rejected this API key (HTTP {}). Confirm it is active for your "
                    "account.".format(response.status_code)
                ),
                code=response.status_code,
            )

        try:
            data = response.json()
        except ValueError:
            raise exceptions.MotorStateAPIBadResponseCodeError(
                message="{} {} returned non-JSON (HTTP {}): {}".format(
                    method, path, response.status_code, (response.text or "")[:300]
                ),
                code=response.status_code,
            )

        # 422 carries a real, readable business reason in the body; let callers read it off the
        # payload rather than turning it into a generic transport failure here.
        if response.status_code >= 400 and response.status_code != 422:
            raise exceptions.MotorStateAPIBadResponseCodeError(
                message="{} {} returned HTTP {}: {}".format(
                    method, path, response.status_code, str(data)[:300]
                ),
                code=response.status_code,
            )
        return data

    # -- account options -----------------------------------------------------

    def get_order_options(self, refresh: bool = False) -> typing.Dict:
        """
        ``GET /api/OrderOptions`` -- the shipping and payment codes valid for THIS dealer.

        Cached per client: it is per-account static configuration, and both the quote path and
        the shipping-method list need it. 17 shipping codes on a real account, from PFXG
        (FedEx Ground) to tkpp (Truck Freight) and P-UP (Pickup at Motor State).
        """
        if self._order_options is None or refresh:
            self._order_options = self._request("GET", "/api/OrderOptions") or {}
        return self._order_options

    def shipping_methods(self) -> typing.List[typing.Dict]:
        return list((self.get_order_options() or {}).get("ShippingMethods") or [])

    def payment_methods(self) -> typing.List[typing.Dict]:
        return list((self.get_order_options() or {}).get("PaymentMethods") or [])

    def default_payment_method(self) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
        """
        (code, credit_card_last4) to use when the dealer has not pinned one.

        Preference order is deliberate -- see _PAYMENT_PREFERENCE. COD is chosen only when it is
        the sole option, because Motor State refuses it for drop shipments and FedEx retired
        every COD service, so it fails most real carts.
        """
        methods = self.payment_methods()
        if not methods:
            return None, None
        by_code = {str(m.get("Code") or "").strip().upper(): m for m in methods}
        for preferred in _PAYMENT_PREFERENCE:
            if preferred in by_code:
                m = by_code[preferred]
                return str(m.get("Code") or ""), (m.get("CreditCardLast4") or None)
        non_cod = [m for c, m in by_code.items() if c not in _COD_CODES]
        chosen = non_cod[0] if non_cod else methods[0]
        return str(chosen.get("Code") or ""), (chosen.get("CreditCardLast4") or None)

    def ping(self) -> typing.Any:
        """``GET /api/Ping`` -- cheapest call that still proves the key works."""
        return self._request("GET", "/api/Ping")

    # -- request building ----------------------------------------------------

    def _purchase_order(
        self,
        lines: typing.Sequence[typing.Dict],
        payment_method_code: str,
        purchase_order_number: str,
        shipping_method_code: typing.Optional[str] = None,
        credit_card_last4: typing.Optional[str] = None,
        drop_ship_address: typing.Optional[typing.Dict] = None,
        special_instructions: typing.Optional[str] = None,
        signature_required: typing.Optional[bool] = None,
        freight_options: typing.Optional[typing.Dict] = None,
    ) -> typing.Dict:
        # Two fields are mandatory on EVERY priced request, including the non-committal ones,
        # and both fail in ways that do not name the missing field -- so they are refused here
        # rather than at Motor State:
        #   * no PaymentMethodCode -> an inquiry returns HasErrors with an EMPTY rate list, and
        #     ShippingEstimate returns a bare HTTP 500.
        #   * no PurchaseOrderNumber -> "Purchase Order Number has not been entered", again with
        #     no rates, even though an inquiry commits nothing and creates no order.
        if not payment_method_code:
            raise ValueError(
                "Motor State requires a PaymentMethodCode on every priced request; "
                "see get_order_options() for the codes this account may use."
            )
        if not str(purchase_order_number or "").strip():
            raise ValueError(
                "Motor State requires a PurchaseOrderNumber on every priced request, including "
                "OrderInquiry and ShippingEstimate; pass a quote reference for those."
            )
        body: typing.Dict[str, typing.Any] = {
            "PaymentMethodCode": payment_method_code,
            "PurchaseOrderNumber": purchase_order_number,
            "Lines": [
                {
                    "LineNumber": line.get("line_number") or index,
                    "PartNumber": line.get("part_number"),
                    "QuantityShip": line.get("quantity"),
                }
                for index, line in enumerate(lines, start=1)
            ],
        }
        if shipping_method_code:
            body["ShippingMethodCode"] = shipping_method_code
        if credit_card_last4:
            body["CreditCardLast4"] = credit_card_last4
        if drop_ship_address:
            body["DropShipAddress"] = drop_ship_address
        if special_instructions:
            body["SpecialInstructions"] = special_instructions
        if signature_required is not None:
            body["SignatureRequired"] = bool(signature_required)
        if freight_options:
            body["FreightOptions"] = freight_options
        return body

    # -- operations ----------------------------------------------------------

    def order_inquiry(self, lines: typing.Sequence[typing.Dict], **kwargs: typing.Any) -> typing.Dict:
        """
        ``POST /api/OrderInquiry`` -- prices and availability for a cart, plus the freight
        options. Commits nothing. This is the quote: one call covers both.
        """
        return self._request(
            "POST", "/api/OrderInquiry", json=self._purchase_order(lines, **kwargs)
        ) or {}

    def shipping_estimate(self, lines: typing.Sequence[typing.Dict], **kwargs: typing.Any) -> typing.Dict:
        """``POST /api/ShippingEstimate`` -- rates only, for re-pricing on a method change."""
        return self._request(
            "POST", "/api/ShippingEstimate", json=self._purchase_order(lines, **kwargs)
        ) or {}

    def validate_order(self, lines: typing.Sequence[typing.Dict], **kwargs: typing.Any) -> typing.Dict:
        """``POST /api/OrderValidation`` -- checks the cart against Motor State's order rules."""
        return self._request(
            "POST", "/api/OrderValidation", json=self._purchase_order(lines, **kwargs)
        ) or {}

    def place_order(self, lines: typing.Sequence[typing.Dict], **kwargs: typing.Any) -> typing.Dict:
        """``POST /api/Order`` -- submits a real order."""
        return self._request(
            "POST", "/api/Order", json=self._purchase_order(lines, **kwargs)
        ) or {}

    def get_orders(
        self,
        order_numbers: typing.Optional[typing.Sequence[str]] = None,
        po_numbers: typing.Optional[typing.Sequence[str]] = None,
        updated_after: typing.Optional[str] = None,
    ) -> typing.List[typing.Dict]:
        """
        ``GET /api/Order`` -- order detail, looked up by Motor State's order number or by OUR PO
        number. Returns the OrderResponse list: one entry per requested number, each carrying
        ``Found`` and the ``Order`` itself.
        """
        params: typing.Dict[str, str] = {}
        if order_numbers:
            params["orderNumbers"] = ",".join(str(n) for n in order_numbers)
        if po_numbers:
            params["poNumbers"] = ",".join(str(n) for n in po_numbers)
        if updated_after:
            params["updatedAfter"] = updated_after
        if not params:
            return []
        data = self._request("GET", "/api/Order", params=params)
        if isinstance(data, dict):
            return [data]
        return list(data or [])

    def get_carton_details(self, order_number: str) -> typing.Any:
        """``GET /api/Order/GetCartonDetails`` -- per-carton tracking for a placed order."""
        return self._request(
            "GET", "/api/Order/GetCartonDetails", params={"orderNumber": order_number}
        )
