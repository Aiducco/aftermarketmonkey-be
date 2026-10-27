"""
SOAP client for ATD Connect's order service -- previewOrder, placeOrder and getOrderDetail.

Why SOAP when the rest of this package is REST: ATD runs two parallel APIs off one host, and
ordering only exists on the SOAP one. The REST surface (``/rs/3_6``) carries the catalog,
pricing and availability we already ingest; orders live at ``/ws/3_6/orders`` and have no REST
equivalent we could confirm -- ``/rs/3_6/order/previeworder`` 401s, but so does
``/rs/3_6/bogusfamily/bogusoperation``, so that probe proves nothing either way.

Hand-rolled XML rather than a SOAP library. The service is six operations with flat request
bodies and no WS-* beyond plain credentials, so zeep would add a dependency, a WSDL fetch on
every client construction, and a code-generation step, to save a dozen lines of string building.

Verified live against ws.atdconnect.com on 2026-10-27, from the production host (ATD allowlists
by IP, so this is unreachable from a laptop -- the feed works for the same reason):

  endpoint     https://ws.atdconnect.com/ws/3_6/orders     document/literal, soapAction ""
  namespace    http://api.atdconnect.com/atd/3_6/orders    NOT the atd.com guess a WSDL-less
                                                           reading suggests; a wrong namespace
                                                           returns a bare 404 with no body,
                                                           which looks like a dead endpoint
  operations   previeworder placeorder getorderdetail getorderstatusbycriteria
               getorderapprovalstatus updateorderapprovalstatus
  no cancel    there is no cancel operation in the schema at all

**previewOrder commits nothing.** It takes the same cart as placeOrder and returns the priced
result -- order total, per-line cost/FET/line total, and per-fulfillment freight, ship method,
estimated delivery and source DC. It is both the price quote and the shipping quote in one
call, which is why this adapter can be exercised end to end without placing an order.

Authentication is HTTP Basic, the same username/password the REST feed uses. Entitlement is
NOT the same, though: an account can be fully authorised for the catalog and still get
"Unable to authenticate User's Credentials" from this service, because ATD grants order access
separately. That fault is what a non-entitled account sees, and it is indistinguishable from a
wrong password -- see ATDOrderNotEntitledError.
"""
import logging
import typing
import xml.etree.ElementTree as ET

import requests
from django.conf import settings

from src.integrations.clients.atd import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ATD-ORDER-CLIENT]"

DEFAULT_SOAP_BASE_URL = "https://ws.atdconnect.com/ws/3_6"
# ATD's sandbox. Their own process requires a confirmed test order here before they will enable
# ordering on a live account, so this is a real deployment target rather than a curiosity.
SANDBOX_SOAP_BASE_URL = "https://testws.atdconnect.com/ws/3_4"

ORDERS_NS = "http://api.atdconnect.com/atd/3_6/orders"
COMMON_NS = "http://api.atdconnect.com/atd/3_6/common"
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"

REQUEST_TIMEOUT_SECONDS = 60

# The fault ATD returns when the credentials are fine for the catalog but the account has no
# order entitlement. Matched on text because the fault carries no code to distinguish it.
_NOT_ENTITLED_FAULT = "unable to authenticate user's credentials"


def _text(parent: typing.Optional[ET.Element], tag: str) -> typing.Optional[str]:
    """Child text by local name, ignoring namespace -- ATD's responses are inconsistently prefixed."""
    if parent is None:
        return None
    for child in parent:
        if child.tag.rsplit("}", 1)[-1] == tag:
            return (child.text or "").strip() or None
    return None


def _children(parent: typing.Optional[ET.Element], tag: str) -> typing.List[ET.Element]:
    if parent is None:
        return []
    return [c for c in parent if c.tag.rsplit("}", 1)[-1] == tag]


def _find(root: ET.Element, tag: str) -> typing.Optional[ET.Element]:
    for el in root.iter():
        if el.tag.rsplit("}", 1)[-1] == tag:
            return el
    return None


def _escape(value: typing.Any) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _el(tag: str, value: typing.Any) -> str:
    """One ordered element, or nothing when the value is empty -- ATD's schema is a sequence,
    so an element carrying None must be omitted rather than sent empty."""
    if value is None or value == "":
        return ""
    return "<ord:{tag}>{value}</ord:{tag}>".format(tag=tag, value=_escape(value))


class ATDOrderClient:
    def __init__(self, credentials: typing.Dict, sandbox: typing.Optional[bool] = None) -> None:
        creds = credentials or {}
        self.username = str(creds.get("username") or "").strip()
        self.password = str(creds.get("password") or "")
        self.client_id = str(creds.get("client_id") or "").strip()
        self.location = str(creds.get("location") or "").strip()

        missing = [
            k for k, v in (
                ("username", self.username), ("password", self.password),
                ("location", self.location),
            ) if not v
        ]
        if missing:
            raise ValueError("Invalid ATD credentials -- missing: {}.".format(", ".join(missing)))

        if sandbox is None:
            sandbox = bool(getattr(settings, "ATD_ORDERS_USE_SANDBOX", False))
        configured = getattr(settings, "ATD_SOAP_BASE_URL", None)
        self.base_url = str(
            configured or (SANDBOX_SOAP_BASE_URL if sandbox else DEFAULT_SOAP_BASE_URL)
        ).rstrip("/")
        self.sandbox = sandbox

    # -- transport -----------------------------------------------------------

    def _post(self, operation: str, body: str) -> ET.Element:
        envelope = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<soapenv:Envelope xmlns:soapenv="{soap}" xmlns:ord="{ns}" xmlns:c="{common}">'
            "<soapenv:Body>{body}</soapenv:Body></soapenv:Envelope>"
        ).format(soap=SOAP_NS, ns=ORDERS_NS, common=COMMON_NS, body=body)

        url = "{}/orders".format(self.base_url)
        try:
            response = requests.post(
                url,
                data=envelope.encode("utf-8"),
                headers={
                    "Content-Type": "text/xml;charset=UTF-8",
                    # soapAction is "" for every operation in this WSDL, but omitting the header
                    # entirely makes some stacks reject the request.
                    "SOAPAction": "",
                    "clientId": self.client_id,
                },
                auth=(self.username, self.password),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.RequestException as e:
            raise exceptions.ATDConnectionError(
                "{} request to {} failed: {}".format(operation, url, str(e))
            )

        if response.status_code == 404 and not response.content:
            # Not a missing endpoint: ATD answers an unroutable SOAP body with a bare 404 and no
            # content, which is what a wrong namespace or a misspelled operation produces.
            raise exceptions.ATDAPIException(
                "{} was not routed by ATD (404, empty body) -- the request element or namespace "
                "is wrong, not the URL.".format(operation)
            )

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError:
            raise exceptions.ATDAPIException(
                "{} returned unparseable XML ({}): {}".format(
                    operation, response.status_code, response.text[:300]
                )
            )

        fault = _find(root, "Fault")
        if fault is not None:
            detail = _find(fault, "message")
            message = (detail.text if detail is not None and detail.text else None) or (
                _text(fault, "faultstring") or "unknown SOAP fault"
            )
            if _NOT_ENTITLED_FAULT in message.lower():
                raise exceptions.ATDOrderNotEntitledError(message.strip())
            raise exceptions.ATDAPIException("{} failed: {}".format(operation, message.strip()))

        if response.status_code >= 400:
            raise exceptions.ATDAPIBadResponseCodeError(
                "{} returned HTTP {}.".format(operation, response.status_code)
            )
        return root

    # -- cart ----------------------------------------------------------------

    def _cart_xml(
        self,
        line_items: typing.Sequence[typing.Dict],
        customer_po_number: typing.Optional[str] = None,
        pickup: bool = False,
        ship_to: typing.Optional[typing.Dict] = None,
        shipping_method: typing.Optional[str] = None,
        fill_kill: typing.Optional[str] = None,
        customer_comment: typing.Optional[str] = None,
    ) -> str:
        """
        The ``cart`` element, shared verbatim by previewOrder and placeOrder -- which is the
        point: a preview that differs from the submit prices something nobody is going to buy.
        """
        lines = []
        for index, item in enumerate(line_items, start=1):
            lines.append(
                "<ord:lineitem>{}{}{}{}</ord:lineitem>".format(
                    _el("cartlinenumber", item.get("cart_line_number") or index),
                    _el("atdproductnumber", item.get("atd_product_number")),
                    _el("quantity", item.get("quantity")),
                    _el("fillkill", item.get("fill_kill")),
                )
            )

        consumer = ""
        if ship_to:
            consumer = (
                "<ord:consumerdata><ord:consumershipto>"
                "{name}{phone}{email}"
                "<c:address>{a1}{a2}{city}{state}{zipcode}</c:address>"
                "</ord:consumershipto></ord:consumerdata>"
            ).format(
                name=_el("name", ship_to.get("name")),
                phone=_el("phonenumber", ship_to.get("phone")),
                email=_el("emailaddress", ship_to.get("email")),
                a1="<c:address1>{}</c:address1>".format(_escape(ship_to.get("address1") or "")),
                a2=(
                    "<c:address2>{}</c:address2>".format(_escape(ship_to["address2"]))
                    if ship_to.get("address2") else ""
                ),
                city="<c:city>{}</c:city>".format(_escape(ship_to.get("city") or "")),
                state="<c:state>{}</c:state>".format(_escape(ship_to.get("state") or "")),
                zipcode="<c:zipcode>{}</c:zipcode>".format(_escape(ship_to.get("postal_code") or "")),
            )

        return (
            "<ord:order>{po}{comment}{fillkill}{pickup}{shipmethod}{consumer}{lines}</ord:order>"
        ).format(
            po=_el("customerponumber", customer_po_number),
            comment=_el("customercomment", customer_comment),
            fillkill=_el("fillkill", fill_kill),
            pickup="<ord:pickup>{}</ord:pickup>".format("true" if pickup else "false"),
            shipmethod=_el("shippingmethod", shipping_method),
            consumer=consumer,
            lines="".join(lines),
        )

    # -- operations ----------------------------------------------------------

    def preview_order(self, line_items: typing.Sequence[typing.Dict], **kwargs: typing.Any) -> typing.Dict:
        """
        Price and shipping-quote a cart WITHOUT committing anything. Safe to call repeatedly.
        """
        body = "<ord:previeworderrequest>{loc}{cart}</ord:previeworderrequest>".format(
            loc=_el("locationnumber", self.location),
            cart=self._cart_xml(line_items, **kwargs),
        )
        return self._parse_preview(self._post("previewOrder", body))

    def place_order(self, line_items: typing.Sequence[typing.Dict], **kwargs: typing.Any) -> typing.Dict:
        """
        Commit the order. Returns ``{"confirmation_number": ...}`` -- ATD's placeOrder response
        carries nothing else, so anything richer has to come from a follow-up getOrderDetail.
        """
        body = "<ord:placeorderrequest>{loc}{cart}</ord:placeorderrequest>".format(
            loc=_el("locationnumber", self.location),
            cart=self._cart_xml(line_items, **kwargs),
        )
        root = self._post("placeOrder", body)
        order = _find(root, "order")
        return {
            "confirmation_number": _text(order, "confirmationnumber"),
            "raw": ET.tostring(root, encoding="unicode")[:20000],
        }

    def get_order_detail(self, confirmation_number: str) -> typing.Dict:
        """Full status for one order: totals, lines, and per-fulfillment tracking."""
        body = (
            "<ord:getorderdetailrequest>{loc}{conf}</ord:getorderdetailrequest>"
        ).format(
            loc=_el("locationnumber", self.location),
            conf=_el("confirmationnumber", confirmation_number),
        )
        return self._parse_order_detail(self._post("getOrderDetail", body))

    # -- parsing -------------------------------------------------------------

    @staticmethod
    def _parse_preview(root: ET.Element) -> typing.Dict:
        order = _find(root, "previeworder") or _find(root, "order")
        lines = []
        for line in _children(order, "orderline"):
            fulfillments = [
                {
                    "status": _text(f, "status"),
                    "quantity": _text(f, "quantity"),
                    "freight": _text(f, "freight"),
                    "ship_method": _text(f, "shipmethod"),
                    "estimated_delivery": _text(f, "estimateddelivery"),
                    "source_dc": _text(f, "sourcedc"),
                    "source_dc_name": _text(f, "sourcedcname"),
                }
                for f in _children(line, "fulfillment")
            ]
            lines.append({
                "cart_line_number": _text(line, "cartlinenumber"),
                "atd_product_number": _text(line, "atdproductnumber"),
                "description": _text(line, "description"),
                "quantity": _text(line, "quantity"),
                "cost": _text(line, "cost"),
                "fet": _text(line, "fet"),
                "line_total": _text(line, "linetotal"),
                "discontinued": _text(line, "discontinued"),
                "replaced": _text(line, "replaced"),
                "fulfillments": fulfillments,
            })
        return {
            "order_total": _text(order, "ordertotal"),
            "threshold_message": _text(order, "thresholdmessage"),
            "customer_po_number": _text(order, "customerponumber"),
            "lines": lines,
            "raw": ET.tostring(root, encoding="unicode")[:20000],
        }

    @staticmethod
    def _parse_order_detail(root: ET.Element) -> typing.Dict:
        detail = _find(root, "orderdetail")
        lines = []
        for line in _children(detail, "orderline"):
            product = _find(line, "product")
            fulfillments = [
                {
                    "status": _text(f, "status"),
                    "quantity": _text(f, "quantity"),
                    "quantity_shipped": _text(f, "quantityshipped"),
                    "quantity_backordered": _text(f, "quantitybackordered"),
                    "line_total": _text(f, "linetotal"),
                    "ship_method": _text(f, "shipmethod"),
                    "carrier_name": _text(f, "carriername"),
                    "shipped_date": _text(f, "shippeddate"),
                    "estimated_delivery_date": _text(f, "estimateddeliverydate"),
                    "actual_delivery_date": _text(f, "actualdeliverydate"),
                    "tracking_number": _text(f, "trackingnumber"),
                    "tracking_url": _text(f, "trackingurl"),
                    "invoice_number": _text(f, "invoicenumber"),
                    "order_number": _text(f, "ordernumber"),
                }
                for f in line.iter()
                if f.tag.rsplit("}", 1)[-1] in ("fulfillment", "detailfulfillment")
            ]
            lines.append({
                "cart_line_number": _text(line, "cartlinenumber"),
                "atd_product_number": _text(product, "atdproductnumber"),
                "product_name": _text(product, "productname"),
                "quantity": _text(product, "quantity"),
                "cost": _text(product, "cost"),
                "fulfillments": fulfillments,
            })
        return {
            "confirmation_number": _text(detail, "confirmationnumber"),
            "customer_po_number": _text(detail, "customerponumber"),
            "subtotal": _text(detail, "subtotal"),
            "freight": _text(detail, "freight"),
            "order_total": _text(detail, "ordertotal"),
            "lines": lines,
            "raw": ET.tostring(root, encoding="unicode")[:20000],
        }
