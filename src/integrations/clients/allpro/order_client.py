"""
AllPro's "Fulfillment Master" (FM) API -- stock check, order submission, cancellation, tracking
and billing. Entirely separate from the FTP catalog feed in ``client.py``: different host,
different protocol, and **different credentials** (see below).

Endpoints, all on one host (``allpro.myshipzone.com`` in production,
``allpro-sandbox.myshipzone.com`` for testing), verified live 2026-10-27/28 -- the sandbox with
Trident's own FM account, production structurally (same four endpoints, byte-identical error
strings, no credentials sent):

  POST /erp/orderproxy            form-encoded  type=stockcheck | ordersend | ordersend_shipfrom
  POST /erp/orderproxy/cancel     raw XML       transaction=ordercancel | itemcancel
  POST /erp/gettracking/details   raw XML       transaction=gettracking
  POST /erp/orderproxy/billing    JSON          auth via HTTP HEADERS, not the body

**The credentials are not the feed's.** Trident's FTP login (apd-9010066) is rejected here with
"Invalid username/password"; FM issues a separate account ("Username and password will be
assigned"). This is the Turn 14 arrangement, not ATD's one-credential-set-for-everything.

Four behaviours worth knowing before reading the code:

**Failures come back HTTP 200.** Every error is a 200 with ``<error msg="..."/>`` as the whole
body. The status code is never the signal; _raise_for_error is.

**"Invalid OW username/password" is not your password.** FM authenticates you, then opens a
session to its own upstream inventory system ("OW") using credentials IT stores for your
account. When that mapping is missing the request dies there, and the message names a password
so it reads exactly like a credential failure. Proven against the sandbox: the same error comes
back for an empty request with no <part> at all, for AllPro's own documented example part, and
for all nine branch values -- i.e. before anything in our payload is looked at. It gets its own
exception class so a dealer is told to call AllPro rather than re-enter a correct password.

**One order, one branch.** Mixed ``branch`` values across <part> elements are rejected outright,
so a cart spanning AllPro's three warehouses (600 Virginia / 800 Florida / 900 Alabama) must be
submitted as separate orders. A blank branch means "all locations" on a stock check.

**There is no freight quote.** Nothing in this API prices shipping before an order: stockcheck
returns cost/list/core only, ``shipcost`` on an order is an INPUT (and only read when the account
is configured pay_shipping), and real freight first appears after shipment, as ``cost`` on
tracking and ``ship_cost`` on billing.
"""
import datetime
import logging
import typing
import xml.etree.ElementTree as ET

import requests
from django.conf import settings

from src.integrations.clients.allpro import exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[ALLPRO-FM-CLIENT]"

DEFAULT_BASE_URL = "https://allpro.myshipzone.com"
SANDBOX_BASE_URL = "https://allpro-sandbox.myshipzone.com"

REQUEST_TIMEOUT_SECONDS = 90  # a stock check pages through their upstream; it is not fast

# AllPro's three warehouses, as the feed's own columns name them (see
# master_parts._allpro_warehouse_availability, confirmed live). Blank = every location.
BRANCH_ALL = ""
BRANCH_NAMES = {"600": "Virginia", "800": "Florida", "900": "Alabama"}

# Substring of the upstream-provisioning failure described in the module docstring. Matched as a
# substring because their message carries a trailing script line number that will move.
_UPSTREAM_FAULT = "invalid ow username/password"
# FM's own credential rejection, by contrast, is this exact whole message.
_AUTH_FAULT = "invalid username/password"

_SHIPPERS = ("ups", "fedex")


def _escape(value: typing.Any) -> str:
    """The five entities their parser requires. Unescaped, the request fails to parse."""
    if value is None:
        return ""
    return (
        str(value)
        .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        .replace('"', "&quot;").replace("'", "&apos;")
    )


def _attr(name: str, value: typing.Any) -> str:
    """One XML attribute, omitted entirely when empty -- their parser treats "" as a real value
    for some fields (shipper in particular, which must be absent rather than blank)."""
    if value is None or str(value) == "":
        return ""
    return ' {}="{}"'.format(name, _escape(value))


def _text(element: typing.Optional[ET.Element], attribute: str) -> typing.Optional[str]:
    if element is None:
        return None
    value = element.get(attribute)
    return value.strip() if isinstance(value, str) else None


def _iter_local(root: ET.Element, tag: str) -> typing.List[ET.Element]:
    """Elements by LOCAL name anywhere in the tree. Their responses are sometimes namespaced and
    sometimes not, and the tracking payload nests one level deeper than the others."""
    return [el for el in root.iter() if el.tag.rsplit("}", 1)[-1] == tag]


def _date(value: typing.Any) -> typing.Optional[datetime.date]:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


class AllProFulfillmentClient:
    def __init__(
        self,
        credentials: typing.Dict,
        sandbox: typing.Optional[bool] = None,
        base_url: typing.Optional[str] = None,
    ) -> None:
        creds = credentials or {}
        self.username = str(creds.get("username") or "").strip()
        self.password = str(creds.get("password") or "")

        missing = [k for k, v in (("username", self.username), ("password", self.password)) if not v]
        if missing:
            raise ValueError(
                "Invalid AllPro Fulfillment Master credentials -- missing: {}.".format(", ".join(missing))
            )

        if sandbox is None:
            sandbox = bool(getattr(settings, "ALLPRO_FM_USE_SANDBOX", False))
        configured = base_url or getattr(settings, "ALLPRO_FM_BASE_URL", None)
        # Production by default, so a dealer arriving with real credentials validates against the
        # host those credentials belong to without anyone remembering to flip a setting.
        self.base_url = str(
            configured or (SANDBOX_BASE_URL if sandbox else DEFAULT_BASE_URL)
        ).rstrip("/")
        self.sandbox = sandbox

    # -- transport -----------------------------------------------------------

    def _request(
        self,
        path: str,
        operation: str,
        data: typing.Optional[typing.Any] = None,
        json_body: typing.Optional[typing.Any] = None,
        headers: typing.Optional[typing.Dict] = None,
    ) -> requests.Response:
        url = "{}{}".format(self.base_url, path)
        try:
            return requests.post(
                url, data=data, json=json_body, headers=headers or {},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.RequestException as e:
            raise exceptions.AllProAPIConnectionError(
                "{} request to {} failed: {}".format(operation, url, str(e))
            )

    def _parse_xml(self, response: requests.Response, operation: str) -> ET.Element:
        try:
            root = ET.fromstring((response.content or b"").strip())
        except ET.ParseError:
            raise exceptions.AllProAPIError(
                "{} returned unparseable XML (HTTP {}): {}".format(
                    operation, response.status_code, (response.text or "")[:300]
                )
            )
        self._raise_for_error(root, operation)
        return root

    def _raise_for_error(self, root: ET.Element, operation: str) -> None:
        """
        Their whole error channel. A failure is a 200 whose entire body is ``<error msg="..."/>``,
        so nothing here may look at the status code.
        """
        errors = _iter_local(root, "error")
        if root.tag.rsplit("}", 1)[-1] == "error":
            errors = [root] + [e for e in errors if e is not root]
        if not errors:
            return
        message = (_text(errors[0], "msg") or "unknown error").strip()
        lowered = message.lower()
        if _UPSTREAM_FAULT in lowered:
            raise exceptions.AllProUpstreamNotProvisionedError(message)
        if _AUTH_FAULT in lowered or "missing username or password" in lowered:
            raise exceptions.AllProAPIAuthError(message)
        raise exceptions.AllProAPIError("{} failed: {}".format(operation, message))

    def _orderproxy(self, type_: str, xml: str, operation: str) -> ET.Element:
        # requests percent-encodes the body, so a literal "+" in a part number arrives as %2B and
        # decodes back to "+". The docs warn that a raw "+" would be read as a space and is
        # auto-corrected only on partno -- that warning is for hand-built bodies, not this one.
        response = self._request(
            "/erp/orderproxy", operation, data={"type": type_, "orderxml": xml}
        )
        return self._parse_xml(response, operation)

    def _msz(self, path: str, xml: str, operation: str) -> ET.Element:
        response = self._request(
            path, operation, data=xml.encode("utf-8"),
            headers={"Content-Type": "application/xml; charset=UTF-8"},
        )
        return self._parse_xml(response, operation)

    # -- request building ----------------------------------------------------

    def _header(self, **extra: typing.Any) -> str:
        attrs = "".join(_attr(k, v) for k, v in extra.items())
        return '<header username="{}" password="{}"{} />'.format(
            _escape(self.username), _escape(self.password), attrs
        )

    def _parts_xml(self, line_items: typing.Sequence[typing.Dict], branch: str) -> str:
        return "".join(
            "<part{}{}{}{}{} ></part>".format(
                _attr("linecode", item.get("linecode")),
                _attr("partno", item.get("partno")),
                _attr("qtyreq", item.get("qtyreq")),
                _attr("cost", item.get("cost")),
                _attr("branch", branch),
            )
            for item in line_items
        )

    def _order_xml(
        self,
        line_items: typing.Sequence[typing.Dict],
        branch: str = BRANCH_ALL,
        po_number: typing.Optional[str] = None,
        order_ref: typing.Optional[str] = None,
        ship_to: typing.Optional[typing.Dict] = None,
        ship_from: typing.Optional[typing.Dict] = None,
        ship_method: typing.Optional[str] = None,
        general_comment: typing.Optional[str] = None,
        shipper: typing.Optional[str] = None,
        shipper_account: typing.Optional[str] = None,
        ship_cost: typing.Optional[typing.Any] = None,
        fill_flag: str = "shipcomplete",
        order_type: str = "delivery",
        del_method: str = "",
    ) -> str:
        """
        The ``<order>`` document shared by stockcheck and ordersend -- deliberately one builder,
        because a stock check that differs from the submitted order quotes something nobody buys.
        """
        if (shipper and not shipper_account) or (shipper_account and not shipper):
            raise ValueError(
                "AllPro requires shipper and shipper_account together or not at all."
            )
        if shipper and str(shipper).lower() not in _SHIPPERS:
            raise ValueError(
                'AllPro shipper must be exactly "ups" or "fedex" (lowercase); got {!r}.'.format(shipper)
            )

        comments = ""
        if ship_method:
            comments += '<comment type="ship" text="{}" />'.format(_escape(ship_method))
        if general_comment:
            comments += '<comment type="general" text="{}" />'.format(_escape(general_comment))

        def address(tag: str, a: typing.Dict) -> str:
            return "<{tag}{c}{a1}{a2}{city}{state}{zip}{phone}{email} />".format(
                tag=tag,
                c=_attr("customer", a.get("customer")),
                a1=_attr("address1", a.get("address1")),
                a2=_attr("address2", a.get("address2")),
                city=_attr("city", a.get("city")),
                state=_attr("state", a.get("state")),
                zip=_attr("zip", a.get("zip")),
                phone=_attr("phonenumber", a.get("phonenumber")),
                email=_attr("email", a.get("email")),
            )

        return (
            '<?xml version="1.0"?><order>{header}{comments}{shipto}{shipfrom}{parts}</order>'
        ).format(
            header=self._header(
                type=order_type, fillflag=fill_flag, delmethod=del_method,
                ponumber=po_number, order_ref=order_ref,
                shipper=(shipper or "").lower() or None, shipper_account=shipper_account,
                shipcost=ship_cost,
            ),
            comments=comments,
            shipto=address("shipto", ship_to) if ship_to else "",
            shipfrom=address("shipfrom", ship_from) if ship_from else "",
            parts=self._parts_xml(line_items, branch),
        )

    # -- operations ----------------------------------------------------------

    def stock_check(
        self, line_items: typing.Sequence[typing.Dict], branch: str = BRANCH_ALL, **kwargs: typing.Any
    ) -> typing.List[typing.Dict]:
        """
        Availability and pricing for one or more parts. Commits nothing; safe to call repeatedly.
        ``branch=""`` (the default) asks across every location.
        """
        xml = self._order_xml(line_items, branch=branch, **kwargs)
        root = self._orderproxy("stockcheck", xml, "stockcheck")
        return [self._parse_stock_part(part) for part in _iter_local(root, "part")]

    def place_order(
        self,
        line_items: typing.Sequence[typing.Dict],
        po_number: str,
        branch: str = BRANCH_ALL,
        ship_from: typing.Optional[typing.Dict] = None,
        **kwargs: typing.Any
    ) -> typing.Dict:
        """Submits a real order. ``ship_from`` switches the request to ordersend_shipfrom."""
        xml = self._order_xml(
            line_items, branch=branch, po_number=po_number, ship_from=ship_from, **kwargs
        )
        type_ = "ordersend_shipfrom" if ship_from else "ordersend"
        root = self._orderproxy(type_, xml, type_)
        return self._parse_order_confirmation(root)

    def cancel_order(self, po_numbers: typing.Sequence[str]) -> typing.List[typing.Dict]:
        """Cancels whole orders. Refused once a tracking number exists."""
        cancels = "".join(
            '<cancel ponumber="{}"></cancel>'.format(_escape(po)) for po in po_numbers
        )
        xml = (
            '<?xml version="1.0"?><MSZ Rev="1.0"><order>{header}{cancels}</order></MSZ>'
        ).format(header=self._header(transaction="ordercancel", status=""), cancels=cancels)
        root = self._msz("/erp/orderproxy/cancel", xml, "ordercancel")
        return self._parse_cancels(root)

    def cancel_line_items(
        self, po_number: str, line_items: typing.Sequence[typing.Dict]
    ) -> typing.List[typing.Dict]:
        """Cancels individual lines, leaving the rest of the order standing."""
        parts = "".join(
            '<part linecode="{}" partno="{}"></part>'.format(
                _escape(i.get("linecode")), _escape(i.get("partno"))
            )
            for i in line_items
        )
        xml = (
            '<?xml version="1.0"?><MSZ Rev="1.0"><order>{header}'
            '<cancel ponumber="{po}">{parts}</cancel></order></MSZ>'
        ).format(header=self._header(transaction="itemcancel", status=""),
                 po=_escape(po_number), parts=parts)
        root = self._msz("/erp/orderproxy/cancel", xml, "itemcancel")
        return self._parse_cancels(root)

    def get_tracking(self, po_numbers: typing.Sequence[str]) -> typing.Dict[str, typing.Dict]:
        """
        Shipment status per order, keyed by PO number.

        The request attribute is spelled ``invoicenumber``, but the value really is the PO number
        -- a holdover their own spec flags. Confirmed live: sending invoicenumber="PO-100293"
        comes back as <part ponumber="PO-100293">.
        """
        parts = "".join(
            '<part invoicenumber="{}" status=""></part>'.format(_escape(po)) for po in po_numbers
        )
        xml = (
            '<?xml version="1.0"?><MSZ Rev="1.0"><order>{header}{parts}</order></MSZ>'
        ).format(header=self._header(transaction="gettracking", status=""), parts=parts)
        root = self._msz("/erp/gettracking/details", xml, "gettracking")
        return self._parse_tracking(root)

    def get_billing(
        self,
        start_date: typing.Optional[str] = None,
        end_date: typing.Optional[str] = None,
        customer_po: typing.Optional[str] = None,
    ) -> typing.List[typing.Dict]:
        """
        Invoiced line items, either for a date range or for one PO (a PO wins and the dates are
        ignored). JSON rather than XML, and the credentials ride in HTTP HEADERS.

        Note this endpoint does NOT validate credentials: measured against the sandbox, a wrong
        password returns the same empty array as a correct one. An empty result therefore cannot
        be read as proof the credentials are good, which is why it is not used for validation.
        """
        body: typing.Dict[str, typing.Any] = {}
        if customer_po:
            body["customer_po"] = customer_po
        else:
            body["start_date"] = start_date
            body["end_date"] = end_date
        response = self._request(
            "/erp/orderproxy/billing", "billing", json_body=body,
            headers={"username": self.username, "password": self.password},
        )
        try:
            data = response.json()
        except ValueError:
            raise exceptions.AllProAPIError(
                "billing returned non-JSON (HTTP {}): {}".format(
                    response.status_code, (response.text or "")[:300]
                )
            )
        if isinstance(data, dict) and data.get("error"):
            raise exceptions.AllProAPIError("billing failed: {}".format(data.get("error")))
        return data if isinstance(data, list) else []

    # -- parsing -------------------------------------------------------------

    @staticmethod
    def _parse_stock_part(part: ET.Element) -> typing.Dict:
        return {
            "partno": _text(part, "partno"),
            "linecode": _text(part, "linecode"),
            "mfgcode": _text(part, "mfgcode"),
            "kit": _text(part, "kit"),
            "uom": _text(part, "uom"),
            "desc": _text(part, "desc"),
            "qtyavail": _text(part, "qtyavail"),
            "qtyreq": _text(part, "qtyreq"),
            "cost": _text(part, "cost"),
            "list": _text(part, "list"),
            "core": _text(part, "core"),
            "branch": _text(part, "branch"),
            # Documented as "currently always returned as 1" -- kept verbatim, never treated as
            # a delivery estimate.
            "deliverytime": _text(part, "deliverytime"),
            "linenum": _text(part, "linenum"),
            "errcode": _text(part, "errcode"),
        }

    @staticmethod
    def _parse_order_confirmation(root: ET.Element) -> typing.Dict:
        header = next(iter(_iter_local(root, "header")), None)
        lines = []
        for part in _iter_local(root, "part"):
            lines.append({
                "partno": _text(part, "partno"),
                "linecode": _text(part, "linecode"),
                "branch": _text(part, "branch"),
                "branchname": _text(part, "branchname"),
                "qtyreq": _text(part, "qtyreq"),
                "qtysup": _text(part, "qtysup"),
                "orderno": _text(part, "orderno"),
                "errcode": _text(part, "errcode"),
            })
        return {
            "state": _text(header, "state"),
            "ponumber": _text(header, "ponumber"),
            "orderdate": _text(header, "orderdate"),
            "errcode": _text(header, "errcode"),
            "lines": lines,
            "raw": ET.tostring(root, encoding="unicode")[:20000],
        }

    @staticmethod
    def _parse_cancels(root: ET.Element) -> typing.List[typing.Dict]:
        out = []
        for cancel in _iter_local(root, "cancel"):
            entry = {
                "ponumber": _text(cancel, "ponumber"),
                "status": _text(cancel, "status"),
                "trackingnumber": _text(cancel, "trackingnumber"),
                # error is the STRING "true"/"false", not a boolean -- a cancel that was refused
                # still returns HTTP 200 with status="success" on the header.
                "error": (_text(cancel, "error") or "").lower() == "true",
                "msg": _text(cancel, "msg"),
                "parts": [
                    {
                        "linecode": _text(p, "linecode"),
                        "partno": _text(p, "partno"),
                        "status": _text(p, "status"),
                        "error": (_text(p, "error") or "").lower() == "true",
                        "msg": _text(p, "msg"),
                    }
                    for p in _iter_local(cancel, "part")
                ],
            }
            out.append(entry)
        return out

    @staticmethod
    def _parse_tracking(root: ET.Element) -> typing.Dict[str, typing.Dict]:
        orders: typing.Dict[str, typing.Dict] = {}
        for part in _iter_local(root, "part"):
            po = _text(part, "ponumber") or _text(part, "invoicenumber")
            if not po:
                continue
            items = []
            # The 7.27.2026 spec returns <item>; the earlier per-API document shows <items>, and
            # explicitly flags that the format changed. Both are accepted so neither version of
            # their stack silently yields an order with no lines.
            for item in part:
                if item.tag.rsplit("}", 1)[-1] not in ("item", "items"):
                    continue
                items.append({
                    "brand": _text(item, "brand"),
                    "sku": _text(item, "sku"),
                    "kit_sku": _text(item, "kit_sku"),
                    "service": _text(item, "service"),
                    "trackingnumber": _text(item, "trackingnumber"),
                    "cost": _text(item, "cost"),
                    "qty": _text(item, "qty"),
                    "status": _text(item, "status"),
                    "ship_date": _date(_text(item, "ship_date")),
                    "package_weight": _text(item, "package_weight"),
                    "gross_weight": _text(item, "gross_weight"),
                    "package_width": _text(item, "package_width"),
                    "package_length": _text(item, "package_length"),
                    "package_height": _text(item, "package_height"),
                })
            orders[po] = {
                "ponumber": po,
                # Present on the <part> itself when the order has no line detail yet.
                "status": _text(part, "status"),
                "service": _text(part, "service"),
                "trackingnumber": _text(part, "trackingnumber"),
                "items": items,
            }
        return orders
