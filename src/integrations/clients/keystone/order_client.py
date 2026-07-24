"""
Transport client for Keystone Automotive's "Electronic Order Web Service" (SOAP 1.1, ASMX) —
shipping quotes, order placement, and order-status/tracking history. Separate from ``client.py``
(the existing implicit-FTPS catalog/pricing client used by the nightly sync pipeline) since this
targets a completely different capability (write operations) over a completely different
transport (SOAP over HTTP vs. FTP) and credential shape (security key + account number, vs.
ftp_user/ftp_password).

No SOAP library (zeep, suds, ...) is installed in this project, and none is warranted for the
three RPC-style calls this client needs — envelopes are built and parsed by hand with
``requests`` + the stdlib ``xml.etree.ElementTree``.

Auth is simpler than Turn 14's OAuth2 flow: every call carries a static ``Key`` (a 36-byte
security key — separate values for test vs. production, IP-whitelisted per key by Keystone) and
``FullAccountNo``. No token exchange or refresh.

SAFETY: ``ship_order_dropship_multiple_parts(order_process_method=1)`` places a REAL order
against Keystone (``order_process_method=0`` is a non-committal proposed-freight check only).
Callers must never invoke it with ``order_process_method=1`` except through an explicit,
user-approved submission — see the Purchase Orders job-queue path this is meant to run behind.
"""
import datetime
import re
import typing
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as _xml_escape

import requests
from django.conf import settings

from common import utils as common_utils
from src.integrations.clients.keystone import exceptions

REQUEST_TIMEOUT_SECONDS = 30
NAMESPACE = "http://eKeystone.com"

# VCPN (Keystone's "full part number"): three-character vendor code + vendor part number.
VCPN_REGEX = re.compile(r"^[A-Z][A-Z0-9]{3,12}$")

_ELECTRONIC_ORDER_URL = settings.KEYSTONE_ORDER_ELECTRONIC_ORDER_URL


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _find_by_local_name(root: ET.Element, local_name: str) -> typing.Optional[ET.Element]:
    for el in root.iter():
        if _local_name(el.tag) == local_name:
            return el
    return None


def _parse_dataset_tables(result_element: ET.Element) -> typing.Dict[str, typing.List[typing.Dict[str, str]]]:
    """
    Parses a .NET DataSet serialized inside a SOAP response (the standard "diffgram" format)
    into ``{table_name: [{column_name: text_value, ...}, ...]}``. Table rows are matched purely
    by local element name (ignoring whatever namespace prefixes the server happens to use), so
    this works across every DataSet-returning method without per-method structural knowledge.
    """
    diffgram = _find_by_local_name(result_element, "diffgram")
    if diffgram is None:
        return {}
    dataset_root = next(iter(diffgram), None)
    if dataset_root is None:
        return {}
    tables: typing.Dict[str, typing.List[typing.Dict[str, str]]] = {}
    for row in dataset_root:
        table_name = _local_name(row.tag)
        columns = {_local_name(col.tag): (col.text or "").strip() for col in row}
        tables.setdefault(table_name, []).append(columns)
    return tables


class KeystoneOrderApiClient(object):
    """One instance per (account_number, security_key) pair — i.e. per CompanyProviders
    connection's ``order`` credentials."""

    def __init__(self, credentials: typing.Dict) -> None:
        self.security_key = credentials.get("security_key", "")
        self.account_number = credentials.get("account_number", "")
        if not self.security_key or not self.account_number:
            raise ValueError("Invalid credentials parameter.")

    # -- Request building / transport ------------------------------------------------------

    @staticmethod
    def _check_no_ampersand(value: str) -> None:
        # Per Keystone's docs: "The '&' character should not be submitted in any of the
        # parameters as it will cause the function to fail" — checked pre-flight for a clearer
        # error than round-tripping to Keystone.
        if value and "&" in value:
            raise ValueError(
                "Value contains '&', which Keystone's Electronic Order API cannot accept: {!r}".format(value)
            )

    def _build_envelope(self, operation: str, params: typing.List[typing.Tuple[str, str]]) -> str:
        body = "".join(
            "<ekey:{tag}>{value}</ekey:{tag}>".format(tag=tag, value=_xml_escape(value or ""))
            for tag, value in params
        )
        return (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:ekey="{ns}">'
            "<soapenv:Body><ekey:{op}>{body}</ekey:{op}></soapenv:Body>"
            "</soapenv:Envelope>"
        ).format(ns=NAMESPACE, op=operation, body=body)

    def _raise_for_fault(self, response_text: str, operation: str) -> None:
        """Auth/authorization failures come back as a SOAP Fault (distinct from the embedded
        "Error: Code NNN..." strings most business-rule rejections use, which arrive in an
        otherwise-200 response and are left for the caller to parse from the result)."""
        try:
            root = ET.fromstring(response_text)
        except ET.ParseError:
            return
        fault = _find_by_local_name(root, "Fault")
        if fault is None:
            return
        faultstring_el = _find_by_local_name(fault, "faultstring")
        message = (faultstring_el.text or "").strip() if faultstring_el is not None else "Unknown SOAP fault"
        lower = message.lower()
        if "not authorized" in lower:
            raise exceptions.KeystoneOrderPermissionError(message)
        if "illegal use" in lower:
            raise exceptions.KeystoneOrderAuthError(message)
        raise exceptions.KeystoneOrderAPIException("Keystone {} SOAP fault: {}".format(operation, message))

    def _request(self, operation: str, params: typing.List[typing.Tuple[str, str]]) -> str:
        envelope = self._build_envelope(operation, params)
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": '"{}/{}"'.format(NAMESPACE, operation),
        }
        try:
            response = requests.post(
                _ELECTRONIC_ORDER_URL,
                data=envelope.encode("utf-8"),
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.ConnectTimeout as e:
            raise exceptions.KeystoneOrderAPIException(
                "Connect timeout calling Keystone {}: {}".format(operation, common_utils.get_exception_message(e))
            )
        except requests.RequestException as e:
            raise exceptions.KeystoneOrderAPIException(
                "Request exception calling Keystone {}: {}".format(operation, common_utils.get_exception_message(e))
            )

        # SOAP 1.1 over HTTP returns 500 for a SOAP Fault — check for one before treating a
        # non-200 status as a hard transport failure.
        self._raise_for_fault(response.text, operation)
        if response.status_code not in (200, 500):
            raise exceptions.KeystoneOrderAPIException(
                "Unexpected HTTP status calling Keystone {} (status_code={}): {}".format(
                    operation, response.status_code, response.text[:2000]
                )
            )
        return response.text

    def _call_dataset(
        self, operation: str, extra_params: typing.List[typing.Tuple[str, str]]
    ) -> typing.Dict[str, typing.List[typing.Dict[str, str]]]:
        params = [("Key", self.security_key), ("FullAccountNo", self.account_number)] + extra_params
        response_text = self._request(operation, params)
        try:
            root = ET.fromstring(response_text)
        except ET.ParseError as e:
            raise exceptions.KeystoneOrderAPIException(
                "Could not parse Keystone {} response as XML ({}). Raw response: {}".format(
                    operation, e, response_text[:2000]
                )
            )
        result = _find_by_local_name(root, "{}Result".format(operation))
        if result is None:
            raise exceptions.KeystoneOrderAPIException(
                "Keystone {} response missing {}Result element. Raw response: {}".format(
                    operation, operation, response_text[:2000]
                )
            )
        return _parse_dataset_tables(result)

    def _call_string(self, operation: str, extra_params: typing.List[typing.Tuple[str, str]]) -> str:
        """Like _call_dataset, but for operations whose SOAP return type is a plain string
        (e.g. CheckInventory) rather than a DataSet/diffgram."""
        params = [("Key", self.security_key), ("FullAccountNo", self.account_number)] + extra_params
        response_text = self._request(operation, params)
        try:
            root = ET.fromstring(response_text)
        except ET.ParseError as e:
            raise exceptions.KeystoneOrderAPIException(
                "Could not parse Keystone {} response as XML ({}). Raw response: {}".format(
                    operation, e, response_text[:2000]
                )
            )
        result = _find_by_local_name(root, "{}Result".format(operation))
        if result is None:
            raise exceptions.KeystoneOrderAPIException(
                "Keystone {} response missing {}Result element. Raw response: {}".format(
                    operation, operation, response_text[:2000]
                )
            )
        return (result.text or "").strip()

    # -- Inventory --------------------------------------------------------------------------

    def check_inventory(self, full_part_no: str) -> str:
        """CheckInventory - one VCPN, live inventory across every warehouse this account can
        see. Returns the raw result string: either "WAREHOUSE,QTY,WAREHOUSE,QTY,..." (e.g.
        "EAST,0,MIDWEST,3,...") or one of Keystone's plain-text business errors ("Invalid part
        number.", "Part is blocked.") - parsing/error detection is the caller's job (see
        src.integrations.live_inventory.keystone), since this is a bare string, not a
        DataSet with its own ErrorMessage column like CheckInventoryBulk has."""
        return self._call_string("CheckInventory", [("FullPartNo", full_part_no)])

    # -- Shipping quote -----------------------------------------------------------------

    def get_shipping_options_multiple_parts_per_warehouse(
        self, part_numbers_qty: str, to_zip: typing.Optional[str] = None
    ) -> typing.Dict[str, typing.List[typing.Dict[str, str]]]:
        """GetShippingOptionsMultiplePartsPerWarehouse. Non-binding — safe to call freely.
        ``part_numbers_qty`` is a pipe-delimited ``K,VCPN,QTY|K,VCPN,QTY`` string (search type
        "K" = VCPN, per Keystone's docs)."""
        return self._call_dataset(
            "GetShippingOptionsMultiplePartsPerWarehouse",
            [("PartNumbersQty", part_numbers_qty), ("ToZip", to_zip or "")],
        )

    # -- Pricing ----------------------------------------------------------------------------

    def check_price_bulk(self, full_part_no: str) -> typing.Dict[str, typing.List[typing.Dict[str, str]]]:
        """CheckPriceBulk (confirmed against Keystone's own WSDL service description — their
        SDK PDF's SOAP sample mislabels this operation "CheckPrice" and the VB.NET stub calls
        it "GetCheckPrice"; the real wire operation name is "CheckPriceBulk"). ``full_part_no``
        is a comma-separated list of VCPNs — Keystone's docs cap this at 12 per call; caller is
        responsible for chunking (see KeystoneOrderAdapter._get_prices).

        RATE LIMIT: Keystone's docs say this "should be used no more than once per hour" and
        warn that overuse of it specifically "may [get your] account suspended" — much
        stricter than every other method here. Never call this directly from a per-quote code
        path without the caching layer in KeystoneOrderAdapter._get_prices sitting in front of
        it."""
        return self._call_dataset("CheckPriceBulk", [("FullPartNo", full_part_no)])

    # -- Order (SUBMIT when order_process_method=1 — real order placement, see module docstring) --

    def ship_order_dropship_multiple_parts(
        self,
        order_process_method: int,
        part_number_quantity: str,
        drop_ship: typing.Dict[str, str],
        po_number: str,
        service_level: str = "",
    ) -> typing.Dict[str, typing.List[typing.Dict[str, str]]]:
        """ShipOrderDropShipMultipleParts. ``order_process_method``: 0 = verify only (proposed
        freight, no order placed), 1 = place the order for real. ``part_number_quantity`` is a
        pipe-delimited ``VCPN,QTY|VCPN,QTY`` string. ``drop_ship`` keys: first_name, last_name,
        (optional) middle_initial/company/address2/phone/email, address1, city, state,
        postal_code, country (defaults "US" if omitted, per Keystone's own default)."""
        self._check_no_ampersand(part_number_quantity)
        self._check_no_ampersand(po_number)
        for v in drop_ship.values():
            self._check_no_ampersand(v)

        return self._call_dataset(
            "ShipOrderDropShipMultipleParts",
            [
                ("OrderProcessMethod", str(order_process_method)),
                ("PartNumberQuantity", part_number_quantity),
                ("DropShipFirstName", drop_ship.get("first_name", "")),
                ("DropShipMiddleInitial", drop_ship.get("middle_initial", "")),
                ("DropShipLastName", drop_ship.get("last_name", "")),
                ("DropShipCompany", drop_ship.get("company", "")),
                ("DropShipAddress1", drop_ship.get("address1", "")),
                ("DropShipAddress2", drop_ship.get("address2", "")),
                ("DropShipCity", drop_ship.get("city", "")),
                ("DropShipState", drop_ship.get("state", "")),
                ("DropShipPostalCode", drop_ship.get("postal_code", "")),
                ("DropShipPhone", drop_ship.get("phone", "")),
                ("DropShipCountry", drop_ship.get("country") or "US"),
                ("DropShipEmail", drop_ship.get("email", "")),
                ("PONumber", po_number),
                ("AdditionalInfo", ""),
                ("ServiceLevel", service_level),
            ],
        )

    # -- Status / tracking ----------------------------------------------------------------

    def get_order_history(
        self, po_number: str = "", from_date: str = "", to_date: str = ""
    ) -> typing.Dict[str, typing.List[typing.Dict[str, str]]]:
        """GetOrderHistory. ``from_date``/``to_date`` in YYYYMMDD format. ``po_number`` alone is
        sufficient (date range is optional when a PO number is given, per Keystone's docs)."""
        return self._call_dataset(
            "GetOrderHistory",
            [("PONumber", po_number), ("FromDate", from_date), ("ToDate", to_date)],
        )

    def test_connection(self) -> None:
        """Cheap connectivity/auth probe for the connect-time validator — Keystone's order API
        has no dedicated ping method, so this calls GetOrderHistory over a narrow 1-day window;
        any non-fault response (including "no orders found") proves the key/account combination
        is valid and IP-whitelisted."""
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        self.get_order_history(from_date=yesterday.strftime("%Y%m%d"), to_date=today.strftime("%Y%m%d"))
