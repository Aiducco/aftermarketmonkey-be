"""
Builds the X12 005010 850 Purchase Order we send Rough Country.

Deliberately free of Django and of the order-adapter dataclasses: everything here is a pure
function over the small input types below, so the whole document can be built and asserted in a
unit test without a database, a connection, or a PurchaseOrder. The adapter
(``src/integrations/orders/rough_country.py``) does the mapping and owns the side effects.

Shaped from the real round trip of 16-19 Oct 2026 rather than from Alluvia's August sample,
which is wrong in three places (``CTT*1`` for two lines, a 4010-era ``ISA11``, and a ``GS04`` a
day behind ``ISA09``). Differences from that sample, on purpose:

* **CTT01 is computed** from the PO1 count, never passed in.
* **No ``SAC``.** The sample sends ``SAC*C*D500***895`` — us dictating an $8.95 charge on our own
  purchase order. We do not know what ``D500`` means and Rough Country prices the order from
  their own system regardless (confirmed: we sent 33.22, they invoiced 33.2168), so we omit it.
* **``TD5-05`` carries one of their numeric codes** (55/58/78/79), not the sample's ``FG``.

One hard rule: ``BEG03`` must not be truncated. Every document Rough Country sends back echoes
it — ``BAK03`` on the 855, ``PRF01`` on the 856, ``BIG04`` on the 810 — and it is the only key we
have to match any of them to a PurchaseOrder. A silently shortened PO number means a confirmed
order whose acknowledgment, tracking and invoice can never be attached to it, so an over-long
one raises instead.
"""
import dataclasses
import datetime
import decimal
import typing

from src.integrations.edi import exceptions as edi_exceptions
from src.integrations.edi import x12

# Rough Country's own TD5-05 values, supplied by Alluvia on 21 Oct 2026. Their list also
# includes "ZZ" = Custom, deliberately not offered here: Custom presumably needs a free-text
# description somewhere in the document and we have not been told where, so exposing an option
# we cannot fully populate would produce orders that route unpredictably.
#
# Their own 856 and 810 came back with TD5-05 / CAD04 = "99" ("Shopground"), which is on none of
# the lists we have been given. Inbound parsing therefore treats these as opaque strings; this
# tuple constrains only what we SEND.
SHIP_METHOD_CODES: typing.Tuple[typing.Tuple[str, str], ...] = (
    ("55", "FedEx Ground"),
    ("58", "FedEx Home Delivery"),
    ("78", "FedEx 2nd Day"),
    ("79", "FedEx Next Day Air"),
)
SHIP_METHOD_NAMES = dict(SHIP_METHOD_CODES)
DEFAULT_SHIP_METHOD_CODE = "55"

# X12 005010 element maxima for the fields we populate. Enforced rather than assumed: the
# standard has no escaping and no truncation semantics, so an over-long element is simply a
# malformed document that their translator may accept, reject, or silently cut.
_MAX_PO_NUMBER = 22  # BEG03
_MAX_CONTACT_NAME = 60  # PER02
_MAX_COMM_NUMBER = 80  # PER04 / PER06
_MAX_NAME = 60  # N102
_MAX_IDENTIFIER = 80  # N104
_MAX_ADDRESS = 55  # N301 / N302
_MAX_CITY = 30  # N401
_MAX_POSTAL = 15  # N403
_MAX_PART_NUMBER = 48  # PO1-07 / PO1-09
_MAX_DESCRIPTION = 80  # PID05

_TWO_PLACES = decimal.Decimal("0.01")


@dataclasses.dataclass(frozen=True)
class Po850Line:
    """One PO1/PID pair. ``line_number`` is PO1-01 and is 1-based and contiguous."""

    line_number: int
    quantity: int
    rough_country_part_number: str  # PO1-07, under the "VC" qualifier
    our_part_number: str  # PO1-09, under "SK"
    unit_price: typing.Optional[decimal.Decimal] = None  # PO1-04
    description: str = ""  # PID05
    unit_of_measure: str = "EA"  # PO1-03


@dataclasses.dataclass(frozen=True)
class Po850ShipTo:
    """The N1*ST consignee block."""

    name: str
    address1: str
    city: str
    state: str
    postal_code: str
    address2: str = ""
    country: str = "US"
    # N104 under qualifier 92. Alluvia's sample puts the literal "DROPSHIP CUSTOMER" here and
    # their 855/856/810 echo it back verbatim, so it reads as a marker rather than an
    # identifier we are expected to populate (Q15 in the plan doc is still open).
    identifier: str = "DROPSHIP CUSTOMER"


@dataclasses.dataclass(frozen=True)
class Po850Contact:
    """The PER*OC order-contact block. Omitted entirely when there is no name to put in it."""

    name: str
    phone: str = ""
    email: str = ""


@dataclasses.dataclass(frozen=True)
class Po850Request:
    po_number: str
    lines: typing.Sequence[Po850Line]
    ship_to: Po850ShipTo
    contact: typing.Optional[Po850Contact] = None
    order_date: typing.Optional[datetime.date] = None
    # DTM*038, "ship no later than". Omitted when None.
    ship_no_later_than: typing.Optional[datetime.date] = None
    ship_method_code: str = DEFAULT_SHIP_METHOD_CODE
    # BEG01 (00 = original) and BEG02. "DS" is dropship, which is the only value Alluvia has
    # ever shown us. A stock order to the shop's own address would conventionally be "SA", but
    # that is unconfirmed (Q14), so the shop-address case still goes out as DS with the shop in
    # N1*ST -- which fulfils correctly -- rather than guessing a code their map may not have.
    purpose_code: str = "00"
    order_type_code: str = "DS"


def _money(value: decimal.Decimal) -> str:
    """Plain decimal string. ``str()`` on a Decimal can yield scientific notation (1E+2), which
    is not a valid X12 numeric."""
    return format(value.quantize(_TWO_PLACES, rounding=decimal.ROUND_HALF_UP), "f")


def _validate(request: Po850Request) -> None:
    po_number = (request.po_number or "").strip()
    if not po_number:
        raise edi_exceptions.EdiBuildError("An 850 needs a PO number for BEG03.")
    if len(po_number) > _MAX_PO_NUMBER:
        raise edi_exceptions.EdiBuildError(
            "PO number {!r} is {} characters; BEG03 allows {}. Truncating it would make Rough "
            "Country's 855, 856 and 810 unmatchable to this order, since all three echo this "
            "value back as the only join key.".format(po_number, len(po_number), _MAX_PO_NUMBER)
        )
    if x12.scrub(po_number) != po_number:
        raise edi_exceptions.EdiBuildError(
            "PO number {!r} contains an X12 delimiter or control character. Sanitising it would "
            "change the value Rough Country echoes back, breaking the match.".format(po_number)
        )
    if not request.lines:
        raise edi_exceptions.EdiBuildError("An 850 needs at least one line item.")
    for line in request.lines:
        if line.quantity <= 0:
            raise edi_exceptions.EdiBuildError(
                "Line {} has quantity {}; an 850 cannot order a non-positive quantity.".format(
                    line.line_number, line.quantity
                )
            )
        if not (line.rough_country_part_number or "").strip():
            raise edi_exceptions.EdiBuildError(
                "Line {} has no Rough Country part number for PO1-07.".format(line.line_number)
            )


def build_850_segments(request: Po850Request) -> typing.List[x12.Segment]:
    """The ST…SE body, excluding ST and SE themselves — ``x12.build_interchange`` adds those and
    computes SE01, so the segment count cannot drift from the content."""
    _validate(request)
    order_date = (request.order_date or datetime.date.today()).strftime("%Y%m%d")
    ship_to = request.ship_to

    segments = [
        x12.segment(
            "BEG",
            request.purpose_code,
            request.order_type_code,
            request.po_number.strip(),
            "",
            order_date,
        )
    ]

    if request.contact is not None and (request.contact.name or "").strip():
        contact = request.contact
        elements: typing.List[str] = ["OC", x12.scrub(contact.name, _MAX_CONTACT_NAME)]
        if (contact.phone or "").strip():
            elements += ["TE", x12.scrub(contact.phone, _MAX_COMM_NUMBER)]
        if (contact.email or "").strip():
            elements += ["EM", x12.scrub(contact.email, _MAX_COMM_NUMBER)]
        segments.append(x12.segment("PER", *elements))

    if request.ship_no_later_than is not None:
        segments.append(x12.segment("DTM", "038", request.ship_no_later_than.strftime("%Y%m%d")))

    # TD5-04 "ZZ" is Transportation Method = mutually defined; TD5-05 carries their own code.
    segments.append(x12.segment("TD5", "", "", "", "ZZ", (request.ship_method_code or "").strip()))

    segments.append(
        x12.segment(
            "N1",
            "ST",
            x12.scrub(ship_to.name, _MAX_NAME),
            "92",
            x12.scrub(ship_to.identifier, _MAX_IDENTIFIER),
        )
    )
    address_elements = [x12.scrub(ship_to.address1, _MAX_ADDRESS)]
    if (ship_to.address2 or "").strip():
        address_elements.append(x12.scrub(ship_to.address2, _MAX_ADDRESS))
    segments.append(x12.segment("N3", *address_elements))
    segments.append(
        x12.segment(
            "N4",
            x12.scrub(ship_to.city, _MAX_CITY),
            x12.scrub(ship_to.state, 2),
            x12.scrub(ship_to.postal_code, _MAX_POSTAL),
            x12.scrub(ship_to.country or "US", 3),
        )
    )

    total = decimal.Decimal("0")
    priced_every_line = True
    for line in request.lines:
        segments.append(
            x12.segment(
                "PO1",
                str(line.line_number),
                str(line.quantity),
                line.unit_of_measure or "EA",
                _money(line.unit_price) if line.unit_price is not None else "",
                "",
                "VC",
                x12.scrub(line.rough_country_part_number, _MAX_PART_NUMBER),
                "SK",
                x12.scrub(line.our_part_number, _MAX_PART_NUMBER),
            )
        )
        if (line.description or "").strip():
            segments.append(x12.segment("PID", "F", "", "", "", x12.scrub(line.description, _MAX_DESCRIPTION)))
        if line.unit_price is None:
            priced_every_line = False
        else:
            total += line.unit_price * line.quantity

    segments.append(x12.segment("CTT", str(len(request.lines))))
    # AMT*TT is the order total. Emitted only when every line carried a price -- a total that
    # silently omits unpriced lines is worse than no total at all, and the element is optional.
    if priced_every_line:
        segments.append(x12.segment("AMT", "TT", _money(total)))
    return segments


def build_850(
    request: Po850Request,
    sender_id: str,
    receiver_id: str,
    interchange_control_number: str,
    group_control_number: str,
    transaction_control_number: str,
    usage_indicator: str = "T",
    now: typing.Optional[datetime.datetime] = None,
) -> str:
    """The complete ISA…IEA interchange, ready to write to the mailbox."""
    return x12.build_interchange(
        sender_id=sender_id,
        receiver_id=receiver_id,
        interchange_control_number=interchange_control_number,
        group_control_number=group_control_number,
        functional_id_code="PO",
        transactions=[
            (
                "850",
                transaction_control_number,
                build_850_segments(request),
            )
        ],
        usage_indicator=usage_indicator,
        now=now,
    )


def filename_for(po_number: str, now: typing.Optional[datetime.datetime] = None) -> str:
    """Our outbound filename. Alluvia never specified a convention (Q6) and accepted this shape
    on the first try; theirs embeds ``~``, so we keep ours boring and greppable."""
    moment = now or datetime.datetime.utcnow()
    return "850_{}_{}.x12".format(x12.scrub(po_number).replace(" ", "_"), moment.strftime("%Y%m%dT%H%M%SZ"))
