"""
Parses the three documents Rough Country sends us: 855 (acknowledgment), 856 (advance ship
notice) and 810 (invoice).

Pure functions over ``x12.Transaction``, with no Django and no PurchaseOrder — matching
``po_850.py``, so every one of these can be asserted against the real files in
``tests/fixtures/rough_country_edi/`` without a database. Applying the results to a
PurchaseOrder is ``src/integrations/services/rough_country_edi.py``.

What the live round trip of 16-19 Oct 2026 forced into the design:

* **Quantities come from ``ACK02``, not ``PO1-02``.** Alluvia's 855 sample contradicts itself on
  line 2 (``PO1-02`` says 1, ``ACK02`` says 3, the 850 ordered 3). ``ACK02`` is the acknowledged
  quantity by definition, so it wins; ``PO1-02`` is kept only for reporting the disagreement.
* **The 856 must be walked as a tree, not scanned flat.** Items hang off whichever pack actually
  contains them, and a real multi-carton shipment has several ``P`` levels each with its own
  ``MAN*CP``. Scanning flat would attach every tracking number to every item.
* **Neither the 856 nor the 810 carries a PO line number.** ``IT1-01`` is empty and the ASN's
  ``LIN01`` is a Rough Country internal id (``24154877``), joinable to nothing of ours. Matching
  is by part number only, which is why both parsers aggregate by part number — our own kit
  expansion can legitimately put one SKU on two lines of a PO.
* **Money is encoded two different ways in the same document.** ``TDS`` and ``SAC05`` are
  implied-two-decimal integers (``TDS*3322`` is $33.22), while ``IT1-04`` is a plain decimal and
  arrives with FOUR places (``33.2168``). Nothing here rounds a unit price to 2dp — Rough
  Country's price is authoritative and the drift compounds with quantity.
* **Carrier and ship-method codes are opaque.** Their 856 returned ``99`` against a published
  list of 55/58/78/79, so these are carried through as strings and never validated.
"""
import dataclasses
import datetime
import decimal
import logging
import typing

from src.integrations.edi import x12

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ROUGH-COUNTRY-EDI]"

_HL_SHIPMENT = "S"
_HL_ORDER = "O"
_HL_PACK = "P"
_HL_ITEM = "I"


# -- shared helpers --------------------------------------------------------------------------


def _date(value: str) -> typing.Optional[datetime.date]:
    """CCYYMMDD. Returns None rather than raising: a malformed date on an otherwise usable
    invoice should not quarantine the whole document."""
    text = (value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def _int(value: str) -> typing.Optional[int]:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return int(decimal.Decimal(text))
    except (decimal.InvalidOperation, ValueError):
        return None


def _decimal(value: str) -> typing.Optional[decimal.Decimal]:
    """An explicit decimal element (PO1-04, IT1-04). Preserved at full precision."""
    text = (value or "").strip()
    if not text:
        return None
    try:
        return decimal.Decimal(text)
    except decimal.InvalidOperation:
        return None


def _implied_decimal(value: str, places: int = 2) -> typing.Optional[decimal.Decimal]:
    """TDS01 and SAC05 carry no decimal point: ``3322`` means 33.22."""
    text = (value or "").strip()
    if not text:
        return None
    negative = text.startswith("-")
    digits = text.lstrip("+-")
    if not digits.isdigit():
        return None
    result = decimal.Decimal(digits).scaleb(-places)
    return -result if negative else result


def _qualified(segment: x12.Segment, start: int, qualifier: str) -> str:
    """Read the value paired with ``qualifier`` in a segment's repeating
    qualifier/value element pairs — e.g. PO1's ``VC*922*SK*RC-922`` from position 6."""
    position = start
    while position < len(segment.elements):
        if segment.get(position).strip().upper() == qualifier.upper():
            return segment.get(position + 1).strip()
        position += 2
    return ""


# -- 855 Purchase Order Acknowledgment -------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Ack855Line:
    line_number: typing.Optional[int]  # PO1-01
    rough_country_part_number: str  # PO1 "VC"
    our_part_number: str  # PO1 "SK"
    quantity_ordered: typing.Optional[int]  # PO1-02, informational only
    quantity_acknowledged: typing.Optional[int]  # ACK02 — authoritative
    ack_code: str  # ACK01
    unit_price: typing.Optional[decimal.Decimal]  # PO1-04

    @property
    def is_accepted(self) -> bool:
        """``IA`` is the only code Rough Country has ever sent and, per Alluvia on 21 Oct 2026,
        the only one they send at all ("we will only send the initial 855 accepting the order").
        ``IB``/``IQ``/``IR`` are handled anyway because an acknowledgment that silently means
        something else is the expensive kind of surprise."""
        return self.ack_code.upper() in ("IA", "IC")

    @property
    def is_rejected(self) -> bool:
        return self.ack_code.upper() in ("IR", "R1", "R2", "R3")

    @property
    def is_backordered(self) -> bool:
        return self.ack_code.upper() in ("IB", "BP")


@dataclasses.dataclass(frozen=True)
class Ack855:
    po_number: str  # BAK03 — our own PO number, the only join key
    purpose_code: str  # BAK01
    ack_type_code: str  # BAK02
    ack_date: typing.Optional[datetime.date]  # BAK04
    lines: typing.Tuple[Ack855Line, ...]
    declared_line_count: typing.Optional[int]  # CTT01


def parse_855(transaction: x12.Transaction) -> Ack855:
    bak = transaction.find("BAK") or x12.Segment(tag="BAK")
    ctt = transaction.find("CTT")

    lines: typing.List[Ack855Line] = []
    pending: typing.Optional[x12.Segment] = None
    for seg in transaction.body():
        if seg.tag == "PO1":
            if pending is not None:
                # A PO1 with no ACK after it. Recorded with no acknowledged quantity rather
                # than dropped, so the apply step can flag it instead of silently confirming.
                lines.append(_ack_line(pending, None))
            pending = seg
        elif seg.tag == "ACK" and pending is not None:
            lines.append(_ack_line(pending, seg))
            pending = None
    if pending is not None:
        lines.append(_ack_line(pending, None))

    return Ack855(
        po_number=bak.get(3).strip(),
        purpose_code=bak.get(1).strip(),
        ack_type_code=bak.get(2).strip(),
        ack_date=_date(bak.get(4)),
        lines=tuple(lines),
        declared_line_count=_int(ctt.get(1)) if ctt else None,
    )


def _ack_line(po1: x12.Segment, ack: typing.Optional[x12.Segment]) -> Ack855Line:
    return Ack855Line(
        line_number=_int(po1.get(1)),
        rough_country_part_number=_qualified(po1, 6, "VC"),
        our_part_number=_qualified(po1, 6, "SK"),
        quantity_ordered=_int(po1.get(2)),
        quantity_acknowledged=_int(ack.get(2)) if ack is not None else None,
        ack_code=(ack.get(1).strip() if ack is not None else ""),
        unit_price=_decimal(po1.get(4)),
    )


# -- 856 Advance Ship Notice -----------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Asn856Item:
    rough_country_part_number: str  # LIN "VC"
    our_part_number: str  # LIN "SK"
    quantity_shipped: typing.Optional[int]  # SN1-02
    # Tracking numbers of the pack this item actually sits in — not the whole shipment's.
    tracking_numbers: typing.Tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Asn856:
    shipment_id: str  # BSN02 — Rough Country's own shipment identifier
    po_number: str  # PRF01
    ship_date: typing.Optional[datetime.date]  # DTM*011
    carrier_code: str  # TD5-05, opaque
    warehouse_code: str  # N1*SE identifier (e.g. "435")
    warehouse_name: str  # N1*SE name
    # Every distinct tracking number in the document, pack-level MAN*CP first. This is what
    # goes on PurchaseOrderDistributorOrder.tracking_numbers.
    tracking_numbers: typing.Tuple[str, ...]
    items: typing.Tuple[Asn856Item, ...]


def _hl_blocks(transaction: x12.Transaction) -> typing.List[typing.Dict]:
    """Split the transaction into its HL hierarchy. Segments before the first HL (BSN) are not
    part of any level and are read separately."""
    blocks: typing.List[typing.Dict] = []
    current: typing.Optional[typing.Dict] = None
    for seg in transaction.body():
        if seg.tag == "HL":
            if current is not None:
                blocks.append(current)
            current = {
                "id": seg.get(1).strip(),
                "parent": seg.get(2).strip(),
                "level": seg.get(3).strip().upper(),
                "segments": [],
            }
        elif current is not None:
            current["segments"].append(seg)
    if current is not None:
        blocks.append(current)
    return blocks


def parse_856(transaction: x12.Transaction) -> Asn856:
    bsn = transaction.find("BSN") or x12.Segment(tag="BSN")
    blocks = _hl_blocks(transaction)
    by_id = {b["id"]: b for b in blocks}

    shipment_tracking: typing.List[str] = []
    pack_tracking: typing.Dict[str, typing.List[str]] = {}
    po_number = ""
    ship_date = None
    carrier_code = ""
    warehouse_code = ""
    warehouse_name = ""
    items: typing.List[Asn856Item] = []

    for block in blocks:
        level = block["level"]
        segments = block["segments"]
        if level == _HL_SHIPMENT:
            for seg in segments:
                if seg.tag == "TD5" and seg.get(5).strip():
                    carrier_code = seg.get(5).strip()
                elif seg.tag == "REF" and seg.get(1).strip().upper() == "CN":
                    # Shipment-level tracking. On their single-package test this equalled the
                    # pack's MAN*CP; on a multi-package shipment it cannot, and which one is
                    # authoritative is still unconfirmed — so both are collected.
                    shipment_tracking.append(seg.get(2).strip())
                elif seg.tag == "DTM" and seg.get(1).strip() == "011":
                    ship_date = _date(seg.get(2))
                elif seg.tag == "N1" and seg.get(1).strip().upper() == "SE":
                    warehouse_name = seg.get(2).strip()
                    warehouse_code = seg.get(4).strip()
        elif level == _HL_ORDER:
            for seg in segments:
                if seg.tag == "PRF" and seg.get(1).strip():
                    po_number = seg.get(1).strip()
        elif level == _HL_PACK:
            collected = [
                seg.get(2).strip()
                for seg in segments
                if seg.tag == "MAN" and seg.get(1).strip().upper() in ("CP", "GM")
            ]
            pack_tracking[block["id"]] = [t for t in collected if t]
        elif level == _HL_ITEM:
            lin = next((s for s in segments if s.tag == "LIN"), None)
            sn1 = next((s for s in segments if s.tag == "SN1"), None)
            if lin is None:
                continue
            items.append(
                Asn856Item(
                    rough_country_part_number=_qualified(lin, 2, "VC"),
                    our_part_number=_qualified(lin, 2, "SK"),
                    quantity_shipped=_int(sn1.get(2)) if sn1 is not None else None,
                    tracking_numbers=tuple(pack_tracking.get(_enclosing_pack(block, by_id), [])),
                )
            )

    ordered: typing.List[str] = []
    for tracking in [t for values in pack_tracking.values() for t in values] + shipment_tracking:
        if tracking and tracking not in ordered:
            ordered.append(tracking)

    return Asn856(
        shipment_id=bsn.get(2).strip(),
        po_number=po_number,
        ship_date=ship_date or _date(bsn.get(3)),
        carrier_code=carrier_code,
        warehouse_code=warehouse_code,
        warehouse_name=warehouse_name,
        tracking_numbers=tuple(ordered),
        items=tuple(items),
    )


def _enclosing_pack(block: typing.Dict, by_id: typing.Dict[str, typing.Dict]) -> str:
    """Walk up the HL parent chain to the pack containing this item. Items are not always a
    direct child of a pack — a shipment with no cartons hangs them straight off the order — so
    this returns "" rather than assuming."""
    seen = set()
    parent_id = block["parent"]
    while parent_id and parent_id not in seen:
        seen.add(parent_id)
        parent = by_id.get(parent_id)
        if parent is None:
            return ""
        if parent["level"] == _HL_PACK:
            return parent["id"]
        parent_id = parent["parent"]
    return ""


# -- 810 Invoice -----------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Invoice810Line:
    line_number: typing.Optional[int]  # IT1-01 — always empty so far
    rough_country_part_number: str  # IT1 "VC"
    our_part_number: str  # IT1 "SK"
    quantity: typing.Optional[int]  # IT1-02
    unit_price: typing.Optional[decimal.Decimal]  # IT1-04, up to 4dp, never rounded here
    description: str  # PID05


@dataclasses.dataclass(frozen=True)
class Invoice810:
    invoice_number: str  # BIG02
    invoice_date: typing.Optional[datetime.date]  # BIG01
    po_number: str  # BIG04
    net_days: typing.Optional[int]  # ITD07
    ship_date: typing.Optional[datetime.date]  # DTM*011
    carrier_code: str  # CAD04, opaque
    carrier_name: str  # CAD05
    # TDS01. Their test invoice excluded the freight SAC entirely while the standard says TDS is
    # inclusive, and that invoice carried no freight at all — so whether this includes freight
    # is still genuinely unknown. Never reconcile it by equality against recomputed lines.
    total_amount: typing.Optional[decimal.Decimal]
    freight: typing.Optional[decimal.Decimal]  # SAC05 where SAC02 is a freight service code
    lines: typing.Tuple[Invoice810Line, ...]

    @property
    def line_subtotal(self) -> decimal.Decimal:
        """Sum of quantity x unit_price at full precision. Compare against ``total_amount`` with
        a tolerance: at 4dp unit prices the two legitimately differ by rounding."""
        total = decimal.Decimal("0")
        for line in self.lines:
            if line.quantity is not None and line.unit_price is not None:
                total += decimal.Decimal(line.quantity) * line.unit_price
        return total


# SAC02 service codes we have seen carry freight. "D240" is what their sample 810 labelled
# "Freight"; the set is open, so SAC13's free-text label is also checked.
_FREIGHT_SERVICE_CODES = {"D240", "D500", "G830"}


def parse_810(transaction: x12.Transaction) -> Invoice810:
    big = transaction.find("BIG") or x12.Segment(tag="BIG")
    itd = transaction.find("ITD")
    cad = transaction.find("CAD")
    tds = transaction.find("TDS")

    ship_date = None
    for seg in transaction.find_all("DTM"):
        if seg.get(1).strip() == "011":
            ship_date = _date(seg.get(2))
            break

    freight = None
    for seg in transaction.find_all("SAC"):
        code = seg.get(2).strip().upper()
        label = seg.get(13).strip().upper()
        if code in _FREIGHT_SERVICE_CODES or "FREIGHT" in label:
            freight = _implied_decimal(seg.get(5))
            break

    lines: typing.List[Invoice810Line] = []
    pending: typing.Optional[x12.Segment] = None
    for seg in transaction.body():
        if seg.tag == "IT1":
            if pending is not None:
                lines.append(_invoice_line(pending, ""))
            pending = seg
        elif seg.tag == "PID" and pending is not None:
            lines.append(_invoice_line(pending, seg.get(5).strip()))
            pending = None
    if pending is not None:
        lines.append(_invoice_line(pending, ""))

    return Invoice810(
        invoice_number=big.get(2).strip(),
        invoice_date=_date(big.get(1)),
        po_number=big.get(4).strip(),
        net_days=_int(itd.get(7)) if itd else None,
        ship_date=ship_date,
        carrier_code=cad.get(4).strip() if cad else "",
        carrier_name=cad.get(5).strip() if cad else "",
        total_amount=_implied_decimal(tds.get(1)) if tds else None,
        freight=freight,
        lines=tuple(lines),
    )


def _invoice_line(it1: x12.Segment, description: str) -> Invoice810Line:
    return Invoice810Line(
        line_number=_int(it1.get(1)),
        rough_country_part_number=_qualified(it1, 6, "VC"),
        our_part_number=_qualified(it1, 6, "SK"),
        quantity=_int(it1.get(2)),
        unit_price=_decimal(it1.get(4)),
        description=description,
    )
