"""
Generic ANSI X12 (005010) envelope reader/writer — no Rough Country specifics, so the next
EDI distributor reuses it. Document-level mapping lives in
``src/integrations/clients/rough_country/edi.py``.

Two things here are worth knowing before touching them:

**Delimiters are read from the ISA, never assumed.** The ISA segment is fixed-width (106
characters), which is what makes it self-describing: the element separator is whatever sits at
offset 3, the component separator at 104, the segment terminator at 105 and the repetition
separator at 82. Rough Country's own four samples disagree with each other on the repetition
separator (``^`` on the 810, ``|`` on the 855/856, and ``U`` on the 850 — the latter being the
4010 usage indicator left in a 005010 document), so reading them positionally is the only way
this survives their generator.

**CR/LF is noise, not structure.** Segments are terminated by ``~``. Their sample files are
variously CRLF and — in the 850's case — *mixed* CRLF and bare CR, with segment breaks appearing
both mid-line and at line ends. We therefore flatten all CR/LF out before locating the ISA's
fixed offsets. A genuinely newline-terminated interchange would be destroyed by that, so it is
detected and rejected with a clear error rather than silently mis-parsed.
"""
import dataclasses
import datetime
import logging
import re
import typing

from src.integrations.edi import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[EDI-X12]"

# Offset of each self-describing character within the fixed-width ISA segment.
_ISA_LENGTH = 106
_ISA_ELEMENT_SEPARATOR_OFFSET = 3
_ISA_REPETITION_SEPARATOR_OFFSET = 82
_ISA_COMPONENT_SEPARATOR_OFFSET = 104
_ISA_SEGMENT_TERMINATOR_OFFSET = 105

_ISA_ELEMENT_COUNT = 16
_ISA_SENDER_ID_LENGTH = 15

_CRLF_RE = re.compile(r"[\r\n]")


@dataclasses.dataclass(frozen=True)
class Delimiters:
    element: str = "*"
    component: str = ">"
    segment: str = "~"
    # 005010's ISA11. We emit the standard "^"; see the module docstring for why we never
    # assume it on the way in.
    repetition: str = "^"


DEFAULT_DELIMITERS = Delimiters()


@dataclasses.dataclass
class Segment:
    tag: str
    # Elements 1..n, excluding the tag. Trailing empty elements are preserved on parse (a
    # sender's own padding is sometimes meaningful) and trimmed on build.
    elements: typing.List[str] = dataclasses.field(default_factory=list)

    def get(self, position: int) -> str:
        """1-indexed element access — ``seg.get(3)`` is BEG03. Returns "" for an element the
        sender omitted entirely, so callers never have to length-check first."""
        if position < 1 or position > len(self.elements):
            return ""
        return self.elements[position - 1]

    def render(self, delimiters: Delimiters = DEFAULT_DELIMITERS) -> str:
        elements = list(self.elements)
        while elements and elements[-1] == "":
            elements.pop()
        return delimiters.element.join([self.tag] + elements)


def segment(tag: str, *elements: typing.Any) -> Segment:
    """``segment("BEG", "00", "DS", po_number, "", date)`` — None becomes an empty element so
    callers can pass optional values straight through."""
    return Segment(tag=tag, elements=["" if e is None else str(e) for e in elements])


@dataclasses.dataclass
class Transaction:
    """One ST…SE transaction set."""
    transaction_set_code: str  # ST01, e.g. "855"
    control_number: str  # ST02
    segments: typing.List[Segment]  # ST…SE inclusive, exactly as received

    def body(self) -> typing.List[Segment]:
        """Segments between ST and SE — what every document parser actually walks."""
        return [s for s in self.segments if s.tag not in ("ST", "SE")]

    def find(self, tag: str) -> typing.Optional[Segment]:
        for s in self.body():
            if s.tag == tag:
                return s
        return None

    def find_all(self, tag: str) -> typing.List[Segment]:
        return [s for s in self.body() if s.tag == tag]


@dataclasses.dataclass
class FunctionalGroup:
    """One GS…GE group."""
    functional_id_code: str  # GS01, e.g. "PO"/"PR"/"SH"/"IN"/"FA"
    sender_id: str  # GS02
    receiver_id: str  # GS03
    control_number: str  # GS06
    version: str  # GS08
    transactions: typing.List[Transaction]


@dataclasses.dataclass
class Interchange:
    """One ISA…IEA interchange."""
    sender_qualifier: str  # ISA05
    sender_id: str  # ISA06, whitespace-stripped
    receiver_qualifier: str  # ISA07
    receiver_id: str  # ISA08, whitespace-stripped
    control_number: str  # ISA13
    usage_indicator: str  # ISA15, "P" or "T"
    delimiters: Delimiters
    groups: typing.List[FunctionalGroup]

    def transactions(self) -> typing.List[Transaction]:
        return [t for g in self.groups for t in g.transactions]


# -- Parsing ---------------------------------------------------------------------------------


def detect_delimiters(payload: str) -> typing.Tuple[Delimiters, str]:
    """
    Read the four delimiters out of the ISA's fixed offsets and return them alongside the
    CR/LF-flattened payload they were read from (callers should parse *that*, not the original).
    """
    stripped = payload.lstrip("﻿ \t\r\n")
    if not stripped.startswith("ISA"):
        raise exceptions.EdiParseError(
            "Payload does not begin with an ISA segment — not an X12 interchange."
        )
    flat = _CRLF_RE.sub("", stripped)
    if len(flat) < _ISA_LENGTH:
        raise exceptions.EdiParseError(
            "Truncated interchange: the ISA segment is fixed at {} characters, got {}.".format(
                _ISA_LENGTH, len(flat)
            )
        )

    terminator = flat[_ISA_SEGMENT_TERMINATOR_OFFSET]
    if terminator.isalnum() or terminator.isspace():
        # We flattened CR/LF above, so a newline-terminated interchange lands here with the
        # first character of the GS segment where its terminator should be. Fail loudly rather
        # than mis-parse: the whole file's segment boundaries would be wrong.
        raise exceptions.EdiParseError(
            "Could not read a segment terminator at ISA offset {} (found {!r}). Either the ISA "
            "is not fixed-width, or the interchange is newline-terminated, which this reader "
            "does not support.".format(_ISA_SEGMENT_TERMINATOR_OFFSET, terminator)
        )

    delimiters = Delimiters(
        element=flat[_ISA_ELEMENT_SEPARATOR_OFFSET],
        component=flat[_ISA_COMPONENT_SEPARATOR_OFFSET],
        segment=terminator,
        repetition=flat[_ISA_REPETITION_SEPARATOR_OFFSET],
    )
    return delimiters, flat


def parse_segments(payload: str) -> typing.Tuple[typing.List[Segment], Delimiters]:
    delimiters, flat = detect_delimiters(payload)
    segments = []
    for raw in flat.split(delimiters.segment):
        raw = raw.strip()
        if not raw:
            continue
        parts = raw.split(delimiters.element)
        segments.append(Segment(tag=parts[0].strip(), elements=[p for p in parts[1:]]))
    if not segments:
        raise exceptions.EdiParseError("Interchange contains no segments.")
    return segments, delimiters


def parse_interchange(payload: str) -> Interchange:
    """
    Parse one ISA…IEA interchange. Raises EdiParseError on anything structurally wrong; callers
    treat that as "quarantine this file and alert", never as "skip it quietly".
    """
    segments, delimiters = parse_segments(payload)

    isa = segments[0]
    if isa.tag != "ISA":
        raise exceptions.EdiParseError("First segment is {!r}, expected ISA.".format(isa.tag))
    if len(isa.elements) < _ISA_ELEMENT_COUNT:
        raise exceptions.EdiParseError(
            "ISA has {} elements, expected {} — the ISA is fixed-width and cannot be short.".format(
                len(isa.elements), _ISA_ELEMENT_COUNT
            )
        )
    if not any(s.tag == "IEA" for s in segments):
        # The single most valuable check we can make against an FTP source: a file picked up
        # mid-write parses perfectly right up to the point it was truncated.
        raise exceptions.EdiParseError(
            "Interchange has no IEA trailer — the file is truncated or was read while still "
            "being written."
        )

    groups: typing.List[FunctionalGroup] = []
    current_group: typing.Optional[typing.Dict] = None
    current_transaction: typing.Optional[typing.Dict] = None

    for seg in segments[1:]:
        if seg.tag == "GS":
            current_group = {
                "functional_id_code": seg.get(1).strip(),
                "sender_id": seg.get(2).strip(),
                "receiver_id": seg.get(3).strip(),
                "control_number": seg.get(6).strip(),
                "version": seg.get(8).strip(),
                "transactions": [],
            }
        elif seg.tag == "GE":
            if current_group is not None:
                groups.append(FunctionalGroup(**current_group))
                current_group = None
        elif seg.tag == "ST":
            current_transaction = {
                "transaction_set_code": seg.get(1).strip(),
                "control_number": seg.get(2).strip(),
                "segments": [seg],
            }
        elif seg.tag == "SE":
            if current_transaction is not None:
                current_transaction["segments"].append(seg)
                transaction = Transaction(**current_transaction)
                if current_group is not None:
                    current_group["transactions"].append(transaction)
                else:
                    # A transaction outside any GS/GE — malformed, but keep it rather than drop
                    # it silently so the document still reaches a human.
                    logger.warning(
                        "%s ST*%s outside any functional group; wrapping in a synthetic group.",
                        _LOG_PREFIX,
                        transaction.transaction_set_code,
                    )
                    groups.append(
                        FunctionalGroup(
                            functional_id_code="",
                            sender_id="",
                            receiver_id="",
                            control_number="",
                            version="",
                            transactions=[transaction],
                        )
                    )
                current_transaction = None
        elif seg.tag == "IEA":
            continue
        elif current_transaction is not None:
            current_transaction["segments"].append(seg)

    if current_transaction is not None:
        raise exceptions.EdiParseError(
            "Transaction ST*{} has no SE trailer — the interchange is truncated.".format(
                current_transaction["transaction_set_code"]
            )
        )

    return Interchange(
        sender_qualifier=isa.get(5).strip(),
        sender_id=isa.get(6).strip(),
        receiver_qualifier=isa.get(7).strip(),
        receiver_id=isa.get(8).strip(),
        control_number=isa.get(13).strip(),
        usage_indicator=isa.get(15).strip(),
        delimiters=delimiters,
        groups=groups,
    )


# -- Building --------------------------------------------------------------------------------


def scrub(
    value: typing.Any,
    max_length: typing.Optional[int] = None,
    delimiters: Delimiters = DEFAULT_DELIMITERS,
) -> str:
    """
    Make an arbitrary string safe to place in an element.

    X12 has no escaping mechanism at all: a single ``*`` inside a customer's name silently
    becomes an element boundary and shifts every following element by one, and a ``~`` splits
    the segment in half. Both would be accepted by our own writer and rejected (or worse,
    misread) by Rough Country's translator. Every free-text value we emit — names, addresses,
    part descriptions — goes through here.
    """
    text = "" if value is None else str(value)
    for delimiter in (delimiters.element, delimiters.component, delimiters.segment, delimiters.repetition):
        text = text.replace(delimiter, " ")
    # Control characters (including the CR/LF that a pasted address line commonly carries)
    # break the interchange the same way a delimiter does.
    text = "".join(" " if ord(c) < 32 or ord(c) == 127 else c for c in text)
    text = re.sub(r"\s+", " ", text).strip()
    if max_length is not None:
        text = text[:max_length]
    return text.strip()


def _pad(value: str, length: int) -> str:
    """ISA06/ISA08 are fixed-width and must be space-padded to exactly 15."""
    text = (value or "").strip()
    if len(text) > length:
        raise exceptions.EdiBuildError(
            "Trading-partner id {!r} is {} characters; ISA allows {}.".format(text, len(text), length)
        )
    return text.ljust(length)


def _control(value: typing.Any, length: int) -> str:
    text = str(value or "").strip()
    if not text.isdigit():
        raise exceptions.EdiBuildError("Control number {!r} must be numeric.".format(text))
    if len(text) > length:
        raise exceptions.EdiBuildError(
            "Control number {!r} exceeds its {}-digit element.".format(text, length)
        )
    return text.zfill(length)


def build_interchange(
    sender_id: str,
    receiver_id: str,
    interchange_control_number: typing.Any,
    group_control_number: typing.Any,
    functional_id_code: str,
    transactions: typing.List[typing.Tuple[str, str, typing.List[Segment]]],
    usage_indicator: str = "P",
    now: typing.Optional[datetime.datetime] = None,
    delimiters: Delimiters = DEFAULT_DELIMITERS,
    version: str = "005010",
) -> str:
    """
    Wrap one or more transaction sets in a GS/GE and an ISA/IEA and render the whole thing.

    ``transactions`` is a list of ``(transaction_set_code, control_number, body_segments)``,
    where ``body_segments`` excludes ST and SE — both are generated here so the SE01 segment
    count is always correct rather than something each document builder has to remember. The
    850 sample Alluvia sent gets its own SE01 right and its CTT wrong, which is exactly the
    class of mistake worth making structurally impossible.

    Every segment is terminated with ``~`` followed by a newline: the terminator is what the
    receiving translator actually reads, and the newline only makes the file greppable by a
    human debugging a rejected order.
    """
    if not transactions:
        raise exceptions.EdiBuildError("Cannot build an interchange with no transaction sets.")

    moment = now or datetime.datetime.utcnow()
    isa = Segment(
        tag="ISA",
        elements=[
            "00",
            " " * 10,
            "00",
            " " * 10,
            "ZZ",
            _pad(sender_id, _ISA_SENDER_ID_LENGTH),
            "ZZ",
            _pad(receiver_id, _ISA_SENDER_ID_LENGTH),
            moment.strftime("%y%m%d"),
            moment.strftime("%H%M"),
            delimiters.repetition,
            "00501",
            _control(interchange_control_number, 9),
            "0",  # ISA14: no TA1 interchange acknowledgment requested.
            usage_indicator,
            delimiters.component,
        ],
    )
    gs = segment(
        "GS",
        functional_id_code,
        sender_id.strip(),
        receiver_id.strip(),
        moment.strftime("%Y%m%d"),
        moment.strftime("%H%M"),
        str(int(group_control_number)),
        "X",
        version,
    )

    lines = [isa.render(delimiters), gs.render(delimiters)]
    for transaction_set_code, control_number, body in transactions:
        st = segment("ST", transaction_set_code, control_number)
        # SE01 counts every segment from ST through SE inclusive.
        se = segment("SE", str(len(body) + 2), control_number)
        for seg in [st] + list(body) + [se]:
            lines.append(seg.render(delimiters))
    lines.append(segment("GE", str(len(transactions)), str(int(group_control_number))).render(delimiters))
    lines.append(
        segment("IEA", "1", _control(interchange_control_number, 9)).render(delimiters)
    )

    return "".join("{}{}\n".format(line, delimiters.segment) for line in lines)
