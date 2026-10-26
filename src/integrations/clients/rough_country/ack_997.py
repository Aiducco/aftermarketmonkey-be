"""
Builds the X12 005010 997 Functional Acknowledgment we send back for each document Rough
Country publishes to us.

Alluvia's folder layout has an ``/Inbound/997`` and Michael confirmed on 21 Oct 2026 that they
want these ("Yes please"), for the 855/856/810 they send us. Note the asymmetry: there is no
``/Outbound/997``, so we acknowledge their documents and they never acknowledge ours. A
syntactically bad 850 of ours is discarded silently, which is why the no-ASN alerting in the
poller matters as much as this file does.

A 997 acknowledges a FUNCTIONAL GROUP (one GS…GE), not a file and not an interchange: AK1 names
the group by its GS01 functional identifier and GS06 control number, each AK2/AK5 pair names one
transaction set inside it, and AK9 closes with the counts. Those counts have to agree with the
AK2s present or the acknowledgment is itself malformed, so they are computed here rather than
passed in.
"""
import datetime
import typing

from src.integrations.edi import exceptions as edi_exceptions
from src.integrations.edi import x12

# AK501 / AK901. We only ever send "A" today: our reader either parses a whole interchange or
# rejects it outright, and a rejected one is quarantined for a human rather than nacked back —
# telling Rough Country "R" would invite them to resend a document we may simply have failed to
# read, which is a worse outcome than a quiet investigation.
ACCEPTED = "A"
REJECTED = "R"


def build_997_segments(
    group_functional_id_code: str,
    group_control_number: str,
    transactions: typing.Sequence[typing.Tuple[str, str]],
    status: str = ACCEPTED,
) -> typing.List[x12.Segment]:
    """
    ``transactions`` is ``[(ST01, ST02), ...]`` from the group being acknowledged — e.g.
    ``[("855", "163802490")]``.
    """
    if not group_control_number:
        raise edi_exceptions.EdiBuildError("A 997 must name the GS06 control number of the group it acknowledges.")
    if not transactions:
        raise edi_exceptions.EdiBuildError("A 997 must acknowledge at least one transaction set.")

    segments = [
        x12.segment("AK1", group_functional_id_code, group_control_number),
    ]
    for transaction_set_code, transaction_control_number in transactions:
        segments.append(x12.segment("AK2", transaction_set_code, transaction_control_number))
        segments.append(x12.segment("AK5", status))

    count = str(len(transactions))
    accepted_count = count if status == ACCEPTED else "0"
    # AK901 group status, AK902 sets included, AK903 sets received, AK904 sets accepted.
    segments.append(x12.segment("AK9", status, count, count, accepted_count))
    return segments


def build_997(
    group_functional_id_code: str,
    group_control_number: str,
    transactions: typing.Sequence[typing.Tuple[str, str]],
    sender_id: str,
    receiver_id: str,
    interchange_control_number: str,
    our_group_control_number: str,
    transaction_control_number: str,
    usage_indicator: str = "T",
    status: str = ACCEPTED,
    now: typing.Optional[datetime.datetime] = None,
) -> str:
    """
    The complete interchange. ``group_control_number`` is THEIR GS06 being acknowledged;
    ``our_group_control_number`` is ours for the envelope carrying this 997 — two different
    sequences that are easy to conflate and produce an acknowledgment pointing at itself.
    """
    return x12.build_interchange(
        sender_id=sender_id,
        receiver_id=receiver_id,
        interchange_control_number=interchange_control_number,
        group_control_number=our_group_control_number,
        # GS01 for a functional acknowledgment is "FA" regardless of what is being acked.
        functional_id_code="FA",
        transactions=[
            (
                "997",
                transaction_control_number,
                build_997_segments(
                    group_functional_id_code=group_functional_id_code,
                    group_control_number=group_control_number,
                    transactions=transactions,
                    status=status,
                ),
            )
        ],
        usage_indicator=usage_indicator,
        now=now,
    )


def filename_for(acknowledged_control_number: str, now: typing.Optional[datetime.datetime] = None) -> str:
    moment = now or datetime.datetime.utcnow()
    return "997_{}_{}.x12".format(
        x12.scrub(acknowledged_control_number).replace(" ", "_"),
        moment.strftime("%Y%m%dT%H%M%SZ"),
    )
