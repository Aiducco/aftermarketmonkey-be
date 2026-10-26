"""
Row-locked X12 control-number counters.

Every interchange carries three sequences: ISA13 (interchange), GS06 (functional group) and
ST02 (transaction set). ISA13 is the one that matters — a partner's translator rejects a
duplicate outright, and because Rough Country publishes no 997 for our 850s that rejection is
*silent*: the shop sees a submitted PO, nothing ever ships, and nobody finds out until the
customer calls. That is the failure this module exists to make impossible.

**Why not a timestamp.** The manual test 850 derived its control number from ``%m%d%H%M``, which
is fine for one file sent by hand and wrong for anything else: two submits in the same minute
collide, and the sequence is not monotonic across a year boundary. ``next_value`` takes a real
row lock instead.

**Call this OUTSIDE the transaction that does the sending.** A number must never be reused once
it has been handed out, even for a send that then failed — gaps in the sequence are harmless,
duplicates are not. If this is called inside a caller's ``atomic()`` block and that block later
rolls back, the increment rolls back with it and the next send re-mints a number the partner may
already have seen. The adapter therefore allocates before it touches anything else; see
``src/integrations/orders/rough_country.py``.

The counters are per (partner_id, kind), so a second EDI distributor gets its own sequences for
free. Deleting or truncating ``edi_control_numbers`` restarts a partner at 1 and will re-issue
numbers that partner has already rejected once — treat those rows as durable.
"""
import logging
import typing

from django.db import transaction

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[EDI-CONTROL]"

# ISA13 is a fixed 9-digit element, so the sequence wraps rather than overflowing it. At any
# plausible order volume this is tens of thousands of years away; it is here so the failure mode
# is a documented wrap instead of an EdiBuildError nobody expected.
_MODULUS = 10**9 - 1

_MIN_WIDTH = {
    src_models.EdiControlNumber.KIND_INTERCHANGE: 9,  # ISA13 is fixed-width
    src_models.EdiControlNumber.KIND_GROUP: 1,  # GS06 is 1-9
    src_models.EdiControlNumber.KIND_TRANSACTION: 4,  # ST02 is 4-9
}


def next_value(partner_id: str, kind: str) -> int:
    """
    Allocate and return the next integer for this (partner_id, kind).

    Wrapped in its own ``atomic`` block so the ``SELECT ... FOR UPDATE`` has a transaction to
    hold the lock in. Read the module docstring before calling this from inside a wider
    transaction of your own.
    """
    partner = (partner_id or "").strip()
    if not partner:
        raise ValueError("partner_id is required to allocate a control number.")

    with transaction.atomic():
        counter, created = src_models.EdiControlNumber.objects.select_for_update().get_or_create(
            partner_id=partner,
            kind=kind,
            defaults={"value": 0},
        )
        counter.value = (counter.value % _MODULUS) + 1
        counter.save(update_fields=["value", "updated_at"])

    if created:
        logger.info("%s Started a new %s counter for %s.", _LOG_PREFIX, kind, partner)
    return counter.value


def _formatted(partner_id: str, kind: str) -> str:
    value = next_value(partner_id, kind)
    return str(value).zfill(_MIN_WIDTH[kind])


def next_interchange_control_number(partner_id: str) -> str:
    """ISA13 / IEA02, zero-padded to the element's fixed 9 characters."""
    return _formatted(partner_id, src_models.EdiControlNumber.KIND_INTERCHANGE)


def next_group_control_number(partner_id: str) -> str:
    """GS06 / GE02. Not fixed-width, so this is the bare number."""
    return _formatted(partner_id, src_models.EdiControlNumber.KIND_GROUP)


def next_transaction_control_number(partner_id: str) -> str:
    """ST02 / SE02, padded to the element's 4-character minimum."""
    return _formatted(partner_id, src_models.EdiControlNumber.KIND_TRANSACTION)


def peek(partner_id: str) -> typing.Dict[str, int]:
    """Current value of each counter without allocating anything — for the CLI and for
    diagnosing a rejected interchange after the fact."""
    rows = src_models.EdiControlNumber.objects.filter(partner_id=(partner_id or "").strip())
    return {row.kind: row.value for row in rows}
