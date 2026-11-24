"""
Space PO-number prefixes (pure, no Django): the rules a prefix must follow. A space's prefix is put in front
of the PO numbers it sends to distributors, so a distributor-side order can be told apart by space.

Rules: 2-6 letters or digits, starting with a letter; compared without case; no prefix may start another one
(so a PO number can never belong to two spaces); not "AMS" and not a start of it (our own PO numbers are
"AMS-...").
"""
import re
import typing

MIN_LENGTH = 2
MAX_LENGTH = 6
OWN_PO_PREFIX = "AMS"

_FORMAT = re.compile(r"^[A-Za-z][A-Za-z0-9]{%d,%d}$" % (MIN_LENGTH - 1, MAX_LENGTH - 1))


def format_error(prefix: str) -> typing.Optional[str]:
    """None when the prefix is well-formed, else why not."""
    if not _FORMAT.match(prefix):
        return "A prefix is {}-{} letters or digits and starts with a letter.".format(MIN_LENGTH, MAX_LENGTH)
    if OWN_PO_PREFIX.startswith(prefix.upper()):
        return "{} is reserved for our own PO numbers.".format(OWN_PO_PREFIX)
    return None


def conflicting_prefix(prefix: str, others: typing.Iterable[typing.Optional[str]]) -> typing.Optional[str]:
    """The first of ``others`` that equals ``prefix``, starts it or is started by it (compared without case)."""
    mine = prefix.upper()
    for other in others:
        if not other:
            continue
        theirs = other.upper()
        if mine.startswith(theirs) or theirs.startswith(mine):
            return other
    return None
