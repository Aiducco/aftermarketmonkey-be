class EdiError(Exception):
    """Base class for every EDI-layer failure."""
    pass


class EdiParseError(EdiError):
    """A received payload isn't a well-formed X12 interchange (bad ISA, truncated file,
    unterminated envelope). Distinct from a *semantically* unexpected document, which parses
    fine and is reported by the per-document parsers instead."""
    pass


class EdiBuildError(EdiError):
    """We were asked to generate a document we can't legally encode — e.g. a control number
    that won't fit its element, or a partner id longer than ISA06/ISA08's fixed 15 characters."""
    pass


class EdiTransportError(EdiError):
    """FTP-level failure: connect, login, list, get, put, rename."""
    pass
