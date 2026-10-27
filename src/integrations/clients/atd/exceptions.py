class ATDAPIException(Exception):
    pass


class ATDAPIBadResponseCodeError(ATDAPIException):
    def __init__(self, message: str, code: int) -> None:
        ATDAPIException.__init__(self, message)
        self.message = message
        self.code = code


class ATDAuthError(ATDAPIBadResponseCodeError):
    """Bad username/password/clientId, or the credentials don't have access to this location --
    see atd-connect-api.md section 2's two documented error shapes, both normalized to this."""


class ATDConnectionError(ATDAPIException):
    """Network/timeout/unreachable-host failures, and any 5xx or malformed response."""


class ATDOrderNotEntitledError(ATDAPIException):
    """
    ATD answered, and said this account may not use the order service.

    Separate from ATDAuthError because the credentials are not necessarily wrong: the same
    username and password that pull a 34,000-product catalog over REST get
    "Unable to authenticate User's Credentials" from the SOAP order service until ATD grants
    order access to the account, which they do separately and only after a sandbox test order.
    Confirmed live 2026-10-27 with SunCoast's working feed credentials.

    Reported to a dealer as "ask ATD to enable ordering", not as "check your password", because
    re-entering a correct password forever is the obvious wrong path here.
    """
    pass
