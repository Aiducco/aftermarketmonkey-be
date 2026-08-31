class Turn14APIException(Exception):
    pass


class Turn14APIBadResponseCodeError(Turn14APIException):
    def __init__(self, message: str, code: int) -> None:
        # Passing message through to Exception.__init__ is the fix: this used to call it with no
        # args, so str(e) was always "" regardless of what message said -- every generic `except
        # Exception as e: ... str(e)` handler (e.g. the pricing job runner's) silently lost the
        # real error, confirmed live 2026-08-31 investigating a batch of Turn14 pricing failures
        # that recorded a blank error_message. Every existing catch site reads .code/.message
        # directly rather than str(e), so this is additive -- nothing relied on the old behavior.
        Turn14APIException.__init__(self, message)
        self.message = message
        self.code = code


class Turn14PermissionError(Turn14APIBadResponseCodeError):
    """Credentials are valid, but the account lacks permission for the requested resource."""