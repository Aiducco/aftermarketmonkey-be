"""
Account fingerprint for a shop-system connection (pure, no Django): a hash of the credential fields that identify
the external account, so two spaces cannot hold the same account (AC-43). Which fields identify an account is
declared per system in its catalog entry (``account_identity_fields``); nothing here knows any system.
"""
import hashlib
import typing


def account_fingerprint(
    identity_fields: typing.Sequence[str], credentials: typing.Optional[typing.Dict[str, typing.Any]]
) -> typing.Optional[str]:
    """sha256 (hex, 64 characters) of the identity fields' values, field names sorted; None when there are no
    identity fields or one of them is empty (the account cannot be recognised, so nothing is compared)."""
    fields = sorted({str(f) for f in identity_fields or []})
    if not fields:
        return None
    credentials = credentials or {}
    parts = []
    for field in fields:
        value = credentials.get(field)
        value = "" if value is None else str(value).strip()
        if not value:
            return None
        parts.append("{}={}".format(field, value))
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()
