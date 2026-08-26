"""
Turn a filter dict from the API into a Meilisearch filter expression.

Pure: dict in, string out. No IO, no Django.

Two rules carry all the risk:

  * **Every key is whitelisted.** An unrecognised key is a 400, never a silent drop. A filter the
    server quietly ignores shows the user more results than they asked for, and they have no way
    to tell that happened -- which is indistinguishable from the search being broken.
  * **Every string value is quoted and escaped.** Filter expressions are a query language; an
    unescaped quote in a value is injection into it.

``False`` **omits its clause entirely** rather than emitting ``field = false``. The tri-state
booleans in this index (``is_3pmsf`` and friends) are absent from a document when unknown, so
``is_3pmsf = false`` matches only tires positively known not to be certified -- almost none. A UI
toggle in its off position means "don't filter", not "show me the uncertified ones".
"""
import decimal
import numbers
import typing


class UnknownFilterField(ValueError):
    """Raised for a key that is not a filterable attribute. The API turns this into a 400."""


class InvalidFilterValue(ValueError):
    """Raised for a value whose shape has no defined translation."""


def _quote(value: str) -> str:
    """
    Wrap a value in double quotes for the filter grammar, escaping backslashes and quotes.

    Order matters: backslashes first, or the escape character added for a quote would itself be
    escaped on the second pass.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return '"{}"'.format(escaped)


def _scalar(field: str, value: typing.Any) -> str:
    if isinstance(value, bool):
        # Only True reaches here; False is dropped by build_filter. See the module docstring.
        return "{} = true".format(field)
    if isinstance(value, numbers.Number) or isinstance(value, decimal.Decimal):
        return "{} = {}".format(field, value)
    return "{} = {}".format(field, _quote(str(value)))


def _range(field: str, spec: typing.Mapping[str, typing.Any]) -> typing.Optional[str]:
    minimum, maximum = spec.get("min"), spec.get("max")
    clauses = []
    if minimum is not None:
        clauses.append("{} >= {}".format(field, minimum))
    if maximum is not None:
        clauses.append("{} <= {}".format(field, maximum))
    if not clauses:
        return None
    return " AND ".join(clauses)


def build_clause(field: str, value: typing.Any) -> typing.Optional[str]:
    """One field's clause, or ``None`` when this value means "no constraint"."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "{} = true".format(field) if value else None
    if isinstance(value, (list, tuple, set)):
        members = [v for v in value if v is not None]
        if not members:
            return None
        if len(members) == 1:
            return _scalar(field, members[0])
        # OR within a field, AND between fields -- picking two load ranges means "either", not
        # "both", which nothing could satisfy.
        return "({})".format(" OR ".join(_scalar(field, v) for v in members))
    if isinstance(value, dict):
        if not set(value) <= {"min", "max"}:
            raise InvalidFilterValue("{}: object filters accept only 'min' and 'max'".format(field))
        return _range(field, value)
    if isinstance(value, (str, numbers.Number, decimal.Decimal)):
        if isinstance(value, str) and not value.strip():
            return None
        return _scalar(field, value)
    raise InvalidFilterValue("{}: unsupported filter value type {}".format(field, type(value).__name__))


def build_filter(
    filters: typing.Optional[typing.Mapping[str, typing.Any]],
    allowed_fields: typing.AbstractSet[str],
) -> str:
    """
    The whole expression, or ``""`` when nothing constrains the search.

    ``allowed_fields`` is the index's ``filterableAttributes``; passing it in rather than
    importing it keeps this module free of the search package and testable on its own.
    """
    if not filters:
        return ""
    unknown = sorted(set(filters) - set(allowed_fields))
    if unknown:
        raise UnknownFilterField(
            "Unknown filter field(s): {}. Allowed: {}".format(", ".join(unknown), ", ".join(sorted(allowed_fields)))
        )
    clauses = []
    for field in sorted(filters):
        clause = build_clause(field, filters[field])
        if clause:
            clauses.append(clause)
    return " AND ".join(clauses)
