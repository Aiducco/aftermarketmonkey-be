"""
Which code reads which source.

The registry is split in two on purpose, and the split is the design decision worth understanding
before adding a brand.

**The declaration is data.** Which brands we have a source for, what kind of source it is, where
the file comes from, who sends it, when it was last pulled, and the column map -- all of that is
rows in ``tire_brand_sources``. It changes constantly and mostly by people who are not deploying:
a rep sends the spreadsheet from a new address, a portal moves, a brand we had nothing for gets a
contact. None of that should be a code change, and a brand with no source at all is a row too,
with ``status='planned'``, so that the table is the work list rather than a list of the work
already done.

**The transport is code.** Reading a CSV, calling an endpoint, crawling a site -- that is what
lives here, keyed by ``handler``. Most brands need no new code: a spreadsheet is ``'csv'`` and a
JSON endpoint is ``'http_json'``, both driven entirely by config, so the integration is a column
map. A brand needing a scrape or an unusual auth gets its own module under ``loaders/`` and its
own handler key; there is nothing generic to write for those, and pretending otherwise produces a
framework that fits no site.

Registering::

    from src.integrations.brand_data.registry import loader

    @loader("michelin_databook")
    def load(ctx):
        yield from ...

The module must be imported for that to take effect, which ``loaders/__init__.py`` does for every
module in the package -- see the note there.
"""
import typing

from src import models as src_models
from src.integrations.brand_data import base

_LOADERS: typing.Dict[str, base.BrandLoader] = {}


def loader(name: str) -> typing.Callable[[base.BrandLoader], base.BrandLoader]:
    """Register a loader under the key a ``TireBrandSource.handler`` names."""

    def register(function: base.BrandLoader) -> base.BrandLoader:
        existing = _LOADERS.get(name)
        if existing is not None and existing is not function:
            raise RuntimeError(f"two loaders claim the handler {name!r}: {existing!r} and {function!r}")
        _LOADERS[name] = function
        return function

    return register


def loader_names() -> typing.List[str]:
    _ensure_loaded()
    return sorted(_LOADERS)


def get_loader(handler: str) -> base.BrandLoader:
    _ensure_loaded()
    try:
        return _LOADERS[handler]
    except KeyError:
        raise base.SourceConfigError(
            f"no loader registered for handler {handler!r}; known handlers: {', '.join(sorted(_LOADERS)) or '(none)'}"
        ) from None


def resolve(source: src_models.TireBrandSource) -> base.BrandLoader:
    """
    The loader for a registry row, or a refusal that says which of the two reasons applies.

    A blank handler is not a bug -- it is what a planned source looks like -- so it gets its own
    message rather than being reported as an unknown handler.
    """
    if not source.handler:
        raise base.SourceConfigError(
            f"{source.slug}: no handler set. This source is a declaration, not a pull -- "
            f"give it a handler (one of: {', '.join(loader_names()) or 'none registered'}) or "
            f"leave it at status='planned' and collect it by hand."
        )
    return get_loader(source.handler)


def _ensure_loaded() -> None:
    # Imported lazily rather than at module import: ``loaders`` imports back into this module for
    # the decorator, and doing it eagerly here is a circular import.
    from src.integrations.brand_data import loaders  # noqa: F401
