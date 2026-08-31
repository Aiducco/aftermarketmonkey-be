"""
Brand data: tire specifications collected from the manufacturers themselves.

Everything else under ``src/integrations`` is a *trading partner* -- a distributor whose feed we
ingest to sell their stock -- or, in the case of ``simpletire``/``tdg``, a reseller's catalog we
read to describe tires we already carry. This package is neither. It collects what the company
that made the tire publishes about it, from whatever channel that company happens to publish
through, and lands it verbatim in ``raw_tire_specs``.

Why it is separate
------------------
Two properties make manufacturer data a different kind of thing rather than one more feed:

**Reach.** A reseller lists what a reseller stocks. A manufacturer publishes the whole line,
including the sizes nobody stocks and the OE-fitment variants that never reach an aftermarket
shelf. The catalog merges we already run are bounded by somebody else's inventory decisions.

**Authority.** There is no further source to reconcile a manufacturer's own data book against.
Where a reseller and a manufacturer disagree about a tread depth, that is not two opinions.

And one property makes it awkward: there is no such thing as "the manufacturer API". Some brands
have a dealer portal with a JSON endpoint, some email a spreadsheet once a quarter, some publish a
PDF data book somebody has to read, and some have nothing but a website. The channel is per brand
and it changes. That is the whole reason for the registry.

The three pieces
----------------
``registry``   Loaders, keyed by name, and the lookup from a ``TireBrandSource`` row to the
               callable that reads it. The *declaration* of each source -- brand, method, config,
               state -- lives in the database (``src.models.TireBrandSource``), because most of
               what it holds changes without a deploy and a brand with no source yet is a row in
               it too. The code that reads a source lives here.
``loaders``    The transports. ``csv`` reads a file somebody sent; ``http_json`` calls an
               endpoint. Both are config-driven, so most brands need no code at all -- a column
               map in ``TireBrandSource.config`` is the whole integration. Brand-specific loaders
               (a scrape, an odd auth flow) are modules in the same package.
``ingest``     The runner every source goes through, whatever read it: map -> validate -> upsert
               into ``raw_tire_specs`` -> record a ``TireBrandSourceRun``. Sources differ in how
               bytes arrive and in nothing else, so everything after that is shared.

What this package does not do
-----------------------------
It does not touch ``tire_specs``. Landing a brand's data and merging it into the catalog are
separate jobs -- see ``docs/BRAND_TIRE_DATA_INITIATIVE.md`` for why, and for the phase that adds
the merge. ``src.integrations.services.tire_catalog`` already holds the matching rules that merge
will use.
"""
