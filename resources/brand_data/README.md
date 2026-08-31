# Manufacturer tire data files

Files a brand sent us, and the declarations that tell the ingest how to read them.

    resources/brand_data/
      sources/<brand>.json          committed: the source declaration and its column map
      <brand>/<edition>.xlsx        not committed: the file itself

Put each file in a per-brand folder with the edition in the name (`2026-q3-us-databook.xlsx`),
because a manufacturer sheet is a snapshot: the next one supersedes it rather than amending it,
and knowing which edition a row came from is the difference between a stale spec and a wrong one.

Register a declaration, then pull it:

    manage.py seed_tire_brand_sources --from-json resources/brand_data/sources/<brand>.json
    manage.py ingest_brand_tire_specs --source <slug> --dry-run
    manage.py ingest_brand_tire_specs --source <slug>

See `docs/BRAND_TIRE_DATA_INITIATIVE.md` for the whole picture and
`src/integrations/brand_data/mapping.py` for what a column map may contain.
