---
paths:
  - "src/models.py"
  - "migrations/*.py"
  - "src/api/services/**/*.py"
  - "src/integrations/services/**/*.py"
---
# Data rules (Django ORM, production Postgres)
- Models live in `src/models.py` next to their siblings, with an explicit `db_table`.
- Migrations are HAND-WRITTEN: next number in `migrations/`, copy the style of the latest one, correct
  `dependencies`. Never run raw `makemigrations` output into the repo. Never edit a migration already on main.
- Schema and data changes are separate migrations. `RunPython` always has a reverse function.
- New non-null column on an existing table: add nullable (or with default) -> backfill -> tighten later.
- Postgres-only SQL (e.g. `NOW()`) is fine in migrations but breaks the SQLite test/dev settings (migrations are
  disabled there); say so in the plan. Large tables: avoid locking operations, call out index builds.
- Multi-step writes (user + company + profile, etc.) go in `transaction.atomic()` so no partial state survives.
- Money is `DecimalField`, never float. Datetimes are aware UTC (`timezone.now()`).
- Wrap cache calls the way the surrounding file does (Redis can be down); never let a cache error fail a request.
- Services never trust ids from the caller for company scoping: filter by the company id passed from the view.
- Default Django settings point at PRODUCTION. Verify with `--settings=conf.settings_dev` (throwaway SQLite) or
  `conf.settings_test`; never run a write path on default settings.
