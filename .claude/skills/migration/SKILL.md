---
name: migration
description: House recipe for a hand-written Django migration in this repo. Use when a task changes models.
---
Migrations here are hand-written and run on PRODUCTION Postgres at deploy (`migrate --no-input` in CI deploy).

1. Edit the model in `src/models.py` (next to siblings, explicit `db_table`).
2. Find the latest migration: `ls migrations | sort | tail -3`; copy its style; next number; set `dependencies` to
   the true latest (watch for unmerged numbers on other branches).
3. Schema change and data backfill are separate migrations. `RunPython` always has a reverse function.
   Non-null column on an existing table: add nullable/with default -> backfill -> tighten later.
4. Postgres-only SQL is allowed here but must be called out (SQLite dev/test settings disable migrations).
5. Verify without touching prod: build the schema from models with `seed_dev --settings=conf.settings_dev`
   (fresh SQLite) and run the code path; read the migration by hand against the model diff; if a Postgres check is
   needed, state it as unverified for the human. Never run `migrate` on default settings.
6. Report the migration filename, what it does to existing rows, lock/size concerns, and how to roll back.
