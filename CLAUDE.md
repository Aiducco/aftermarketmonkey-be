# aftermarketmonkey-be

Django 4.1 + plain `django.views.View` JSON endpoints (no DRF, no OpenAPI) on Python 3.12,
Postgres, Redis cache, Meilisearch. Single Django app `src`. Deps: pip-tools. Deploys on push
to `main` (production) and `staging` (stage.aftermarketscout.com) via GitHub Actions: never push.
Branches are cut from `staging` and PRs target `staging`; `main` only moves by a promotion PR `staging` -> `main`
(hotfix exception: `hotfix/<slug>` from `main`, then a back-merge PR into `staging`).

## Commands (always use ./venv/bin/python)
- Run existing tests: ./venv/bin/python manage.py test <dotted.module> --settings=conf.settings_test
  (name modules explicitly, e.g. src.domain.tests.test_tire_size)
- Format files you touched: ./venv/bin/black --line-length=120 <files> ; ./venv/bin/isort --profile black <files>
- Add a dependency: edit requirements.in, then `make update_reqs` (pip-compile)
- Sanity check: ./venv/bin/python manage.py check --settings=conf.settings_test
- DB-touching commands, shells, migrate: NEVER on default settings (prod). See below.

## Layout
- src/models.py: ALL models (10k lines). Add new ones next to their siblings.
- migrations/: hand-written, numbered (0xxx_name.py). Copy the style of the latest one. Never
  edit a migration already on main. Never commit raw makemigrations output.
- src/api/urls.py -> src/api/views/<area>.py (thin: auth, parse JSON, call a service, return
  JSON) -> src/api/services/<area>.py (business logic) ; request/response shapes in src/api/schemas/.
- src/integrations/: distributor and shop-management integrations. HTTP clients in
  clients/<slug>/client.py, logic in services/, live inventory in live_inventory/.
- src/search/ (Meilisearch), src/domain/ (pure tire/wheel/spec parsing, has tests).
- src/management/commands/: background and batch work (no Celery). ~100 commands.
- Auth: JWT middleware sets request.user and request.company_id. Errors: JSON
  {"message": ..., "error_code": ...?} with an HTTP status.

## Always
- Match black (120) and isort (profile black). Do NOT reformat src/constants.py.
- New views: csrf_exempt class-based View, call a service, return via the file's _json_response.
- Check disk (`df -h` on DB host) before any bulk write: commands write to prod from your Mac.

## Never
- Run manage.py on default settings for writes, shell, migrate or test (all hit prod DB).
- Commit with bare `git commit` or `git add -A`; use `git commit -m "..." -- <paths>`.
- Push, stash, checkout, reset, clean, or switch branches.
- Read or print .env* files or summit_racing_cookie.local.txt.

## Verification over test authorship

Speed matters more here than suite coverage. **Verify changes by running them — do not
write new test files or test functions unless explicitly asked.**

Verify by execution instead:

- Run the code path directly: a management command, a `python -c` snippet, a throwaway
  script in the scratchpad, or the Django shell.
- Run the *existing* tests for the modules you touched (see the caveat below).
- Check real output — a row in the DB, a rendered page, an API response — and report
  what you actually saw, not what should have happened.

This overrides:

- `karpathy-guidelines` section 4, whose examples rewrite every task as "write a test
  that reproduces it, then make it pass". Keep the rest of that section: still define
  explicit success criteria and loop until verified — just verify by running, not by
  authoring tests.
- `senior-python-engineer/references/testing.md` **only**. Read it when tests are
  explicitly requested; it is not a standing instruction to add tests. The rest of
  `senior-python-engineer` — core principles, code style, layering, non-negotiables —
  stays fully in force.

Do not end a turn by offering to add tests. If a change is genuinely risky to ship
unverified, say which behaviour is unverified and why — don't convert that into a
test-writing proposal.

### Caveat: running the existing suite

`manage.py test` and `manage.py shell` point at **production** (`.env` sets `DATABASE_HOST`
to the prod host, and Django's test runner creates `test_<name>` on whatever host it is
given). Use the in-repo override instead:

```bash
python manage.py test <module> --settings=conf.settings_test
```

`conf/settings_test.py` is SQLite in-memory with `MIGRATION_MODULES = {"src": None}`, so
tables come straight from the models — migration `0058` backfills with Postgres-only
`NOW()`, which SQLite has no function for. Name test **modules** explicitly: `src` has no
`__init__.py`, so package-level discovery dies in unittest's loader.
