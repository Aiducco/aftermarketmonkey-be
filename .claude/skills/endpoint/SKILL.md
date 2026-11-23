---
name: endpoint
description: House recipe for adding or changing an API endpoint in this Django repo (plain views, marshmallow schemas, services). Use when a task adds a route or changes a request/response shape.
---
Recipe (copy the closest existing endpoint, e.g. onboarding or support):

1. Contract first: the entry exists in workspace/contracts/api.md (method, path, auth, request, response, errors).
   If not, stop: run /contract-change.
2. Schema: add the marshmallow `Schema` in `src/api/schemas/<area>.py` (required/optional fields and length limits).
3. Service: add the function in `src/api/services/<area>.py`. All logic and ORM here; `transaction.atomic()` for
   multi-step writes; scope by the company id passed in; raise `ValueError` for user-facing failures.
4. View: class in `src/api/views/<area>.py` with `csrf_exempt`; auth check, parse JSON, validate via the schema,
   call the service, return `_json_response({"message", "data"}, status)`. Map errors: 400 validation, 401, 402/403
   where the plan says, 404, 500 generic message (log details, never return them).
5. Route in `src/api/urls.py` beside siblings.
6. Verify by running: `manage.py check --settings=conf.settings_test`; a throwaway script (scratchpad) using
   `django.test.Client` on `conf.settings_dev`-style SQLite (or the local stack + curl) hitting happy path,
   unauthenticated, wrong company, invalid input; run existing affected test modules. Report the real output.
7. Black-format only the files you touched (`./venv/bin/black --line-length=120`).
