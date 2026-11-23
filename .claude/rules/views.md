---
paths:
  - "src/api/views/**/*.py"
  - "src/api/urls.py"
  - "src/api/schemas/**/*.py"
---
# API view rules (plain Django, no DRF)
- Class-based `django.views.View`, `@method_decorator(csrf_exempt, name="dispatch")`, one method per HTTP verb.
  Copy the nearest existing view (e.g. `src/api/views/support.py`, `src/api/views/onboarding.py`).
- A view only: authenticates, parses JSON defensively (invalid body -> 400 "Invalid JSON body"), validates with
  the marshmallow schema in `src/api/schemas/<area>.py`, calls ONE function in `src/api/services/<area>.py`,
  returns via the file's `_json_response`. No ORM queries and no business logic in views.
- Authenticated endpoints check `request.user.is_authenticated` (401 "User not authenticated") and take the
  company only from `request.company_id` (400 "No company found in token"), never from the body or URL.
  Staff-only endpoints check `request.user.is_staff`.
- Success: `{"message": "...", "data": {...}}` with 200/201. Errors: `{"message": "..."}` plus `"error_code"`
  when the FE branches on it. 402 = plan limit (`billing_services.PLAN_LIMIT_ERROR_CODE`), 403 +
  `account_needs_upgrade` = suspended member. Do not invent new status conventions.
- Never leak whether an email/account exists on unauthenticated endpoints unless the contract says so.
- Register new routes in `src/api/urls.py` next to their siblings.
- Any change to a request/response shape or URL is a contract change: it needs a `contracts/api.md` entry and
  the matching `fe/src/api/*.ts` change in the same plan. Stop and report if the plan did not allow it.
