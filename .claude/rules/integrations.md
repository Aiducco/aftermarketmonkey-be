---
paths:
  - "src/integrations/**/*.py"
  - "src/management/commands/**/*.py"
---
# Integration and batch-job rules
- Distributor HTTP lives in `src/integrations/clients/<slug>/client.py`; logic in `src/integrations/services/`;
  live inventory in `src/integrations/live_inventory/`. Copy the closest existing distributor.
- Respect `src/integrations/rate_limit.py`; credentials only via `src/integrations/credentials.py`; never log
  secrets, tokens or full credential payloads.
- Never fabricate fitment, pricing or inventory. Unknown stays unknown; do not default a missing value to a
  plausible one. Pricing sync paths are gated by plan/connection state: keep those gates.
- Management commands run locally but write to the DB in the active settings: default = PRODUCTION. Check disk
  (`df -h` on the DB host) before any bulk write, support a dry-run/limit flag, and make reruns idempotent.
- Network calls need a timeout and bounded retries. Scrapers must not follow user-supplied URLs (SSRF).
- No new dependency without a line of justification in the plan.
