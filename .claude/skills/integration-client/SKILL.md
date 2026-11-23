---
name: integration-client
description: Recipe for adding or extending a distributor integration (client, services, live inventory). Use when a task adds a distributor or a new call to an existing one. Earlier distributor-research agents are archived under workspace/docs/archive/be-legacy/ for reference.
---
1. Pick the closest existing distributor in `src/integrations/clients/<slug>/client.py` and its service in
   `src/integrations/services/`; read both before writing.
2. HTTP in the client only: timeouts, bounded retries, rate limit via `src/integrations/rate_limit.py`, credentials
   via `src/integrations/credentials.py`. Never log secrets.
3. Never fabricate fitment, pricing or inventory; unknown stays unknown. Keep plan/connection gating on pricing sync.
4. Register in `src/integrations/` wiring the way the sibling does (provider seed, live inventory registry).
5. Verify against fixtures or a sandbox using a throwaway script on `conf.settings_dev`; never call a distributor
   with real customer credentials from a dev run; run the sibling's existing test module.
6. Report match rates or samples you actually observed, and what needs real credentials to verify.
