# Turn 14 — Handoff

Two things live here: **what has already been built and deployed to the DB**, and **what the
next agent needs to find out on the server before any more code is written**.

Read §1 first. The production pipeline is failing on every run, and that matters more than
anything in the Turn 14 modernisation plan.

Companion doc: [TURN14_INTEGRATION_PLAN.md](TURN14_INTEGRATION_PLAN.md) — the full analysis
of Turn 14's proposed model against our integration.

---

## 1. STOP AND READ — production is broken right now

All of this is from the production DB (`5.161.121.143`, `scheduled_task_execution` and
`integration_pricing_sync_job`), not from the server shell.

### 1.1 `ingest_all_providers` fails on every single run

```
error_message: "keywords must be strings"
Every run since at least 2026-08-24 00:00. 41 of 83 runs failed in the last 14 days.
Same error kills ingest_all_providers_sync_all_master_parts (39/81 failed).
```

The message is almost certainly Meilisearch rejecting an index attribute (a non-string in a
`searchableAttributes` / `filterableAttributes` / `stop-words` payload, or a non-string document
key). **Nobody has found the actual traceback yet** — `scheduled_task_execution.error_message`
only stores `str(e)`. It is in the app log on the server. That is job #1.

### 1.2 Consequence: nobody's pricing has synced in 5+ days

Phase 3 (`enqueue_all_active_company_provider_pricing_jobs`) runs *after* Phase 2, so when
Phase 2 dies, no pricing jobs are ever enqueued.

```
ingest_all_providers_enqueue_pricing_jobs   last ran 2026-08-20   (then stopped)
ingest_all_providers_meilisearch_reindex    last ran 2026-08-20   (then stopped)
```

Turn 14 pricing staleness, per company, as of 2026-08-25:

| Company | Last successful pricing sync | Days stale |
|---|---|---|
| Insane Automotive and Offroad | 2026-07-05 | **51** |
| DC Customs | 2026-07-07 | **49** |
| Texas Track Works | 2026-07-07 | **49** |
| Advanced Trucks Inc | 2026-07-07 | **49** |
| Southern Off-Road Specialists | 2026-07-07 | **49** |
| Parsee | 2026-07-22 | **34** |
| Gel | 2026-08-12 | 13 |
| AftermarketScout Demo | 2026-08-12 | 13 |
| Trident | 2026-08-19 | 6 |
| TICK_PERFORMANCE / Trident Motorsports / DMZ Apps / Swampcat / THOR Offroad | 2026-08-20 | 5 |

Job queue health:

```
COMPLETED  6874
FAILED     5679          <- 45% failure rate lifetime
RUNNING    1405          <- stuck; claimed and never finished (OOM kills / restarts)
```

The 1 405 stuck `RUNNING` rows are never reclaimed — `claim_next_open_job` only picks up `OPEN`,
and there is no stale-RUNNING sweeper for this table (there is one for
`scheduled_task_execution`, via `cleanup_stale_started_executions`, but nothing equivalent
here). **This is a real bug and worth fixing regardless of the Turn 14 work.**

### 1.3 Other providers permanently broken

| Task | Failures | Cause (from `error_message`) |
|---|---|---|
| `ingest_all_providers_quadratec` | **81/81** | `Quadratec static feed file missing: /app/resources/quadratec/pricingSheet_quad.xlsx` |
| `ingest_all_providers_dlg` | **81/81** | `DLG file not found on SFTP: /uploads/dlg_inventory.csv` |
| `ingest_all_providers_keystone` | 40/83 | `Login rejected by FTP server. 530 Login or password incorrect!` |

Note the Quadratec path is `/app/resources/...` — a **container** path, while
`command_runner.sh` in this repo uses `/root/aftermarketmonkey_be`. Those disagree. Confirming
how the app is actually deployed (Docker? bare venv? both?) is on the server checklist below.

### 1.4 The real cadence is not what the code comments claim

Derived from run timestamps, last 3 days:

| Task | Actual schedule | Avg | Max | Notes |
|---|---|---|---|---|
| `ingest_all_providers` | **every 4 h** (00,04,08,12,16,20) | 2 h 30 m | **8 h** | avg runtime > half the interval; max runtime **exceeds** it → **runs overlap** |
| `fetch_turn_14_inventory_updates` | **every 30 min**, 24/7 | 31 s | 38 m | already close to Dan's 10-min tier |
| `fetch_turn_14_items_updates` | **last ran 2026-04-22** | — | — | **dead for 4 months** |
| `refresh_confirmed_purchase_orders` | never audited | — | — | unknown — see checklist |
| `process_integration_pricing_sync_jobs` | never audited | — | — | unknown — see checklist |
| `index_parts_meilisearch` | never audited | — | — | unknown — see checklist |

Two corrections to my own earlier analysis in the plan doc, both now fixed there:

- I described `ingest_all_providers` as **nightly**. It is **4-hourly**.
- I described the inventory delta as running **once nightly at `minutes=30`**. It has its own
  30-minute cron and has been running all along.

**The 8-hour max runtime on a 4-hour cron is the most under-appreciated fact here.** Overlapping
ingest runs mean two processes doing the same upserts concurrently — a plausible contributor to
the OOM kills that left 1 405 pricing jobs stuck in `RUNNING`.

---

## 2. What is already built (this branch, uncommitted)

**Nothing is committed. Nothing is deployed. Two migrations ARE already applied to the
production DB.** So production currently has three unused tables and one unused column, while
running the old code. Harmless, but code and schema must be resynced.

### 2.1 Phase 0 — rate limiting (done, tested live)

| File | What |
|---|---|
| `src/integrations/rate_limit.py` *(new)* | Cross-process fixed-window limiter, Postgres-backed. Hard buckets (hour/day) raise `RateBudgetExhausted`; soft buckets (minute/second) sleep. `UsageMeter` for audit rows. |
| `src/integrations/clients/turn_14/rate_limit.py` *(new)* | Turn 14's limits as buckets; process-wide token cache keyed by client_id. |
| `src/models.py` → `ApiRateBucket` | Counter table. **Migration `0175` APPLIED to prod.** |
| `clients/turn_14/client.py` | Removed a bogus `@limits(calls=20, period=60)` that capped us at 20 req/min against an allowed 5 000/h. Buckets now per-credential. 429 → `RateBudgetExhausted`, honouring `Retry-After`. |
| `clients/turn_14/order_client.py` | Had **no** rate limiting at all. Now shares the credential's budget and token with the catalog client; `2/s` on quotes. |
| `integration_pricing_sync_jobs.py` | Budget exhaustion → job returns to `OPEN` deferred via new `not_before` column, not `FAILED`. **Migration `0175`.** |
| `ingest_all_providers.py` | Writes requests/elapsed/rate/budget-left into the audit row. |

Measured live (30 real requests): **20.0 → 60.3 req/min (3.0×)**. Turn 14's own HTTP latency
(~845 ms/request) is now the bottleneck, not us; the hourly ceiling is 83 req/min.

Why hard buckets raise instead of sleeping: Turn 14 **deactivates credentials** hit by the
hourly limit 10× in 30 days. `RateBudgetExhausted` deliberately does *not* subclass
`Turn14APIException`, because the per-brand `except Turn14APIException: continue` handlers would
otherwise swallow it and march through 464 more doomed brands.

### 2.2 Phases 2–7 — built but NOT verified at scale

| File | What | State |
|---|---|---|
| `services/turn_14_global.py` *(new)* | Single credential resolver for shared tables; `TURN14_GLOBAL_CLIENT_ID/_SECRET` settings override with fallback to `primary=True`. | untested in prod |
| `services/turn_14_sweeps.py` *(new)* | Flat sweeps: items, items/data, inventory, fitment, dropship, shipping estimates, per-company pricing. | smoke-tested, 2 pages each |
| `services/turn_14_order_sweeps.py` *(new)* | Hourly bulk tracking + invoices. | **never run** |
| `models.py` → `Turn14DropshipController`, `Turn14ItemShippingEstimate` | New global caches. **Migration `0176` APPLIED to prod.** | empty |
| `clients/turn_14/client.py` | `_paginated()` helper replacing 12 duplicated blocks; 8 new methods. Client is *shorter* than before. | live-tested |
| `orders/turn_14.py` | Warehouse override (`preferred_location`: default/ds/01/02/03/59), per connection or per order account. | unit-tested only |
| 5 management commands | `sync_turn14_global_sweep`, `sync_turn14_fitment_sweep`, `sync_turn14_order_sweeps`, plus `--minutes`/`--days` on the two update commands. | **never run** |

Measured page sizes (live) — the whole justification for flat sweeps:

| Endpoint | rows/page | pages | full sweep |
|---|---|---|---|
| `/v1/items` | **1 000** | 776 | 776 req |
| `/v1/items/data` | 450 | 1 724 | 1 724 req |
| `/v1/inventory` | **1 000** | 776 | 776 req |
| `/v1/pricing` | **1 000** | 776 | 776 req |
| `/v1/items/fitment` | 200 | 3 879 | 3 879 req |
| `/v1/pricing/brand/{id}` | 200 | — | **~4 200 req** |

Flat pricing floor: 776 ÷ 83.3/min = **9.3 min**. Per-brand: **~50 min**. That is the whole
difference between Dan's "< 10 minutes" being reachable and not.

### 2.3 ⚠️ Fitment volume — needs a decision before running

The 2-page smoke test wrote **30 591 fitment rows from 400 items** (~76 vehicles/item).
Extrapolated over 775 800 items: **~59 million rows** in `turn14_item_fitments`, with a
`unique_together (item_external_id, vehicle_id)` index on top.

**Do not run `sync_turn14_fitment_sweep` in production without deciding this is acceptable.**
Check disk headroom first. Options: accept it, restrict to brands we actually sell, or store
vehicle_ids as an array per item instead of one row per pair.

### 2.4 Known environment trap for the next agent

This repo's venv has **`psycopg` 3.3.4 installed but not in `requirements.txt`**. `pgbulk`
branches on whichever psycopg it can import, so locally it picks the v3 API and every
`pgbulk.upsert` dies with `'psycopg2.extensions.connection' object has no attribute 'pgconn'`.
Production (psycopg2-binary only) is fine. To test locally:

```python
import pgbulk.core
from psycopg2.extensions import quote_ident
pgbulk.core.psycopg_maj_version = 2
pgbulk.core.quote_ident = quote_ident
```

---

## 3. Server checklist — what I could not determine from the DB

Run these and paste the output back. Everything else in this doc came from the DB; these are
the genuine gaps.

### 3.1 The failing pipeline (highest priority)

```bash
# The actual traceback behind "keywords must be strings" — the DB only stores str(e)
grep -n -A 40 "keywords must be strings" /root/aftermarketmonkey_be/logs/*.log | tail -80
```

```bash
# Same, if the app runs in Docker rather than from /root
docker ps -a && docker compose logs --tail 400 2>/dev/null | grep -B 5 -A 40 "keywords must be strings"
```

### 3.2 The real schedule

```bash
crontab -l; echo "--- other users ---"; for u in $(cut -d: -f1 /etc/passwd); do crontab -l -u $u 2>/dev/null | sed "s/^/[$u] /"; done
```

```bash
ls -la /etc/cron.d/ /etc/cron.daily/ /etc/cron.hourly/ && cat /etc/cron.d/* 2>/dev/null
```

```bash
systemctl list-timers --all --no-pager
```

I specifically need to know whether these are scheduled, and how often — none of them write
audit rows, so the DB cannot tell me:

- `process_integration_pricing_sync_jobs` (drains the pricing queue — with `--workers`?)
- `refresh_confirmed_purchase_orders` (PO status/invoice refresh)
- `index_parts_meilisearch`
- `check_company_provider_connections`
- `fetch_turn_14_items_updates` — **last audited run 2026-04-22**; is its cron gone?

### 3.3 Deployment shape

```bash
# Container path /app/resources/... appears in errors, but command_runner.sh uses /root/... — which is real?
docker ps --format '{{.Names}}\t{{.Image}}\t{{.Status}}' ; ls -la /app 2>/dev/null; ls -la /root/aftermarketmonkey_be
```

```bash
cat /root/aftermarketmonkey_be/command_runner.sh; git -C /root/aftermarketmonkey_be log --oneline -5; git -C /root/aftermarketmonkey_be status --short
```

```bash
/root/aftermarketmonkey_be/venv/bin/pip list 2>/dev/null | grep -iE "psycopg|pgbulk|django |meilisearch"
```

### 3.4 Capacity — decides whether overlap and fitment are survivable

```bash
free -h; df -h; nproc; uptime
```

```bash
# Did the OOM killer take the ingest process? Explains the 1405 stuck RUNNING jobs.
dmesg -T 2>/dev/null | grep -i -E "killed process|out of memory" | tail -20
journalctl -k --no-pager | grep -i "out of memory" | tail -20
```

```bash
# Are two ingest runs overlapping right now?
ps aux | grep -E "manage.py|python" | grep -v grep
```

### 3.5 Broken feeds (quick wins, unrelated to Turn 14)

```bash
ls -la /app/resources/quadratec/ /root/aftermarketmonkey_be/resources/quadratec/ 2>/dev/null
```

Keystone FTP credentials are being rejected (`530 Login or password incorrect`) — needs
rotating with Keystone, not a code fix.

---

## 4. Recommended order of work

The Turn 14 modernisation is worth doing, but it is **not** the most urgent thing.

1. **Fix `keywords must be strings`.** Every ingest run has died on it, which is why no pricing
   has synced for anyone in 5+ days. Nothing else matters until this is green.
2. **Reclaim the 1 405 stuck `RUNNING` pricing jobs** and add a stale-RUNNING sweeper for
   `integration_pricing_sync_job`, mirroring `cleanup_stale_started_executions`.
3. **Stop `ingest_all_providers` overlapping itself** — 8 h max runtime on a 4 h cron. Either a
   lockfile/advisory lock, or lengthen the interval. Likely implicated in the OOM kills.
4. **Deploy Phase 0** (rate limiting). Self-contained, 3× throughput, migrations already applied.
5. **Then** the flat sweeps — and re-measure a real pricing sync against the numbers in §2.2
   before trusting them.
6. Decide the fitment question in §2.3 before running that sweep.
7. Decide the house-account question: `primary=True` is **Trident Motorsports (a customer)**, and
   `TICK_PERFORMANCE`'s Turn 14 credentials are **empty strings** — so every code path hardcoded
   to TICK_PERFORMANCE is silently dead. `TURN14_GLOBAL_CLIENT_ID/_SECRET` exists to fix this
   without a deploy.

## 5. Open questions for Dan (Turn 14)

1. Is there a bulk/paginated `v1/dropship` list, or only `GET /v1/dropship/{dropship_id}`?
2. Is `GET /v1/pricing/changes` supported? We depend on it; it is not in the documentation.
3. Does "unlocked by valid credentials and a price record for that SKU" mean we may serve cached
   global data to a customer whose own credentials have not yet pulled pricing for that SKU?
4. `items/updates` takes `days` (plural) — your note says `day=1`. Confirm.
5. Integrator credentials: separate client_id, or elevated permissions on ours? **Does the global
   credential get its own 5 000/h quota, or share the customer's?**
6. Your glossary lists three warehouses (PA, TX, NV) + `ds`. The enhancement mentions
   01/02/03/**59** — what is 59?

## 6. Security note

The production root password was shared in plaintext in the chat that produced this document.
**Rotate it.** Also rotate the Turn 14 API client_id/secret, which were shared the same way.
