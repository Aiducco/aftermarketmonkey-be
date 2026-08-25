# Turn 14 Integration — Current State & Plan for Dan Ziegler's New Model

- Reference: Dan Ziegler (Turn 14), email 2026-08-21 — "Global Cache/DB for T14" proposal.
- Reference: Turn 14 API technical docs (apiary spec, 57 documented endpoints).
- Reference: Turn 14 `api_settings.php` — Acceptable Usage Policy (rate limits, §3).

**"Global" here means "fetched once with one designated set of credentials and shared by every
company."** That is already what our schema supports. Until Turn 14 issues us integrator-level
credentials we keep using a designated house connection; when they do, we swap one resolver
function (Phase 1) and nothing downstream changes.

---

## 1. Executive summary

Our **data model already matches Dan's global/customer split exactly.** Every table he lists as
cacheable — items, items/data, items/fitment, inventory, locations — is already a
company-agnostic table. The only per-company table is `turn14_brand_pricing`. There is no
schema boundary to migrate.

What does not match is how we *drive* those tables. Findings in order of impact:

| # | Finding | Impact |
|---|---|---|
| 1 | The catalog client is self-throttled to **20 requests/minute**. Turn 14 allows **5 000/hour**. | We use ~24% of our quota. This is the single biggest cause of multi-hour syncs. |
| 2 | That throttle is a **process-wide shared counter** (`ratelimit` holds state on the decorator, created once at class-definition time — verified empirically). Turn 14's quota is **per credential set**. | All 11 company connections queue behind **one** 20/min budget instead of getting 11 × 5 000/hour. |
| 3 | We page **per brand** across 464 brands. Per-brand endpoints return **200 rows/page**; the flat endpoints return **1 000**. | Pricing: ~4 200 requests/company instead of **776**. A **5.4×** waste, before the throttle. |
| 4 | **Fitment has never run** — `turn14_item_fitments` = **0 rows**. | We discard Turn 14's ACES fitment for 793 k items. |
| 5 | `primary=True` is **Trident Motorsports** — a customer. All "global" catalog fetches run on a customer's credentials. | Shared data populated from a customer account; their quota absorbs our global sync. |
| 6 | **TICK_PERFORMANCE's Turn 14 credentials are empty strings.** Every code path that hardcodes `Company.objects.filter(name='TICK_PERFORMANCE')` throws `ValueError` and is silently swallowed by `except ValueError: continue`. | **New-brand catalog ingestion is dead code.** When a new brand appears, its items/media/inventory are never fetched. |
| 7 | Token issuance is limited to **10/minute per IP**; our client permits **20/min**, and caches tokens **per client instance** while constructing a new client **inside the brand loop**. | 464 token requests per sweep from one IP, against a 10/min ceiling. |
| 8 | `v1/tracking`, `tracking/package_details`, `v1/dropship`, `shipping/item_estimation`, and bulk date-range `invoices`/`orders` are **not implemented**. | No package tracking, no dropship fees, no landed cost; per-PO polling instead of one bulk sweep. |
| 9 | Cadence is 4-hourly ingest + a 30-minute inventory delta, not Dan's five tiers. But `ingest_all_providers` **fails on every run** (`keywords must be strings`), so Phase 3 never enqueues pricing jobs — nobody's Turn 14 pricing has synced in 5+ days, and five companies are ~7 weeks stale. | This, not the sync design, is the live production problem. See [TURN14_HANDOFF.md](TURN14_HANDOFF.md) §1. |
| 10 | 7.67 M `turn14_brand_pricing` rows; ~2.8 M belong to companies with no active Turn 14 connection. | Orphaned storage. |

---

## 2. Current state

### 2.1 Live production numbers (2026-08-25)

```
turn14_brands            464     (all 464 mapped)
turn14_items         793 420
turn14_brand_data    742 506
turn14_brand_inventory 771 768
turn14_item_fitments       0     <-- never populated
turn14_locations           4
turn14_brand_pricing 7 671 615   across 11 companies (~780k each)

Turn 14 CompanyProviders: 14 total, 11 active
  primary=True        -> Trident Motorsports (id 14)   <-- a customer, not the house account
  TICK_PERFORMANCE    -> id 1, credentials EMPTY       <-- hardcoded in code, non-functional
  3 connections have client_id lengths of 7/10/12 chars (real ones are 40) -- likely invalid
```

### 2.2 Measured API page sizes

Probed live against production, one request each:

| Endpoint | rows/page | total_pages | requests for a full sweep |
|---|---|---|---|
| `GET /v1/items?page` | **1 000** | 776 | **776** |
| `GET /v1/items/data?page` | **450** | 1 724 | **1 724** |
| `GET /v1/inventory?page` | **1 000** | 776 | **776** |
| `GET /v1/pricing?page` | **1 000** | 776 | **776** (per company) |
| `GET /v1/items/fitment?page` | **200** | 3 879 | **3 879** |
| `GET /v1/pricing/brand/{id}?page` | **200** | — | **~4 200** (per company, all 464 brands) |

**The flat endpoints return 5× more rows per request than the per-brand ones.** That is the
core of Dan's efficiency argument and it is measurable, not theoretical.

### 2.3 Code map

| Concern | File |
|---|---|
| Read-only catalog/pricing/inventory client | `src/integrations/clients/turn_14/client.py` |
| Order API client | `src/integrations/clients/turn_14/order_client.py` |
| Fetch + upsert services | `src/integrations/services/turn_14.py` (2 719 lines) |
| Order adapter | `src/integrations/orders/turn_14.py` |
| On-demand single-item inventory | `src/integrations/live_inventory/turn_14.py` |
| Global catalog -> MasterPart/ProviderPart | `src/integrations/services/master_parts.py` |
| Per-company pricing job queue | `src/integrations/services/integration_pricing_sync_jobs.py` |
| Nightly orchestrator | `src/management/commands/ingest_all_providers.py` |
| Confirmed-PO refresh | `src/integrations/services/confirmed_purchase_order_sync.py` |

### 2.4 Endpoint coverage vs. Dan's list

**Global / cacheable:**

| Endpoint | Our method | Status |
|---|---|---|
| `GET /v1/items?page` | — | MISSING (per-brand only) |
| `GET /v1/items/brand/{id}` | `get_items_for_brand` | used, but only for *newly discovered* brands — and that path is broken (finding #6) |
| `GET /v1/items/{item_id}` | — | missing |
| `GET /v1/items/updates?page&days` | `get_items_updates` | OK, nightly, `days=1` |
| `GET /v1/items/data?page` | — | MISSING (per-brand only) |
| `GET /v1/items/data/brand/{id}` | `get_brand_media` | new brands only (broken path) |
| `GET /v1/items/fitment?page` | — | MISSING |
| `GET /v1/items/fitment/brand/{id}` | `get_item_fitment_for_brand` | implemented, **never scheduled — 0 rows** |
| `GET /v1/inventory?page` | — | MISSING (per-brand only) |
| `GET /v1/inventory/brand/{id}` | `get_inventory_items_for_brand` | new brands only (broken path) |
| `GET /v1/inventory/{item_id}` | `get_inventory_item` | OK, live refresh |
| `GET /v1/inventory/updates?minutes` | `get_inventory_items_updates` | OK, nightly, `minutes=30` |
| `GET /v1/dropship/{id}` | — | MISSING (we store `dropship_controller_id`, never resolve it) |
| `GET /v1/shipping` | `get_shipping_options` | OK (service levels) |
| `GET /v1/shipping/item_estimation` | — | MISSING |
| `GET /v1/locations` | `get_locations` | implemented, **not scheduled** (4 rows) |
| `GET /v1/brands` | `get_brands` | OK, nightly |

**Customer-specific:**

| Endpoint | Our method | Status |
|---|---|---|
| `GET /v1/pricing?page` | — | MISSING (per-brand only) |
| `GET /v1/pricing/brand/{id}` | `get_pricelists` | OK, full sweep per company |
| `GET /v1/pricing/changes` | `get_pricing_changes` | OK — but **undocumented in the spec**; confirm with Dan |
| `POST /v1/quote` | `create_quote` | OK |
| `POST /v1/order`, `/order/from_quote` | `create_order`, `promote_quote_to_order` | OK |
| `GET /v1/orders/{id}`, `/orders/po/{ref}` | `get_order`, `get_orders_by_po_number` | OK |
| `GET /v1/orders?start&end` | — | missing |
| `GET /v1/invoices/po/{ref}` | `get_invoices_by_po_number` | OK, per-PO |
| `GET /v1/invoices?start&end` | — | MISSING (Dan's hourly uninvoiced sweep) |
| `GET /v1/tracking` | — | MISSING |
| `GET /v1/tracking/package_details` | — | MISSING |
| `GET /v1/credits`, `/payments`, `/documents` | — | missing (not in Dan's proposal) |

### 2.5 Current schedule

All Turn 14 catalog work runs inside `ingest_all_providers` (`_run_turn14`), which production
runs **every 4 hours** (00/04/08/12/16/20), not nightly — average runtime 2 h 30 m, maximum 8 h,
so runs overlap. The inventory delta has its own **30-minute** cron and has been running all
along. `fetch_turn_14_items_updates` has not run since **2026-04-22**. See
[TURN14_HANDOFF.md](TURN14_HANDOFF.md) §1.4 for the measured schedule.

```
Phase 1  fetch_and_save_turn_14_brands()              GET /brands
         sync_unmapped_turn_14_brands_to_brands()
         if new brands: items + items/data + inventory  <-- BROKEN (finding #6)
         fetch_and_save_turn_14_items_updates()        GET /items/updates?days=1
         fetch_and_save_turn_14_inventory_updates()    GET /inventory/updates?minutes=30
Phase 2  sync_all_master_parts_global()                DB-only
Phase 3  enqueue_all_active_company_provider_pricing_jobs()
           Turn 14 rows use_delta_fetch=True -> GET /pricing/changes
Phase 4  Meilisearch reindex (separate nightly cron)
```

Never scheduled: `fetch_turn14_locations`, `fetch_turn_14_all_brand_fitment`,
`fetch_turn_14_all_brand_items`, `fetch_turn_14_all_brand_pricing`.

Order refresh is a separate cron (`refresh_confirmed_purchase_orders`) polling each CONFIRMED
PO individually.

### 2.6 Credential resolution — three incompatible strategies

| Strategy | Used by | Resolves to |
|---|---|---|
| `CompanyProviders.filter(primary=True)` | brands, items/updates, inventory/updates, fitment, locations | **Trident Motorsports** (a customer) |
| `Company.objects.filter(name='TICK_PERFORMANCE')` hardcoded | `sync_unmapped_turn_14_brands_to_brands`, `..._for_turn14_brands` | id 1, **empty credentials -> dead path** |
| `CompanyBrands.filter(brand=brand).first()` | `fetch_and_save_all_turn_14_brand_{items,data,inventory}` | arbitrary company |

---

## 3. Rate limits — the real budget

From Turn 14's Acceptable Usage Policy:

| Scope | Limit |
|---|---|
| Per **IP** | 10 token requests/minute |
| Per **credential set**, per second | 5 GET, 2 quote |
| Per **credential set**, per hour | 5 000 GET |
| Per **credential set**, per 24 h | 30 000 GET |

**Sustained ceiling is the hourly one: 5 000/h = ~83/min**, well below the 5/s burst rate.
Budget long sweeps against 5 000/h, not 5/s.

**Enforcement has teeth — this is not just a 429 to retry through.** Credentials are
**deactivated** if the hourly limit is hit 10 times in 30 days, or the daily limit exceeded
twice in 30 days. Re-enabling requires a support conversation. So we need a **budget guard that
refuses to issue the request**, not just backoff-on-429.

### 3.1 What our client does wrong

`Turn14ApiClient._request` (client.py:385-389):

```python
@sleep_and_retry
@limits(calls=SECOND_LIMIT, period=1)     # 5/s      correct
@limits(calls=20, period=60)              # 20/min   <-- WRONG: not a documented limit
@limits(calls=HOUR_LIMIT, period=3600)    # 5000/h   correct
@limits(calls=DAY_LIMIT, period=86400)    # 30000/d  correct
def _request(...)
```

The `20/60` limiter is a copy-paste of the one on `_create_authorization_token` (where a
per-minute cap *is* right, because token issuance is capped per IP). Its inline comment even
says "5 requests per second", confirming the paste. **It is not a Turn 14 limit and must go.**

`sleep_and_retry` wraps the whole stack, so the tightest limiter governs: **20/min = 1 200/h**
against an allowed 5 000/h.

Two further problems with the same decorators:

- **Shared state across companies.** `ratelimit.RateLimitDecorator` stores `num_calls` /
  `last_reset` on the decorator object, built once when the class body executes. Verified: two
  separate instances share one counter. Turn 14's quota is **per credential set**, so 11
  companies should have 11 × 5 000/h — instead they all queue behind one counter.
- **Token cache is per instance.** `fetch_and_save_all_turn_14_brand_{items,data,inventory}`
  construct a client **inside** the brand loop -> 464 token requests per sweep, from one IP,
  against a **10/min** ceiling. And our token limiter allows 20/min, i.e. **2× the documented
  per-IP limit**.

### 3.2 Cost model — before and after

Per-company full pricing sweep:

| | requests | at current 20/min | at 5 000/h |
|---|---|---|---|
| Per-brand (`pricing/brand/{id}`, 200/page) | ~4 200 | **3.5 h** | 50 min |
| Flat (`pricing?page`, 1 000/page) | **776** | 39 min | **~9 min** (~2.6 min at 5/s burst) |

All 11 companies, today: 46 200 requests through one shared 20/min counter = **~38 hours**.
That is why the delta path (`pricing/changes`) had to be introduced.

After Phase 0 + Phase 2 the picture is (measured, not projected):

- Per-credential limiters mean the 11 companies no longer share one budget, so they can run
  concurrently without starving each other. Each has its own 5 000/h.
- But Turn 14's own HTTP latency (~845 ms/request) is now the binding constraint on any single
  sweep. Sequential paging reaches ~71 req/min against an 83 req/min hourly ceiling — so a
  single sweep is already near optimal and extra concurrency *within* one company buys little.
- Floor for a flat pricing sweep: 776 ÷ 83.3 = **9.3 min**. That is Dan's "< 10 minutes", and
  it is reachable — but only via Phase 2's flat endpoints. The same sweep done per-brand is
  4 200 requests, a **50 min floor**, no matter how well we rate-limit.

In other words Phase 0 removed the artificial ceiling; **Phase 2 is what actually delivers the
< 10 minute number.**

Daily budget for the **global** credential set (30 000/day):

```
items          776
items/data   1 724
inventory      776
             -----
daily total  3 276   -> fits in a single 5 000/h window (~11 min at 5/s)
fitment      3 879   -> weekly, its own window
inventory/updates  144 runs/day x ~1-3 pages  =  ~150-450
items/updates        6 runs/day x few pages   =  ~30
                                          total well under 30 000/day
```

Per-**company** credential set: pricing 776/day + hourly order sweeps (~48/day) ~= **825/day**
against 30 000. Ample headroom.

---

## 4. Mapping Dan's proposal to work

| Dan's item | Gap | Phase |
|---|---|---|
| Global cache: items / items/data / fitment / inventory / dropship / shipping / locations | Tables already global; drivers per-brand and partly per-customer | 1, 2, 4 |
| Weekly: `items/fitment` | Never runs | 3 |
| Daily full sweep: items, items/data, pricing, locations, dropship, shipping, inventory | Brand-scoped only, new brands only; locations/dropship/shipping absent | 2, 4 |
| Hourly: tracking + package_details; invoices for uninvoiced orders | Not implemented — per-PO polling | 5 |
| Every 4 h: `items/updates?days=1` | Nightly only | 6 |
| 10-min deltas: `inventory/updates?minutes=15` | Nightly only, `minutes=30` | 6 |
| Enhancement: warehouse override (01/02/03/59) | `_DEFAULT_LOCATION = "default"` hardcoded | 7 |

Corrections to send back to Dan:

- He writes `items/updates?day=1`; the parameter is **`days`** (plural). We already use `days`.
- He lists `v1/dropship` and `v1/shipping` as sweepable collections. The spec documents only
  `GET /v1/dropship/{dropship_id}` (single controller) and `GET /v1/shipping` (account service
  levels) — no paginated dropship list. Per-item shipping cost is
  `GET /v1/shipping/item_estimation[/brand/{id}]`. **Ask whether a bulk dropship list exists**;
  otherwise we resolve the distinct `Turn14Items.dropship_controller_id` set individually.
- Turn 14's own glossary says there are **three** warehouses (PA, TX, NV) plus `ds` for
  dropship. Dan's enhancement mentions 01/02/03/**59**. Confirm what 59 is.

---

## 5. Plan

### Phase 0 — Rate limiting done correctly ✅ DONE

Implemented; measured against the live API. What changed:

| Change | File |
|---|---|
| Cross-process fixed-window limiter, hard (raise) vs soft (sleep) buckets, usage meter | `src/integrations/rate_limit.py` (new) |
| Turn 14's limits as buckets + process-wide token cache | `src/integrations/clients/turn_14/rate_limit.py` (new) |
| Counter table | `src.models.ApiRateBucket`, migration `0175_api_rate_buckets` |
| Removed the bogus `20/min`; buckets now per-credential; token cache process-wide | `clients/turn_14/client.py` |
| Added limiting where there was **none**; `2/s` on quotes; shares budget + token with the catalog client | `clients/turn_14/order_client.py` |
| Budget exhaustion → job returns to OPEN deferred via new `not_before`, not FAILED | `integration_pricing_sync_jobs.py` |
| Sweep cost (requests / elapsed / rate / budget left) into the audit row | `ingest_all_providers.py` |
| Upstream 429 → shut the local bucket + defer (was: generic API error, silently skipped a brand) | both clients |
| Self-imposed 100/min governor so a fixed hourly window cannot be burst | `clients/turn_14/rate_limit.py` |
| Removed the exponential-backoff 429 retry loop — it retried into a spent budget | `services/turn_14.py` |

Design decisions worth knowing:

- **Postgres, not Redis.** Redis is not deployed. The counter must be shared across processes
  (`ingest_all_providers`, the delta cron and `process_pricing_sync_jobs` can overlap), and
  Postgres is what every worker already shares. All backend-specific code is inside
  `_consume()`; swapping to Redis later is that one function.
- **Hard buckets raise, they never sleep.** Turn 14 deactivates credentials that hit their
  hourly limit 10× in 30 days. Sleeping on an hour-long window would pin a worker *and* keep us
  against the ceiling. `RateBudgetExhausted` deliberately does not subclass `Turn14APIException`,
  because the per-brand `except Turn14APIException: continue` handlers would otherwise march
  through 464 more doomed brands.
- **One round trip per request.** All buckets are consumed in a single
  `INSERT … ON CONFLICT DO UPDATE … WHERE` over a multi-row `VALUES` list. Three separate round
  trips cost 383 ms per request from a remote DB; one costs 128 ms (and ~2–5 ms on the app
  server, where the DB is local).
- **Credential IDs are hashed** into bucket keys — a client_id must never appear in a table or
  a log line.
- **A 429 is treated as authoritative, our counter is not.** Turn 14 customers may hand the same
  client_id to third-party integrators (their settings page lists ~24 of them), so our count is
  a lower bound on what those credentials actually spent. On 429 we honour `Retry-After`, slam
  the local hour bucket shut so concurrent workers stop without each rediscovering the 429, and
  raise `RateBudgetExhausted`. Previously a 429 was a generic API error that the per-brand
  `except Turn14APIException: break` handlers turned into "skip this brand and carry on".
- **A self-imposed 100/min governor.** The hour bucket is a *fixed* window, so nothing stopped
  us spending 5 000 in the last two minutes of one hour and 5 000 in the first two of the next
  — 10 000 in four minutes, which any rolling-window limiter upstream would reject. 20% above
  the hourly average, so it never binds on normal sequential traffic (measured: 58.9/min with
  the governor vs 60.3/min without) but bounds the burst when workers run concurrently.
- Accepted tradeoff: when a hard bucket is full, the per-second bucket may still be charged one
  slot for the rejected request. Self-correcting within a second.

Measured, from a dev machine (30 live requests, `client_id` 363f…):

```
old cap                 20.0 req/min
now                     60.3 req/min   -> 3.0x
Turn 14 hourly ceiling  83.3 req/min   (5000/h)

Turn 14 HTTP latency    ~845 ms/request  <-- now the bottleneck, not us
token requests for 5 freshly-constructed clients: 1 (was 5)
```

**The limiter is no longer the constraint — Turn 14's own latency is.** Sequential requests top
out near 71/min, and the hourly ceiling is 83/min, so sequential paging is already close to
optimal; concurrency would only help within a burst. The floor for a flat pricing sweep is
776 ÷ 83.3 = **9.3 minutes**, which is what makes Dan's "< 10 minutes" reachable — but only
with Phase 2's flat endpoints. The same sweep per-brand is 4 200 requests = **50 min minimum**,
regardless of how well we rate-limit.

**Deferred to Phase 1 deliberately:** hoisting the six in-loop client constructions
(`turn_14.py:502/643/850/983/1700/1844`). The process-wide token cache already removed the harm
(464 token requests → 1), and those six sites are exactly the credential-resolution code Phase 1
rewrites — refactoring them twice would be churn.

### Phase 1 — One credential resolver (½ day)

```python
def get_global_company_provider() -> CompanyProviders | None:
    """The connection whose credentials populate the SHARED Turn 14 tables.

    Precedence: settings.TURN14_GLOBAL_CLIENT_ID/_SECRET if set (the handover target for
    Turn 14's integrator credentials), else the designated house connection.
    """
```

- Replace all three strategies in §2.6 with this one function.
- **Fix finding #6 first**: either populate TICK_PERFORMANCE's credentials or repoint the house
  account. Today those code paths silently no-op, so new brands never get catalogued.
- Delete the `CompanyBrands...first()` credential path entirely.
- Change `except ValueError: continue` to a **loud** failure — a missing global credential
  should fail the scheduled task, not silently skip 464 brands.
- Reconsider `primary=True` pointing at Trident Motorsports: global sweeps currently consume a
  customer's 5 000/h quota and populate shared tables from their account.
- Construct **one** client per sweep and pass it down.

### Phase 2 — Flat global sweeps (2–3 days)

Add to `client.py`: `get_items(page)`, `get_items_data(page)`, `get_inventory(page)`,
`get_items_fitment(page)`, `get_pricing(page)` — the unscoped `v1/...?page=` variants
(1 000 rows/page vs. 200).

Rewrite the four `fetch_and_save_all_turn_14_brand_*` services as flat sweeps that page the
catalog once and `pgbulk.upsert` in batches, keyed on `external_id` as they already are. Brand
attribution comes from the payload's `brand_id`, resolved against a preloaded
`{external_id: Turn14Brand}` map — no per-brand request needed.

Keep the per-brand functions: still correct for the "new brand discovered" path and for the
pricing-delta path.

**Beyond speed:** a flat sweep is the only way to notice items Turn 14 *removed*. The
per-brand upsert-only path can never deactivate a vanished SKU. Add `last_seen_at` (or compare
`updated_at` against the sweep's start timestamp) and mark unseen items `active=False` after a
**completed** sweep only.

### Phase 3 — Fitment (1 day)

- Schedule weekly, using the flat `items/fitment?page=` sweep: **3 879 requests**, ~13 min at
  5/s, comfortably inside one 5 000/h window.
- Run outside the nightly ingest window; `--resume` already exists as the restart mechanism.
- Wire `Turn14ItemFitment.vehicle_id` -> `VcdbVehicle.vehicle_id` -> `MasterPartFitment`.
  `sync_master_part_fitments_from_turn14_vcdb` already exists for this join and currently has
  nothing to read.

**Highest customer-visible payoff in the plan** — year/make/model search across the whole
Turn 14 catalog, which we cannot do today.

### Phase 4 — New endpoints (2 days)

| Endpoint | Storage | Purpose |
|---|---|---|
| `GET /v1/locations` | `Turn14Location` (exists) | Add to nightly. 4 stale rows today. |
| `GET /v1/dropship/{id}` | new `Turn14DropshipController` (`external_id`, `charges` JSON) | Dropship fees for landed cost. Fetch distinct `dropship_controller_id` from `turn14_items`. |
| `GET /v1/shipping/item_estimation` (flat -- confirmed 2026-08-25 to be 1000 rows/page same as the brand-scoped variant, 795 requests vs. 1081 walking brand by brand) | new `Turn14ItemShippingEstimate` (`item_external_id`, `can_ship`, min/avg/max, `fees` JSON) | Per-item shipping estimate -> true landed cost in search. |

All three are global-cache tables (no company FK).

### Phase 5 — Order-side hourly refresh (done, revised 2026-08-25)

Originally planned as a separate hourly `turn14_order_sweeps.py` service (tracking +
package_details + invoices, standalone from order status). Superseded once live testing
against company 16 showed the pieces don't actually separate cleanly:

- `GET /v1/orders?start_date&end_date` **never returns a closed order at all** — confirmed
  live, a real, verified-Closed order with entries spanning weeks was completely absent from
  it for a date range that fully contained it, while the same account's currently-open orders
  showed up fine. So bulk orders can only ever prove "still open", never "closed".
- `GET /v1/invoices?start_date&end_date` filled that gap exactly: a live test batch of company
  16's 20 confirmed Turn14 POs (all long since closed) were **all 20** present in one invoices
  fetch over a ~5-week window, each carrying its own `tracking` array inline — making the
  separate `GET /v1/tracking` call redundant for anything that's actually shipped.

So Phase 5 is now folded directly into `confirmed_purchase_order_sync._refresh_turn14_orders_for_company`
instead of living in its own module: one bulk `GET /v1/orders` call plus one bulk
`GET /v1/invoices` call per company per cycle (paginated — walks every page, not just the
first, for the rolling-reconciliation batch), each indexed by `purchase_order_number` +
`website_order_number` the same way the old per-PO match did. A reference found in the
invoices index is treated as CLOSED (invoiced == shipped == done) and its invoice is persisted
into `PurchaseOrderInvoice`; a reference found only in the orders index is OPEN. `get_tracking`/
`get_package_details`/`tracking_date_chunks` remain on `order_client.py` (harmless, unused) but
the standalone `turn_14_order_sweeps.py` service and `sync_turn14_order_sweeps` command have
been deleted — there's no longer a separate hourly job to schedule for this at all, it's just
part of `refresh_confirmed_purchase_orders`'s existing Turn14 branch, on its existing
`_TURN14_STALE_CHECK_INTERVAL` (1h) cadence.

Replaces N per-PO calls with a handful of calls per company per cycle — Dan's "less I/O to get
tracking and invoice per customer", just implemented as one path instead of two.

### Phase 6 — Cadence split (done, deployed 2026-08-25)

Cron lives on the host, invoked via `docker exec aftermarketmonkey-be-app-1 python manage.py
<command>` directly (not `command_runner.sh` -- that file exists but no crontab entry actually
uses it). Turn 14 no longer runs inside `ingest_all_providers` at all -- it's fully unbundled
onto the dedicated commands below.

| Cadence | Command | Endpoints |
|---|---|---|
| every 10 min | `fetch_turn_14_inventory_updates --minutes 15` | `inventory/updates?minutes=15`, then a scoped `MasterPart`/`ProviderPartInventory` propagation pass |
| every 4 h | `fetch_turn_14_items_updates --days 1` | `items/updates?days=1`, then scoped propagation |
| ~1 h (rolling, per-PO due date) | `refresh_confirmed_purchase_orders` (Turn14 branch) | `orders`, `invoices`, both paginated bulk — see Phase 5 |
| daily | `sync_turn14_global_sweep` | `items`, `items/data`, `inventory`, `locations`, `dropship`, `shipping/item_estimation`, then propagation, then per-company `pricing` jobs enqueued for every active Turn 14 connection |
| weekly | `sync_turn14_fitment_sweep` | `items/fitment`, decoded straight into `MasterPartFitment` (no `Turn14ItemFitment` intermediate) |

The old `fetch_turn_14_all_brand_fitment` command and its
`fetch_and_save_turn_14_fitment_for_all_brands` service function (which wrote into
`Turn14ItemFitment`, the table this redesign moved away from) have been removed entirely --
`sync_turn14_fitment_sweep` is the only fitment path now.

**Guard rail:** the 10-minute inventory delta and the daily full sweep must not run
concurrently against the same tables. Use `ScheduledTaskExecution` rows as an advisory lock —
the pattern already exists in `cleanup_stale_started_executions`.

### Phase 7 — Warehouse override (1 day)

`orders/turn_14.py::_build_locations` hardcodes `"default"`. Turn 14's quote API accepts an
explicit location code, and Dan explicitly offers this.

- Add `preferred_location` (`"default"` | `"01"` | `"02"` | `"03"` | `"59"` | `"ds"`).
- Thread through `get_shipping_quote` and `submit_order`.
- Populate the picker from `Turn14Location` (Phase 4) so the UI shows warehouse names.
- Spec constraints: one location block per location; unique `item_id` within a block.

### Phase 8 — Storage cleanup (½ day, optional)

7.67 M pricing rows, ~2.8 M belonging to companies with no active Turn 14 connection.

- Purge pricing for companies with no active Turn 14 `CompanyProviders`.
- Longer term: store a global base pricelist once and only the per-company delta
  (`purchase_cost` / `can_purchase`) — the storage half of Dan's proposal. **Defer** until
  Phases 0–2 show what I/O actually costs once throughput is fixed.

---

## 6. Sequencing

```
Phase 0 (rate limiting) -> measure -> Phase 1 (credentials) -> Phase 2 (flat sweeps)
                                           |
                                           +-> Phase 3 (fitment)      [highest user payoff]
                                           +-> Phase 4 (new endpoints)
                                           +-> Phase 5 (order sweeps)
                                                    |
                                                    +-> Phase 6 (cadence) -> Phase 7 (warehouse)
                                                        Phase 8 (cleanup, anytime)
```

Phases 0 and 1 are small, low-risk, and unblock the measurement that tells us how ambitious
Phase 2 needs to be. Phase 1 also fixes a live bug (new brands never catalogued).

Estimate: **10–12 working days** for Phases 0–7, excluding ops work for the new cron tiers and
the first fitment backfill.

---

## 7. Questions

**For Dan:**
1. Is there a bulk/paginated `v1/dropship` list, or only `GET /v1/dropship/{dropship_id}`?
2. Is `GET /v1/pricing/changes?start_date&end_date` supported and stable? We depend on it, but
   it is not in the technical documentation.
3. Under the new model, does "unlocked by valid credentials and a price record for that SKU"
   mean we may serve cached global data to a customer whose own credentials have not yet pulled
   `v1/pricing` for that SKU, or must the price record exist first?
4. Confirm `items/updates` takes `days` (plural) — your note says `day=1`.
5. When integrator-level credentials are issued: separate client_id for the global endpoints,
   or elevated permissions on ours? And **does the global credential get its own 5 000/h quota**,
   or does it share the customer's?
6. The glossary lists three warehouses (PA, TX, NV) + `ds`. Your enhancement mentions
   01/02/03/**59** — what is 59?

**For us:**
1. Warehouse override per company, or per order account? (`CompanyProviderOrderAccount` already
   exists; a company can have several — own-shop vs. dropship.)
2. Fitment backfill window: first run touches ~793 k items. Nightly window, or one-off
   out-of-hours?
3. Which connection becomes the designated global/house account, given TICK_PERFORMANCE's
   credentials are empty and `primary=True` currently points at a customer?
