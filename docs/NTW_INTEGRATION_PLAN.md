# NTW (TBC Corporation) — Full Integration Plan

**Status:** feed implemented 2026-10-27 (Phases 1-4 + product-type classification). Ordering
deferred by decision -- see §9, which stays a plan. Last verified live 2026-10-27.

**What landed:** `BrandProviderKind.NTW = 41`, the `PROVIDER_CATALOG` entry with onboarding
instructions, four models + `migrations/0233_ntw_models.py`, `clients/ntw/` (SFTP, host-key
pinned), `services/ntw.py` (parse, brand mapping, catalog ingest, zero-out, per-company pricing),
five `master_parts` functions wired into all three `sync_all` orchestrators, the feed connection
validator, `_PRICING_SYNC_KINDS` + `_fetch_raw_pricing`, both cron entrypoints, and
`tests/test_ntw_feed.py` against a 19-row fixture of real rows.

NTW is the wholesale arm of **TBC Corporation**. Everything is TBC-branded underneath: the
feed is a TBC MOVEit server, the APIs are a TBC IBM API Connect gateway. There is no "NTW"
API. Three separate surfaces, each with its own auth, and they are *not* versioned together.

| Surface | Endpoint | Auth | Verified |
|---|---|---|---|
| File feed | `moveit.tbccorp.com` (SFTP :22, FTPS :21, REST `/api/v1`) | per-dealer user/password | yes — 119 files pulled from a live dealer account |
| Inventory API | `POST /tbccorp/qa/tbc/api/v2/products/tires` | `X-IBM-Client-Id` + `X-IBM-Client-Secret` | yes — HTTP 200, live stock and cost |
| Order API | `POST /tbccorp/qa/tbc/api/v1/order`, `GET /v1/order/{orderId}` | same two headers **+ HMAC SHA-256 `signature`** | no — not yet exercised |

Note the version split: inventory is `v2`, ordering is `v1`. Treat them as two clients.

## 1. The shape of the integration

**The file feed is the system of record for catalog, cost and quantity.** It is a per-dealer
CSV dropped 4x daily. This is what fills `master_parts`, `provider_parts`,
`provider_part_inventory` and `ntw_company_pricing`.

**The v2 inventory API is on-demand only** — a part-detail refresh and the pre-order
availability check. It is not a bulk source: it answers by tire size, two sizes per call,
~0.9s each. Sweeping a catalog through it would take thousands of calls to reproduce what one
500 KB file already contains.

**The v1 order API is the ordering channel.** It has no quote endpoint, so the quote must be
synthesized from the v2 inventory call (see Phase 7).

This is exactly the division of labour we already run for Keystone (FTP feed + SOAP
CheckInventory + SOAP order) and TireRack (SFTP feed, no API at all). NTW is the TireRack
ingest shape with Keystone's API layer bolted on.

### Verified feed facts

```
/Trident Motorsports/Outbound/DELTI_0037746990_<YYYYMMDDHHMMSS>.csv
```

- **119 files, exactly 4.0/day**, generated at 07:xx, 11:xx, 16:xx, 23:xx.
- **494,850–510,207 bytes** across 30 days — a ±1.5% spread, the signature of a snapshot
  rather than a delta.
- **CONFIRMED: it lists in-stock items only.** Not one of the 3,533 rows in the file we profiled
  carried a zero quantity. So it is a complete snapshot *of what is in stock*, and a tire that
  sells out stops appearing rather than arriving as a zero. An upsert alone would leave it
  showing its last known quantity forever — the exact failure we warned NTW about in August.
  `ntw.zero_out_absent_ntw_parts` is the reconciliation pass, and it is safe precisely because
  each file is complete for that account. **This is now implemented**, and it is the one place
  NTW's ingest deliberately differs from every other provider's.
- **30-day rolling retention**, so a gap longer than a month is unrecoverable.
- One file type only — **no separate pricing file**, so cost and quantity arrive together.
- `0037746990` is the TBC **feed** account number, zero-padded to 10.
  **Correction (tested 2026-10-27): it is NOT the API `customerId`.** Every variant —
  `37746990`, `0037746990`, and shorter forms — is rejected by the v2 API with
  `Customer id - 0037746990 is invalid`, while TBC's demo `10062387` returns data. The feed
  account number and the API customer id are different identifiers, so each dealer's API id has
  to come from TBC; it cannot be derived from the filename. This was an inference in the first
  draft of this plan and it was wrong.
- Root listing is denied (`errorCode 2020`) and each dealer sees only its own subtree, so
  per-dealer credentials map 1:1 onto per-dealer feeds with no routing logic on our side.
- `/Incoming` exists and is empty — a dealer→TBC upload path. Possibly an order channel.

## 2. Phase 0 — resolve before writing ingest code

These are cheap to answer and expensive to guess wrong.

1. **Profile a real file.** Pull the newest `DELTI_*.csv` and map headers, row count, and which
   columns carry cost vs. retail vs. quantity vs. warehouse. Every mapping decision below is
   provisional until this is done. *(Needs Gojko's go-ahead to download.)*
2. **Full snapshot or delta?** (§10 Q1.) Snapshot → plain upsert, matching every other
   provider. Delta → we need the zero-out reconciliation we do not have anywhere in the
   codebase today, and which we already told NTW we would require.
3. **Is cost in the file per-dealer?** The file lives in the dealer's own folder, so presumably
   yes. If so `ntw_company_pricing` is sourced per company and there is no shared "primary
   connection" catalog pull. Confirm, because it decides the model layout in Phase 2.
4. **The HMAC signature spec** for the order API (§10 Q4). Blocks Phase 7 entirely.
5. **`dealerCode` vs `customerId` vs `storeNumber`** semantics, and which of them each dealer
   gets. Blocks both API phases.

## 3. Phase 1 — provider registration and onboarding

- `src/enums.py`: `BrandProviderKind.NTW = 41` (40 is `THOROUGHBRED_DIESEL`; 41 is free).
- `src/constants.py`: credential key constants next to `TIRERACK_CREDENTIALS_SFTP_*`
  (`src/constants.py:288`). Host is constant, so default `moveit.tbccorp.com:22` in code and
  ask the dealer for **username and password only** — two fields, not four, and no typo'd
  hostnames.
- `src/constants.py` `PROVIDER_CATALOG`: an `NTW` entry modelled on the TireRack one
  (`src/constants.py:2272`), with `connection_required_fields` and
  `installation_instructions_html` carrying the ready-to-send email the dealer forwards to
  their TBC rep. This is what makes onboarding self-serve.
- `src/constants.py` icon URL + a logo upload, as every other provider has.
- Seed the `providers` row (name "NTW", kind 41, `coming_soon` until Phase 4 lands).

## 4. Phase 2 — models and migration

Clone the TireRack quartet (`src/models.py:2472`–`2547`), which is the per-company-feed shape:

| Model | Table | Purpose |
|---|---|---|
| `NtwBrand` | `ntw_brands` | distinct manufacturer label from the feed; `name` is the natural key |
| `BrandNtwBrandMapping` | `brand_ntw_brand_mapping` | our `Brands` ↔ `NtwBrand` |
| `NtwParts` | `ntw_parts` | raw feed row, `unique_together (part_number, brand)`, full row in `raw_data` JSON |
| `NtwCompanyPricing` | `ntw_company_pricing` | per-company cost, `unique_together (part, company)` |

Keep `raw_data` populated — it is what let us re-parse Motor State and Tri-State without
re-fetching, and the column layout here is unconfirmed.

One migration, `migrations/0232_ntw.py` or next free number.

## 5. Phase 3 — feed transport

**Preferred: TBC pushes to our relay.** `relay_sftp_provisioning` already issues each company
its own relay credentials; Meyer, A-Tech, Crown, DLG and Tri-State all arrive this way. We
then hold zero TBC credentials, rotate nothing, and need no egress allowlisting. TBC runs
MOVEit Automation, so outbound delivery is a configuration on their side, not a build.

**Fallback: we pull from their MOVEit over SFTP.** `src/integrations/clients/ntw/sftp_client.py`,
a near-copy of `src/integrations/clients/tirerack/client.py:27` reading
`sftp_host`/`sftp_port`/`sftp_user`/`sftp_password` from `credentials["feed"]`
(`src/integrations/credentials.py:18`). Two deviations from the TireRack original:

- **Pin the host keys.** TireRack's client does a bare `Transport.connect` with no host-key
  verification. MOVEit's are
  `RSA SHA256:40J0JewCgGCupJ85pxc/dOsu0i2EBybJu6/jQRcWg7I` and
  `ED25519 SHA256:bgfnFVvKsZhMyANNZ0qWG19SX4QRzbKjYL1p0X1QYOM`.
- **Watermark on filename, not content.** The REST API exposes no hash (`folderID, id, isNew,
  name, path, size, uploadStamp, uploadUsername` only), and the embedded timestamp is unique
  and monotonic. Store the last-ingested stamp per connection and skip anything not newer.

A REST client (`/api/v1/token` → `/folders/{id}/files` → download by file id) is a documented
third option if port 22 egress from prod turns out to be blocked. Token life is 1199s with a
refresh token, so a long ingest must refresh mid-run.

## 6. Phase 4 — staging to master parts, inventory and pricing

Mirror the TireRack function set exactly; a reviewer should be able to diff them.

In `src/integrations/services/ntw.py`:

- `sync_unmapped_ntw_brands_to_brands()` — feed brand labels → `NtwBrand` → auto-map to our
  `Brands` via `src/integrations/utils/brand_matching.py`, leaving ambiguous ones unmapped for
  review rather than guessing (precedent: `tirerack.py:150`).
- `fetch_and_save_ntw_catalog()` — transport → `NtwParts`, verbatim, no transformation
  (`tirerack.py:363`).
- `sync_ntw_company_pricing_for_company_provider(company_provider_id)` — that dealer's file →
  `NtwCompanyPricing` (`tirerack.py:570`).

In `src/integrations/services/master_parts.py` (follow `11357`–`11925`):

- `_ntw_provider_external_id(ntw_brand_id, part_number)` — the `provider_external_id` that
  becomes the permanent join key. **This is the field we promised NTW we would match on
  exactly in the recurring path**, so it must be derived from their item id, not from our
  normalized part number.
- `sync_master_parts_from_ntw()` — `NtwParts` → `MasterPart` + `ProviderPart` through
  `master_part_matching` (exact `(brand, part_number)` → guarded normalization → new record).
- `sync_provider_inventory_from_ntw()` → `ProviderPartInventory`, including
  `warehouse_availability` keyed by the feed's location/warehouse column.
- `sync_provider_pricing_from_ntw_for_company(company_id)` and `sync_provider_pricing_from_ntw()`
  → `ProviderPartCompanyPricing`.
- `sync_derived_from_ntw(*, reindex_meilisearch=False, ...)` — the orchestrator the cron calls.

Also: `product_type` must classify to `TIRE` (`src/integrations/utils/product_type.py`); these
are tires, and the wheel path should stay untouched.

## 7. Phase 5 — tire specs

`TireSpec` (`src/models.py:6376`) has a strict precedence: **distributor structured field →
parser → LLM**, and the LLM is forbidden from the size block. NTW arrives as a *distributor
structured* source, i.e. top tier, for every field it carries.

From the v2 API we know TBC publishes `speedRating`, `loadRange`, `loadIndexRange`,
`treadPattern`, `sideWall`, `runFlat`, `lightTruck` and a structured
`tireSize {width, ratio, diameter}`. If the file carries the same columns — Phase 0 confirms —
they feed `TireSpec` directly and we skip enrichment for those fields. Size still goes through
`src.domain.tire_size`, never taken on trust, same rule as every other source.

`lightTruck == "Y"` maps to `VEHICLE_CLASS_LIGHT_TRUCK`; do **not** infer anything from a
blank. Nullable means unknown, never false — `is_3pmsf` especially.

No `WheelSpec` work. If TBC later sends wheels, that is a separate phase.

## 8. Phase 6 — on-demand live inventory

`src/integrations/live_inventory/ntw.py`, registered in that package's `registry.py`.
Interface is one method: `refresh(provider_part) -> ProviderPartInventory`
(`src/integrations/live_inventory/base.py:26`).

Implementation calls `POST /v2/products/tires`. Four verified constraints, each of which will
silently corrupt results if ignored:

1. **`criteria[]` honors only the first two entries.** Five sizes returned two, HTTP 200, no
   warning; reordering proved it takes the first two. Cap requests at two and loop.
2. **`requestedQty` is an availability filter, not a quantity.** Same size returned 15 rows at
   qty 1, 13 at qty 2, 5 at qty 10, 1 at qty 100. Always send **1** for a stock read.
3. **Errors arrive as HTTP 200** with `{errorCd, errorDesc, errorMsg}` and no `product` key.
   Branch on the key, never the status code.
4. **`searchType: "ByPartNumber"` does not work** — errorCd 1006 for every identifier shape
   including TBC's own documented sample. The feed suggested one more candidate and it also
   failed: two Cooper rows carry a TBC-internal item number in `product_id`
   (`COOP-90000002527`) that differs from the part number (`160018024`), and neither that id nor
   its prefixed form returns anything. So a single-SKU refresh has to go **by that SKU's tire
   size** and then filter the response down to the part we wanted. Revisit if TBC fixes it.

Credentials: the IBM client id/secret are **account-level, not per-dealer**, while `customerId`
is per-dealer. So the pair lives in settings/env and the `customerId` lives in the connection's
`credentials["feed"]`. That split is new — every existing live-inventory provider reads one
namespace — and is worth a comment in the class.

## 9. Phase 7 — ordering adapter

`src/integrations/orders/ntw.py` implementing `DistributorOrderAdapter`
(`src/integrations/orders/base.py:281`), registered in `orders/registry.py`.

**`get_shipping_quote` — synthesized, not fetched.** There is no quote endpoint. Build the
quote from a v2 inventory call: `availableSource[].locationId` and `quantityAvailable` give
per-warehouse availability, `productCost` the line cost, `freightPerTire` the freight,
`estimatedDelivery` / `estimatedDeliveryDate` the service level. This is precisely the
pre-submit ATP check we told NTW we perform, and it inherits the existing 15-minute quote TTL
(`PURCHASE_ORDER_QUOTE_TTL_MINUTES`, `conf/settings_base.py:300`) with submit hard-blocked
against a stale quote.

**`submit_order` → `POST /v1/order`.** Body carries `dealerCode`, `customerId`, `poNumber`,
`isDropShip`, `address`, `type` (`PickUp` | ship), `source`, `notes[]`, `items[]`
(`brandName`, `partNo`, `locationId`, `qty`, `carrierServiceCode`, `SCAC`) and `placed`.
Two consequences for us:

- `locationId` is **required per line**, and it comes from the quote's `availableSource`. The
  quote is therefore not advisory — it is where the warehouse decision is made. A line whose
  quote has expired cannot be re-submitted without re-quoting, which the existing TTL already
  enforces.
- The response is an **array** of `{id, items}` — one submit can return several TBC order
  numbers (`SAP_1234567890`). We already model multiple distributor order numbers per PO, so
  map each array element to its own record rather than taking `[0]`.

**`get_order_status` → `GET /v1/order/{orderId}`**, returning `status`, `estDelivery`,
`location`, `pickUpPhone`, `trackingNumber` → `OrderStatusResult`.

**Signing.** Both order calls take `timestamp` and a `signature` (HMAC SHA-256) query
parameter. We do not know what string is signed or with which key — that is §10 Q4 and it
blocks this phase. Build the signer as one small isolated function so the canonicalization can
change without touching the adapter.

**Capabilities:** `supports_cancel()` → `False` (no cancel endpoint exists);
`supports_shipping_method_selection()` → `True` (`carrierServiceCode`/`SCAC` per line);
`supports_invoices()` → `False` until TBC confirms an invoice endpoint;
`fulfillment_channel` → `api`.

## 10. Phase 8 — wiring

- **Feed validator** in `_CONNECTION_VALIDATORS` (`src/api/services/integrations.py:749`) —
  connect, confirm the dealer's folder is readable, confirm at least one `DELTI_*.csv` exists.
  Keep it fast; `check_company_provider_connections` runs it on a cron.
- **Order validator** in `_ORDER_CONNECTION_VALIDATORS` (same file, `:901`) — a `customerId`
  echo check via a cheap v2 call, since a bad id fails loudly there (`400 Customer id ... is
  invalid`).
- **Pricing jobs:** add kind 41 to `_PRICING_SYNC_KINDS`
  (`src/integrations/services/integration_pricing_sync_jobs.py:56`) and a dispatch branch in
  `_fetch_raw_pricing` (`:256`) calling
  `ntw_services.sync_ntw_company_pricing_for_company_provider(cp.id)`.
- **Crons:** `sync_derived_from_ntw` into the nightly `ingest_all_providers`;
  `sync_provider_inventory_from_ntw()` into `sync_provider_inventory`. Their files land at
  07/11/16/23, so an inventory pass shortly after each is the natural cadence — four a day,
  decoupled from the nightly catalog run, which is what we told them we could do.
- **Tests:** a fixture of one real `DELTI` file (trimmed) under
  `src/integrations/tests/fixtures/ntw/`, parser tests, and an adapter test with a recorded
  order response — following `test_rough_country_edi_orders.py`.

## 11. Risks

1. **The in-stock-only problem is real, and now handled for NTW only.**
   `zero_out_absent_ntw_parts` closes it here. **Every other provider still has the gap**: the
   shared apply path is upsert-only with no reconciliation anywhere in `master_parts.py`, so a
   dropped item keeps its last known quantity. NTW's pass is a working model for fixing the
   rest, and that generalization is still unbuilt.
2. **`ByPartNumber` being broken forces size-shaped lookups.** A single-SKU refresh costs a
   full size query. Acceptable for part-detail refreshes, bad if anything ever loops it.
3. **30-day feed retention.** A month-long outage is permanent data loss. Alert on a missing
   drop rather than on a stale catalog.
4. **Dealer cost confidentiality.** If TBC ever proposes one AMS account holding every
   dealer's file (rather than per-dealer accounts), that puts every dealer's cost in one
   folder. Push back; per-dealer accounts are the MOVEit norm and we have one working already.
5. **Order API is unexercised.** Everything in Phase 7 is read off their docs plus one sample
   payload. No call has been made. Expect the signature and `dealerCode` semantics to move.
6. **QA-only base URL.** Their doc lists the same `tbcservicesqa.tbccorp.com` host for
   "Production, Development". We have no production URL for either API.
7. **TLS chain.** `tbcservicesqa.tbccorp.com` serves only its leaf certificate, omitting the
   GoDaddy G2 intermediate, so `requests` fails outright. Bundle the intermediate as the CA
   file for both API clients. Their MOVEit host is fine.

## 12. Open questions for TBC

Feed — **Andrew Pugliese, apugliese@tbccorp.com, 561-383-3100 x4169**:

1. Is `DELTI` a **full snapshot or a delta**? If delta, are zero-quantity and discontinue
   records sent explicitly when an item goes to zero or leaves the catalog?
2. Can MOVEit Automation **push** each dealer's file to our SFTP relay instead of us pulling?
3. Is the cost in the file the **dealer's own** cost, and does it include FET and freight?
4. What is `/Incoming` for — can we deliver orders or POs there?
5. Column dictionary for the file, and is the layout stable / versioned?
6. Per-account **IP allowlisting** — does `5.161.121.143` need adding? (Their token error
   mentions "not allowed to sign on from this location".)

APIs — **API_Support@TBCCORP.com**:

1. The **HMAC SHA-256 signature**: what string is canonicalized, which key signs it, where does
   the key come from, and is `timestamp` epoch seconds? *(Blocks ordering.)*
2. Each dealer's **`customerId`, `storeNumber`, `dealerCode`** — and is `37746990` Trident's,
   as the feed filename implies?
3. Is **`ByPartNumber`** implemented? It fails for every identifier shape including their own
   documented sample.
4. Is **`productCost`** the dealer's net cost? The documented `catalog.dealerCost` /
   `dealerCostDirect` fields never appear in responses.
5. Is the 2-entry **`criteria` cap** intentional? Silent truncation is a bug either way.
6. **Production base URLs** for both APIs.
7. Is there an **invoice** endpoint, and a **cancel** endpoint?
8. Is **drop-ship** enabled per dealer? `isDropShip: true` returned no inventory for this one.

## 13. Sequencing

Phase 0 gates everything and is a day's work once the file is in hand. Phases 1–4 are the
TireRack pattern with new names and carry no unknowns beyond the column layout — that is the
bulk of the value, since it is what puts NTW tires in the catalog with cost and stock. Phase 5
is small if the file carries the spec columns. Phase 6 is self-contained and useful on its own.
Phase 7 is blocked on the signature spec and should not be started until Q1 comes back.

Build order: **0 → 1 → 2 → 3 → 4 → 5 → 6 → 7**, with 8's wiring folded into each phase as it
lands rather than left to the end.
