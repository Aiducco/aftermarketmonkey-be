# Instantly → FreshSales reply sync — implementation plan

Status: **live and working end to end.** As of 2026-10-04 the full backfill has run against the
real FreshSales account: **13 contacts, 6 deals, 20 notes, 0 failures**, and a re-run creates
nothing. 62 tests pass, migration `0205` is applied. The only thing left is installing the cron
line (§9) and deciding the deal amount (§11 Q2).

Both vendors verified live, writes included. The question of whether that FreshSales key could
write — open through the whole design phase because `GET /selector/owners` answers 403 — is
answered: **it can.** The 403 is specific to the owners selector, not a general permission gap.

What is on disk:

| File | What it is |
|---|---|
| `src/models.py` → `InstantlyReply` | The local row; migration `migrations/0205_instantly_reply.py`, applied |
| `src/integrations/clients/instantly/` | `client.py`, `exceptions.py` — emails / leads / campaigns |
| `src/integrations/clients/freshsales/` | `client.py`, `exceptions.py` — accounts / contacts / notes / deals |
| `src/integrations/services/instantly_freshsales_sync.py` | The three passes, plus the dry-run preview |
| `src/management/commands/sync_instantly_replies.py` | The command, with audit wiring |
| `src/integrations/tests/test_instantly_freshsales_sync.py` | 62 tests, no DB and no network |
| `conf/settings_base.py`, `.env.app.example` | Settings and the env template |

Goal: every reply that lands in Instantly becomes a FreshSales contact, and a reply Instantly has
labelled positive also becomes a deal. Driven by a management command on a cron, so the loop runs
unattended like the rest of this repo's integrations.

Everything in §2 marked **verified** was read off the live workspace, not the vendor docs. That
distinction matters here: the docs were wrong or incomplete on four points that change the
design (§2.2).

## 1. What this builds on

Before this work nothing talked to either vendor. The other Instantly-shaped code is
[export_instantly_list.py](scripts/export_instantly_list.py), which builds the **outbound** CSV —
one row per business, de-duplicated by website domain, best verified address per shop. This plan
is the other half of the same loop: that script decides who gets mailed, this one handles what
happens when they answer.

That turns out to matter more than symmetry, and in a way that simplifies the build considerably.
The export uploads City / State / Zip / Tier / Typology / Locations / companyName / website /
phoneNumber as Instantly custom variables, and **Instantly hands them all back on the lead record**
(§2.1, verified). So the CRM contact can be built rich from Instantly alone — no matching back to
`Lead` / `RealTruckLead` is needed for the core flow.

Existing machinery this reuses rather than reinvents:

| Piece | Why it is used here |
|---|---|
| `src/audit/scheduled_tasks.py` | `ScheduledTaskExecution` + `cleanup_stale_started_executions` — how every cron in this repo reports itself |
| `src/integrations/clients/<vendor>/{client,exceptions}.py` | The client layout. [geoapify/client.py](src/integrations/clients/geoapify/client.py) is the closest model: thin transport, module-level `requests.Session`, typed exceptions, no domain logic |
| `src/integrations/services/<name>.py` | Orchestration, called by a thin `BaseCommand` — see [sync_meyer.py](src/management/commands/sync_meyer.py) |

## 2. The vendor APIs

### 2.1 Instantly V2 — verified

Base `https://api.instantly.ai/api/v2`, header `Authorization: Bearer <key>`. The supplied key
works and already carries the scopes we need (emails, leads, campaigns all read OK).

**`GET /emails`** — the Unibox feed, the endpoint we poll. Parameters used:
`email_type=received`, `sort_order=asc`, `min_timestamp_created=<watermark>`, `limit=100`,
`starting_after=<cursor>`. Rate limit **20 requests/minute on this endpoint specifically**.

Fields actually returned on a received email (verified — the API omits null fields entirely, so the
docs' full field list is not what you get):

```
id  timestamp_created  timestamp_email  message_id  thread_id  campaign_id  organization_id
from_address_email  from_address_json  to_address_email_list  to_address_json
subject  body{text,html}  content_preview  eaccount  lead  step
i_status  ai_interest_value  is_unread  is_focused  ue_type
```

**`POST /leads/list`** with `{"limit": 1, "search": "<email>"}` — the interest re-check (§6.3).
Returns the lead with `lt_interest_status`, `company_name`, `website`, `job_title`,
`timestamp_last_interest_change`, and **`payload`** — the custom variables we uploaded:

```
companyName  website  phoneNumber  City  State  Zip  Tier  Typology  Locations
"RealTruck Preferred"  firstName  lastName  jobTitle  email
```

**Interest status** (`i_status` on an email, `lt_interest_status` on a lead — same scale):

| Value | Meaning | Treated as |
|---|---|---|
| `1` | Interested | **positive → deal** |
| `2` | Meeting Booked | **positive → deal** |
| `3` | Meeting Completed | **positive → deal** |
| `4` | Closed / Won | **positive → deal** |
| `0` | Out of Office | contact only, and treated as the auto-reply flag (§2.2) |
| `null` | not labelled yet | contact only, re-check later |
| `-1` | Not Interested | contact only |
| `-2` | Wrong Person | contact only |
| `-3` | Lost | contact only |
| `-4` | No Show | contact only |

### 2.2 Four places the Instantly docs are wrong — each one changes the design

1. **`is_auto_reply` is never returned.** The documented auto-reply flag is absent from every row
   in the live account, so the plan cannot filter on it. Verified substitute: Instantly labels
   out-of-office replies `i_status = 0`, and those rows also carry an `"Out of office Re: …"`
   subject. So **`i_status == 0` is the auto-reply signal**, with a subject-prefix check as backup.
2. **`lt_interest_status` as a filter on `POST /leads/list` is silently ignored.** Asking for
   `lt_interest_status: 1` returned 100 leads of which 95 were `null`, 1 was `-1` and 3 were `1`.
   Filtering server-side by interest is therefore not available; the re-check reads **one lead at a
   time by `search: <email>`**, which is exact and was correct for all 14 addresses tested.
3. **`next_starting_after` is returned even on the last page.** Following it yields a page of zero
   items. The pagination loop must stop on **empty items**, not on a present cursor — stopping on
   the cursor alone would loop forever.
4. **`lead_id` is not returned on an email, only `lead`** (the address). So the local row keys on
   the address, and the lead lookup is by `search`, not by id.

A fifth, non-breaking one: plain `urllib` with its default User-Agent gets a **403** from their
edge; `requests` (and curl) are fine. Worth knowing before someone debugs a phantom auth problem.

### 2.3 What the live account actually looks like (2026-10-03)

| Measure | Value |
|---|---|
| Received replies, all time | **22**, spanning 2026-09-04 → 2026-10-02 |
| Distinct replying addresses | **14** (so people reply more than once per thread) |
| Campaigns | 2 — `Google leads - 1/2 locations`, `Realtruck - 1/3 locations` |
| `i_status = 1` Interested | **12 emails, from 6 distinct addresses** |
| `i_status = -1` Not Interested | 4 |
| `i_status = 0` Out of Office | 2 (both from one address, `mitch@dasmule.com`) |
| `i_status = null` unlabelled | 4 |
| `email.i_status` vs `lead.lt_interest_status` | **agreed on all 14 addresses, zero disagreements** |

So the backfill produces **13 contacts and 6 deals**: 14 distinct addresses less the one that only
ever sent out-of-office replies, and one deal per positive address rather than per positive reply.
Confirmed by `--dry-run --since 2026-09-01` against the live account.

Three conclusions that shape the build:

- **Instantly's AI labelling is real and it works.** 18 of 22 labelled, and `ai_interest_value`
  tracks `i_status` almost exactly. Relying on their tag was the right call — this is the
  question §11 of the previous draft flagged as the main risk, and it is now answered.
- **Volume is tiny.** ~22 replies/month. Rate limits, batching and concurrency are all non-issues;
  the whole sync is a handful of HTTP calls per run. This is the single biggest simplifier in the
  plan and the reason several defaults below are set generously rather than carefully.
- **~18% never get labelled.** The 4 unlabelled replies include two from Sept 4 and Sept 8 — a
  month old and still blank, so they are not just "AI hasn't caught up". They become contacts with
  no deal unless someone labels them in Unibox. §6.3's re-check window is therefore set to **60
  days**, not 7: at this volume re-checking is nearly free, and a label applied by hand weeks later
  should still produce the deal.

### 2.4 FreshSales (Freshworks CRM) — reads verified, writes not yet

Base `https://aftermarketscout.myfreshworks.com/crm/sales/api`, header
`Authorization: Token token=<key>`. Rate limit 1000 requests/hour per account. Auth confirmed
working; the CRM is **empty today** (a lookup for `andrew@rhinoutah.com` returns no contact), so
the backfill writes into a clean account.

Live configuration, read 2026-10-03:

| Thing | Value |
|---|---|
| Deal pipeline | `Default Pipeline` — id `127000525268`, `is_default: true`, the only one |
| First stage | `New` — id `127003680894`, position 1, probability 20, forecast Open |
| Other stages | Qualification 30 · Discovery 40 · Demo 60 · Negotiation 80 · Won 100 · Lost 0 |
| Contact statuses | `New` `Contacted` `Interested` `Unqualified` (lifecycle *Lead*), `Qualified` `Lost` (*Sales Qualified Lead*), `Won` `Churned` (*Customer*) |
| Pipeline's highlighted fields | `amount`, `sales_account_id`, `expected_close` |

Two findings worth acting on:

- **`GET /selector/owners` returns 403 "You are not authorised to perform this operation."** The
  key's permissions are narrower than the docs assume. It costs us nothing directly — the decision
  was already "no owner" — but it is the reason write permission is listed as an open risk in §11
  Q1 rather than assumed. Reads on pipelines, stages, statuses and lookup all work.
- **Instantly's labels map cleanly onto FreshSales contact statuses**, which is free CRM signal we
  would otherwise throw away. See §6.2.

| Endpoint | Body | Use |
|---|---|---|
| `POST /sales_accounts/upsert` | `{"unique_identifier": {"name": …}, "sales_account": {…}}` | The shop as a company |
| `POST /contacts/upsert` | `{"unique_identifier": {"emails": …}, "contact": {…}}` | The person who replied |
| `POST /deals` | `{"deal": {"name", "amount", "sales_account_id", …}}` | Positive replies only |
| `POST /notes` | `{"note": {"description", "targetable_type": "Contact", "targetable_id"}}` | The reply text |

Upsert returns **201 on create, 200 on update** — worth logging, since a 200 on a contact we
thought was new means that shop was already in the CRM.

> **`POST /deals` requires `name`, `amount` *and* `sales_account_id`.** "Default pipeline, first
> stage, no owner" holds for pipeline/stage/owner — those are genuinely optional and FreshSales
> fills them in — but a deal cannot exist without an account and an amount. Hence the sales-account
> upsert in §6.2 and `FRESHSALES_DEFAULT_DEAL_AMOUNT` (default `0`). Vendor requirement, not a
> design choice. §11 Q2.

**All four write endpoints are now verified against the live account** (§11 Q1), including the
shapes the docs left ambiguous: a contact's `emails` array is `[{"value": …, "is_primary": true}]`,
the account association is `sales_accounts: [{"id": <int>, "is_primary": true}]` with the id as an
integer, and a note takes `targetable_type: "Contact"`.

## 3. Decisions taken

| Decision | Choice |
|---|---|
| What makes a reply positive | **Instantly's own label** — `i_status` / `lt_interest_status` in `{1,2,3,4}`. No LLM classification; the repo's Claude/Qwen integrations are untouched. Verified sound in §2.3 |
| Which replies become contacts | **Every human reply**, so a "not interested" is still a complete CRM record. Auto-replies (`i_status == 0`) are stored but not pushed |
| Deal shape | FreshSales defaults for pipeline, stage and owner; account + amount supplied because they are required |
| Contact name | `from_address_json[0].name` — what the person actually typed in their mail client (§6.4) |
| Contact status | Mapped from Instantly's label — Interested / Unqualified / Contacted (§6.2) |
| Trigger | Cron-driven poll, not a webhook (§4) |

## 4. Why polling, not a webhook

Instantly has webhooks (`reply_received`, `lead_interested`, and the rest) and they would be
lower-latency. Polling still wins:

- **No public endpoint to stand up**, and nothing to verify a caller with — Instantly's webhook
  docs document no signing secret and no signature header at all.
- **Replayable.** A dropped webhook is gone; a poll that fails runs again in 15 minutes and picks
  up everything since the watermark. That is the property that makes every other cron here
  recoverable.
- **22 replies a month.** A webhook's latency advantage buys nothing at this volume.

If that changes, `reply_received` can be added later as a fast path writing the same
`InstantlyReply` rows, with the poller demoted to a backstop. Nothing here forecloses it.

## 5. Data model — one new table

`InstantlyReply` in `src/models.py` (`db_table = "instantly_reply"`), migration `0205`.

The local row is the whole idempotency story: what Instantly said, what we pushed, what came back.
A re-run never double-creates, and a failure is visible in SQL rather than only in logs.

```
instantly_email_id      TextField  unique          # Instantly's UUID — the idempotency key
thread_id               TextField  null
campaign_id             TextField  null, indexed
campaign_name           TextField  null            # resolved once per run from /campaigns
lead_email              EmailField       indexed   # the `lead` field — who replied
from_name               TextField  null            # from_address_json[0].name
eaccount                TextField  null            # which of our mailboxes they answered
subject                 TextField  null
body_text               TextField  null            # body.text, for the FreshSales note
email_timestamp         DateTime         indexed   # timestamp_email
instantly_created_at    DateTime         indexed   # timestamp_created — what the watermark reads

interest_status         Integer    null            # i_status, refreshed by §6.3
interest_checked_at     DateTime   null
is_positive             Boolean    default False   # interest_status in {1,2,3,4}
is_auto_reply           Boolean    default False   # derived: i_status == 0 or OOO subject (§2.2)
lead_payload            JSONField  default dict    # Instantly's custom variables (§2.1)

freshsales_account_id   TextField  null
freshsales_contact_id   TextField  null
freshsales_note_id      TextField  null
freshsales_deal_id      TextField  null
contact_synced_at       DateTime   null
deal_created_at         DateTime   null

sync_attempts           Integer    default 0
last_error              TextField  null
created_at / updated_at
```

`lead_payload` is stored rather than re-fetched so the FreshSales push never depends on a second
live call, and so we can see what the CRM record was built from after the fact.

**No separate watermark table.** The watermark is
`max(instantly_created_at) − INSTANTLY_SYNC_OVERLAP_MINUTES`, derived from this table each run. One
less thing to drift out of step with reality, and it self-heals: a half-committed run is re-read on
the next tick and the unique constraint absorbs the duplicates.

**No FK to `Lead` / `RealTruckLead`.** The previous draft had both, to carry shop context into the
CRM. `lead_payload` already carries City / State / Zip / Tier / Typology / Locations / company /
website / phone, because we uploaded it. Matching to our own tables would add only brand lists and
the AI confidence score, at the cost of the two-FK split the `LeadEmail` / `RealTruckLeadEmail` pair
already lives with. Left out; §12 keeps it as an optional follow-up.

## 6. The sync — `manage.py sync_instantly_replies`

Thin `BaseCommand` → `src/integrations/services/instantly_freshsales_sync.py`. Audit name
`sync_instantly_replies`, with `cleanup_stale_started_executions` first so an OOM-killed run
self-heals. Three passes, each independently re-runnable.

### 6.1 Pass 1 — ingest replies

`GET /emails` from the watermark, oldest first, paginating until a page comes back **empty**
(§2.2 #3). Each email is `get_or_create`d on `instantly_email_id`; `is_auto_reply` is derived on
write. Nothing is pushed anywhere in this pass, so a FreshSales outage cannot lose a reply.

First run has no watermark. `--since YYYY-MM-DD` seeds it; without one the command uses
`INSTANTLY_SYNC_INITIAL_DAYS` (7) rather than silently pulling all history. For the initial
backfill of the existing 22 replies, run `--since 2026-09-01` once.

Campaign names are fetched once per run from `GET /campaigns` and cached in memory — two campaigns
today, so it is one call.

### 6.2 Pass 2 — contacts

For every row with `contact_synced_at IS NULL` and `is_auto_reply = False`:

1. `POST /sales_accounts/upsert` keyed on `payload.companyName` (falling back to the reply domain)
   → `freshsales_account_id`
2. `POST /contacts/upsert` keyed on `emails` → `freshsales_contact_id`, carrying the name from
   §6.4 and address / phone / website / job title from `lead_payload`, associated to the account
3. `POST /notes` with the reply body, `targetable_type: "Contact"` → `freshsales_note_id`

Each id is saved the moment it returns, so a crash between steps resumes instead of repeating. A
row that fails increments `sync_attempts`, records `last_error`, and the run moves to the next
reply — one malformed reply must never stall the queue.

The note carries subject, received timestamp, campaign name, Instantly's label, and the reply text.
Since 22 replies came from 14 addresses, a contact legitimately accumulates several notes; that is
the thread history and is wanted.

**Contact status carries Instantly's verdict.** Your CRM already has statuses that line up with
Instantly's labels, so the contact says what happened without anyone opening the note:

| Instantly `i_status` | FreshSales contact status | id |
|---|---|---|
| `1` `2` `3` `4` positive | `Interested` | `127004315050` |
| `-1` `-2` `-3` `-4` negative | `Unqualified` | `127004315051` |
| `0` out of office, or `null` unlabelled | `Contacted` | `127004315049` |

All three sit in the `Lead` lifecycle stage (`128084251084`), so nothing here promotes a contact
past where it belongs. Status is re-applied whenever §6.3 changes the label, so a relabel in
Unibox is reflected in the CRM on the next run. The ids are resolved **by name** at runtime from
`GET /selector/contact_statuses` rather than hardcoded — the numbers above are this account's
today, and a renamed status should fail loudly rather than write a stale id.

### 6.3 Pass 3 — refresh the label, then deals

**The pass that earns its keep.** Instantly's label is not always set when the reply arrives — the
AI applies it shortly after, and a human relabelling in Unibox can change it much later. In this
account 4 of 22 replies are still unlabelled, two of them a month old (§2.3). A one-shot read at
ingest time would permanently miss any of those that later turn positive.

So: for every row that is not yet positive and whose `email_timestamp` is within
`INSTANTLY_INTEREST_RECHECK_DAYS` (**60** — see §2.3), re-read the lead with
`POST /leads/list {"limit": 1, "search": "<lead_email>"}` and update `interest_status` /
`interest_checked_at` / `is_positive` / `lead_payload`. One call per address, because the
server-side interest filter does not work (§2.2 #2); `interest_checked_at` throttles it to once
per `INSTANTLY_INTEREST_RECHECK_HOURS` (6) per address so a 15-minute cron does not re-ask
constantly.

Any row that is positive with `freshsales_deal_id IS NULL` then gets `POST /deals` —
`name` = `"<Company> — <campaign name>"`, `amount` = `FRESHSALES_DEFAULT_DEAL_AMOUNT`,
`sales_account_id` from pass 2 — and nothing else, so FreshSales applies its own default pipeline,
first stage and no owner.

**One deal per address, not per reply.** Andrew Ortega replied twice; that is one opportunity. The
deal is keyed on the contact, and later replies from the same address add notes to the existing
contact and deal rather than opening a second one.

Rows past the re-check window stop being polled and keep whatever label they had.
`--recheck-days N` widens it for one run.

### 6.4 Contact names

`from_address_json[0].name` is what the sender's own mail client put in the From header — a real
name, not a guess. The live data shows both kinds:

```
{"name": "Miguel Bautista",                  "address": "aftermathcustomauto1@gmail.com"}
{"name": "Sundowner Truck Accessories",      "address": "sundownertruckaccessories@gmail.com"}
```

So: split the display name on whitespace into first / last. When it looks like a business rather
than a person — it matches `payload.companyName`, or contains a business token (`truck`,
`accessories`, `auto`, `4x4`, `offroad`, `llc`, `inc`) — leave both name fields blank and let the
sales account carry the identity. Blank is the safe failure: this repo already refuses to guess
names onto outbound mail ([export_instantly_list.py](scripts/export_instantly_list.py) leaves
First/Last empty on purpose), and the same reasoning applies to a CRM record a human will read.

`payload.firstName` / `payload.lastName` exist but are empty strings on every lead tested — the
export never filled them — so they are a fallback that will not fire in practice.

### 6.5 Flags

```
--dry-run            # fetch, derive and match; push nothing; print what would be created
--since YYYY-MM-DD   # override the watermark (first run, or a backfill)
--recheck-days N     # widen the interest re-check window for one run
--limit N            # cap replies pushed, for a cautious first live run
--campaign <uuid>    # restrict to one campaign
```

`--dry-run` is the first thing run against the live FreshSales key, before anything is written.

### 6.6 Rate limits

Instantly's `GET /emails` is 20/min; the client paces off a monotonic clock so pass 1 cannot exceed
it. FreshSales is 1000/hour per account — shared with anything else touching that CRM — and pass 2
spends 3 calls per reply, so a run is capped at `FRESHSALES_MAX_CALLS_PER_RUN` (600), leaving the
rest for the next tick. A 429 from either vendor ends the pass cleanly rather than retrying into
the limit; the watermark means nothing is lost. At 22 replies/month none of this will ever bind —
it is there so a backfill or a runaway campaign cannot get us rate-limited out of our own CRM.

`src/integrations/rate_limit.py` is deliberately not used: it is built for per-credential
distributor sweeps keyed on hashed client ids, and a single-process poller against two fixed keys
does not need a DB round trip per call.

## 7. Failure handling

| Failure | Behaviour |
|---|---|
| Instantly down / 401 | `ScheduledTaskExecution` FAILED, nothing written, next tick retries |
| FreshSales down mid-run | Pass 1 already committed; passes 2–3 resume next tick |
| Reply with no `lead` address | Stored, skipped for push, counted in the run message |
| Duplicate reply in the overlap window | Absorbed by the unique constraint on `instantly_email_id` |
| Killed process (OOM / restart) | `cleanup_stale_started_executions("sync_instantly_replies")` marks the dangling STARTED row FAILED next run |
| A row failing repeatedly | `sync_attempts` + `last_error` on the row; anything over `--max-attempts` is named in the completion message, so it is visible without reading logs |

The completion message records ingested / contacts created / contacts updated / deals created /
skipped / failed, so `ScheduledTaskExecution` alone answers "is this still working".

## 8. Settings

`conf/settings_base.py`, read with `os.environ.get` beside the other vendor keys, and added to
`.env.app.example`.

```
INSTANTLY_API_KEY                    # Bearer token; emails + leads + campaigns read
INSTANTLY_BASE_URL                   = https://api.instantly.ai/api/v2
INSTANTLY_SYNC_OVERLAP_MINUTES       = 60
INSTANTLY_SYNC_INITIAL_DAYS          = 7
INSTANTLY_INTEREST_RECHECK_DAYS      = 60    # §2.3 — 18% are labelled late or never
INSTANTLY_INTEREST_RECHECK_HOURS     = 6
INSTANTLY_TIMEOUT_SECONDS            = 20

FRESHSALES_API_KEY
FRESHSALES_BUNDLE_ALIAS              = aftermarketscout
FRESHSALES_DEFAULT_DEAL_AMOUNT       = 0             # §11 Q2
FRESHSALES_MAX_CALLS_PER_RUN         = 600
FRESHSALES_TIMEOUT_SECONDS           = 20
```

Pipeline, stage, owner and contact-status ids are deliberately **not** settings. Pipeline and stage
are omitted from the deal payload so FreshSales applies its own defaults, and contact statuses are
resolved by name at runtime (§6.2). Nothing in the config carries a Freshworks numeric id, so
renaming or re-ordering a pipeline in the CRM cannot silently misfile a deal.

> Set these as **real environment variables** in `.env.app.production`, not only in a `.env` file —
> `settings_base` reads `os.environ` before `load_dotenv()` runs, so a key present only in `.env`
> reads as empty inside the container.

A missing key makes the command exit with a clear message rather than issuing an unauthenticated
request. Neither key goes in the repo; both were supplied over chat and should be treated as
already-exposed — worth rotating both once the integration is live and settled.

## 9. Deployment

Cron lives on the host and invokes the command inside the app container, which is how the existing
jobs run (`command_runner.sh` exists but, per
[TURN14_INTEGRATION_PLAN.md](docs/TURN14_INTEGRATION_PLAN.md), no crontab entry actually uses it):

```cron
*/15 * * * * docker exec aftermarketmonkey-be-app-1 python manage.py sync_instantly_replies >> /root/logs/instantly_sync.log 2>&1
```

Every 15 minutes: far inside both rate limits, and it gives Instantly's AI time to label a thread
before we read it. The container name must be confirmed against `docker ps` on the box (§11 Q4).

Rollout, so nothing reaches the live CRM unverified:

1. Deploy with no keys set — command exits cleanly, cron not installed
2. Confirm the FreshSales key can write (§11 Q1) — one test contact, with your go-ahead
3. Set the keys in `.env.app.production`
4. `--dry-run --since 2026-09-01` and read the output — expect 22 replies, 13 contacts, 6 deals
5. `--limit 3` for real; inspect those three contacts and deals in FreshSales by eye
6. Full backfill run, then install the cron line
7. Watch `ScheduledTaskExecution` for `sync_instantly_replies` over the first day

Auth and config for both vendors are already confirmed, so step 4 is reachable as soon as Q1 is.

## 10. Tests

`src/integrations/tests/test_instantly_freshsales_sync.py`, following the existing
`test_rough_country_edi.py` / `test_wheel_enrichment.py` shape — both HTTP clients mocked, no
network, fixtures under `src/integrations/tests/fixtures/`. The 22 live replies already captured
are the basis for the fixtures, anonymised.

Cases that must be covered — note that the first three are regression tests for §2.2, the places
the docs lied:

- **Pagination stops on an empty page even though a cursor is still returned** (§2.2 #3)
- **An `i_status == 0` row is treated as an auto-reply** without any `is_auto_reply` field present
- **The interest re-check uses per-email `search`** and never trusts a server-side interest filter
- Re-running the same window creates nothing new — the idempotency claim, asserted not assumed
- Each interest value in §2.1's table maps to deal / no deal
- A label arriving on a *later* run creates the deal then (the §6.3 lag — the whole reason that
  pass exists)
- Two replies from one address produce one contact, one deal, two notes
- FreshSales 200 and 201 both recorded as success
- A pass-2 failure leaves the row retryable with `last_error` set and does not stop the batch
- A business-looking display name leaves first/last blank; a person's name is split
- Each interest value maps to the right contact status, and a relabel updates it
- A contact status renamed in the CRM fails loudly rather than writing a stale id
- Missing key → clean exit, no HTTP call

## 11. Open questions

**Q1 — Can this FreshSales key write? — ANSWERED 2026-10-04: yes.** The 403 on
`GET /selector/owners` is specific to that selector and does not indicate a general permission gap;
accounts, contacts, notes and deals all write successfully. No new key is needed.

What the live backfill produced, read back from the CRM to confirm it is shaped right:

| | |
|---|---|
| Contacts | **13** created (one per address), 7 upserts returning 200 for repeat replies |
| Deals | **6**, all in `Default Pipeline` at stage `New`, unowned, account + contact linked |
| Notes | **20** — one per human reply, so a thread's history accumulates on the contact |
| Failures | **0** |
| Re-run | creates nothing; an idle tick costs 1 FreshSales call and ~2 Instantly calls |

Spot-checked `flatoutauto84@gmail.com`: contact `Zac L`, city Buford, state GA, zip 30518, work
number, contact status `Contacted` (correct — that reply is unlabelled), linked to sales account
*Flat-Out Auto Accessories* as primary with its website. Deal names read as
`Bucks 4x4 — Realtruck - 1/3 locations`.

Repeat replies behaved as designed on real data: Andrew Ortega's three replies and Carl Tucker's
three each produced **one** contact and **one** deal, with the extra replies landing as notes.

**Q2 — Deal amount.** `amount` is required and defaults to `0`. Your Default Pipeline highlights
`amount` as its headline field and aggregates on `expected_deal_value`, so every deal landing at
zero makes the pipeline view read as worthless — the 6 deals from the backfill would total $0. A
nominal per-shop figure would make it useful; name one and it becomes the default. Deal naming is
`"<Company> — <campaign name>"` (e.g. *Rhino Utah — Realtruck - 1/3 locations*) unless you want
something that reads better in your pipeline.

**Q3 — The 4 unlabelled replies.** Two are a month old and still blank, so Instantly's AI is not
going to label them. They will land as contacts with no deal. Either label them in Unibox and the
next run picks them up, or if unlabelled-means-worth-a-look to you, they could get a deal by
default — but that contradicts the "rely on Instantly's tag" decision, so it needs a deliberate yes.

**Q4 — Container name** for the cron line (§9), confirmed from `docker ps` on the box.

## 12. Build order

| Step | Work | State |
|---|---|---|
| 1 | `InstantlyReply` model + migration `0205` | **done**, applied |
| 2 | `clients/instantly/{client,exceptions}.py` — `iter_received_emails`, `find_lead`, `list_campaigns` | **done** |
| 3 | `clients/freshsales/{client,exceptions}.py` — account/contact upsert, deal, note | **done** |
| 4 | `services/instantly_freshsales_sync.py` — the three passes | **done** |
| 5 | `management/commands/sync_instantly_replies.py` + audit wiring | **done** |
| 6 | Settings, `.env.app.example` | **done** |
| 7 | Tests (§10) | **done** — 60 passing |
| 8 | Live dry run | **done** — 22 replies → 13 contacts, 6 deals |
| 9 | FreshSales write check | **done** — the key can write |
| 10 | Live backfill | **done** — 13 contacts, 6 deals, 20 notes, 0 failures |
| 11 | Install the cron line (§9) | **not done** — needs server access |

Verified against the live account, not just in tests:

| Claim | How it was checked |
|---|---|
| Ingest stores every reply | 22 seen, 22 created, 14 distinct addresses |
| **Idempotent** | Second run over the same window: 22 seen, **0 created** |
| The derived watermark works | Third run read only the 1-reply overlap, created 0, table still 22 |
| The re-check throttle works | Repeat runs make **0** lead calls instead of 14 |
| Every reply gets its shop context | 22 of 22 rows carry a lead payload |
| Labels agree with Instantly | 12 positive rows, 2 auto-replies — matching §2.3 exactly |

`instantly_reply` on production now holds those 22 rows. That is what the first real run would have
created anyway, so the staged rollout starts from pass 2 with nothing to re-read; truncate the table
if you would rather watch it fill from empty.

Three things found by running it that the plan had wrong, all now fixed in code and above:

- **Replies that arrived already labelled positive never got their lead payload.** `GET /emails`
  carries no shop details at all — company, city and phone live on the *lead* — and the re-check
  pass only selected rows that were not yet positive. So 12 of the 22 would have become contacts
  built from the address alone, and the four gmail repliers would have become sales accounts named
  `freetuck@gmail.com` rather than *Tuckers Trucks*. The selection is now "stale label **or**
  missing payload". This one was quiet: every record still reaches the CRM, just wrong.

- The preview counted contacts **per reply**, promising 20 contacts for 13 people. Contacts upsert
  on the address, so repeat replies are one contact and one deal; the count now says so, and a
  repeat reply reports as "note on existing contact".
- `Clarksville MC4x4 Sales` — a real From-header name — split into first `Clarksville`, last
  `MC4x4 Sales`. Neither word was a listed business token, so the business check now also treats
  any token containing `4x4` or `4wd` as a shop name, and the token list gained `sales`, `gear`,
  `group`, `center`, `jeep` and others. Verified against all 14 live display names: the five real
  people still split correctly, the four businesses all blank out.

Deliberately not in scope, as follow-ups once the loop is running:

- Matching the reply back to `Lead` / `RealTruckLead` to add brand lists and AI confidence to the
  contact (§5 — `lead_payload` already covers the useful fields)
- A `reply_received` webhook as a low-latency fast path (§4)
- Pushing CRM outcomes back to Instantly (it would need a write scope; the key is read-only today)


## 13. The API keys

Both have been supplied. For the record, where they come from:

**Instantly** — Settings → Integrations → **API Keys** → *Create API Key*. Scopes needed:
`emails:read`, `leads:read`, `campaigns:read`. Read-only is enough; nothing here writes back to
Instantly. The key is shown once and cannot be recovered; revoke an individual key rather than
rotating everything if one leaks.

**FreshSales** — avatar → **Profile Settings** → **API Settings**
(`https://aftermarketscout.myfreshworks.com/crm/sales/personal-settings/api-settings`). It is tied
to your user, so contacts and deals this sync creates are attributed to you and inherit your
permissions — which is why the owners 403 matters (Q1).

Bundle alias: **`aftermarketscout`**.

---

Verified live 2026-10-03: Instantly against the workspace holding campaigns *Google leads - 1/2
locations* and *Realtruck - 1/3 locations*; FreshSales reads against bundle `aftermarketscout`.
Doc sources:
[list emails](https://developer.instantly.ai/api-reference/email/list-email) ·
[list leads](https://developer.instantly.ai/api-reference/lead/list-leads) ·
[webhook events](https://developer.instantly.ai/guides/webhook-events) ·
[API keys](https://developer.instantly.ai/quickstart) ·
[Freshworks CRM API](https://developers.freshworks.com/crm/api/)
