# Distributor Connection Validation – Frontend API Specification

Base URL: `{API_BASE}/api` (e.g. `https://your-backend.com/api`)

Covers `POST /integrations/catalog/<id>/connect/` and `PATCH /integrations/connections/<id>/`
(see `ONBOARDING_API_SPEC.md` for where these fit relative to onboarding — real credential
connection is a separate, post-onboarding flow).

As of this build, five distributors are checked live before credentials are saved — bad
input now fails the request with a specific error instead of silently failing the first
background sync.

**Credentials are namespaced into `feed` and `order`.** A connection's catalog/pricing-sync
credentials (`feed`) and its order-placement credentials (`order`) are always stored, submitted,
and validated separately — even for Turn 14, where the `order` values happen to be the exact
same OAuth client id/secret as `feed` (catalog-API and order-API access are separate permission
grants on Turn 14's side, so entering and validating them again is what actually confirms order
placement works, rather than assuming it does because the feed connected). See **Ordering
credentials** below for the full per-distributor field list.

---

## Endpoints

| Action | Endpoint | Auth |
|--------|----------|------|
| Connect (create) | `POST /api/integrations/catalog/<provider_id>/connect/` | Yes (Bearer) |
| Update (partial patch) | `PATCH /api/integrations/connections/<company_provider_id>/` | Yes (Bearer) |

---

## Request body

Both endpoints take credentials nested under `feed` and/or `order` — **not** a flat object of
bare field names:

```json
{
  "feed": { "client_id": "...", "client_secret": "..." },
  "order": { "account_number": "...", "security_key": "..." }
}
```

- `feed` — required on connect; on PATCH, omit it entirely to leave feed credentials
  untouched.
- `order` — always optional. Omit it to connect/update the feed only. If the distributor has
  no distinct order fields (see **Ordering credentials** below) and you don't send `order`,
  the backend auto-derives it from `feed` for you — don't send a duplicate copy.
- A flat, unwrapped body (e.g. `{"client_id": "..."}`) is rejected with `invalid_input` — it's
  not a legacy alias for `feed`, it's just invalid.

---

## Response shape

Same body shape on both endpoints.

**Success** (`201` connect / `200` update):
```json
{
  "data": {
    "id": 42,
    "company_provider_id": 42,
    "company_id": 7,
    "provider_id": 3,
    "provider_name": "Turn 14",
    "feed_credentials": { "client_id": "abc123" },
    "secrets_configured": { "client_id": true, "client_secret": true },
    "order_credentials": { "account_number": "999" },
    "order_secrets_configured": { "account_number": true, "security_key": true },
    "primary": false,
    "connection_validated": true,
    "order_connection_validated": null,
    "created_at": "2026-07-14T09:12:03Z",
    "updated_at": "2026-07-14T09:12:03Z"
  }
}
```

Credentials in the response are **redacted, never echoed in plaintext**: for a field
considered sensitive (name contains `password`, `secret`, `key`, or `token`), the value in
`*_credentials` is always `null` and `*_secrets_configured[field]` is `true` if a value is
stored. Non-sensitive fields (e.g. `client_id`, `account_number`, `ftp_host`) are returned as
their real stored value. Use `secrets_configured`/`order_secrets_configured` to render
"•••• (set)" for secret fields you can't prefill.

`connection_validated` / `order_connection_validated` are each:
- `true` — we actually tested that namespace's connection and it passed
- `null` — this distributor (or this namespace) isn't validated yet (see Coverage below). A
  save can succeed with `null`; that's expected, not a bug.

**Failure** (`400`, or `404` for `not_found` on the PATCH endpoint):
```json
{
  "message": "Login rejected by FTP server. Error: 530 Login or password incorrect!",
  "error_code": "invalid_credentials"
}
```

`error_code` is the field to branch UI logic on. `message` is plain English and safe to
show the customer as-is, but its exact wording varies per distributor — don't parse it.

---

## Error codes

| `error_code` | HTTP | Fires when | Suggested handling |
|---|---|---|---|
| `missing_fields` | 400 | A required field was left empty. Example: `"Missing required fields: client_id, client_secret"` | Highlight the empty field(s) — names are comma-separated in the message. Should be caught by client-side validation before submit; this is the server backstop. |
| `invalid_input` | 400 | A field was filled in but malformed or out of range (markup % outside 0–100, feed URL wrong prefix, unknown field on a patch). Example: `"wheel_markup must be between 0 and 100 (got 150)."` | Inline validation error on the specific field, same tier as a client-side format error. |
| `invalid_credentials` | 400 | The distributor's server actively rejected the login. Example: `"Login rejected by FTP server. Error: 530 Login or password incorrect!"` | Focus the username/password fields: "Check your credentials and try again." Confirmed distinct from a network failure — never a false positive from a slow server. |
| `permission_denied` | 400 | Login succeeded, but the account can't reach a resource we need (Turn 14 Brands API, a Wheel Pros feed directory). Example: `"Connected to Turn 14, but this account does not have permission to access Brands data…"` | Don't blame the form — show `message` verbatim in a banner; it already names who to contact. Credentials are correct, this is an account-entitlement problem on the distributor's side. |
| `connection_failed` | 400 | Could not reach the distributor at all — timeout, DNS, unexpected response, or (for relay-provisioned providers) our own SFTP account provisioning failed. | "Something went wrong connecting — try again," with a retry action. Not a field problem. |
| `not_found` | 400 connect · 404 patch | The provider or connection id doesn't exist. | Shouldn't happen from normal UI flow — treat as a generic error, log it. |

---

## Coverage — which distributors are actually checked

Only these five run a live check before saving. Everything else in the catalog still saves
immediately with `connection_validated: null` — not a failure, just "not tested yet."

### Turn 14 — API, OAuth2
Fields: `client_id`, `client_secret`

Fetches a token, then calls Brands — a single, unscoped, unpaginated call, the cheapest real
endpoint that still confirms API access beyond the token exchange. Some accounts have valid
credentials but no API permission grant, which otherwise only surfaced as a failed catalog
sync.
- `invalid_credentials` — bad token request
- `permission_denied` — token OK, Brands call rejected

### Keystone — FTPS
Fields: `ftp_user`, `ftp_password`

Login only, no download. Host is fixed and known-good, so a rejected login is reliably
`invalid_credentials`, not a network issue.

### Wheel Pros — SFTP
Fields: `sftp_user`, `sftp_password`, `wheel_markup`, `tire_markup`, `accessories_markup`

Login, then `stat()`s all three feed paths (wheel, tire, accessories) — an account can
authenticate but be locked out of one feed's directory. Markup fields are range-checked
(0–100) before any network call.
- `invalid_credentials`
- `permission_denied` — one or more feeds inaccessible
- `invalid_input` — markup out of range

### Premier / APG Wholesale — FTP
Fields: `ftp_user`, `ftp_password`

Login only, same pattern as Keystone. Host is fixed.

### Rough Country — HTTPS feed URL
Fields: `feed_url`

No login — a ranged request confirms the URL resolves and returns content, without
downloading the full spreadsheet. Prefix format is checked before any network call.
- `invalid_input` — wrong URL prefix
- `connection_failed` — unreachable / bad HTTP status

### Meyer, A-Tech — relay-provisioned SFTP
**Not validated at connect time.** Credentials are ours, generated at connect time — there's
no user-entered password to get wrong, so `connection_validated: null` on connect/update.
Instead these are covered by the periodic `status` check described below, which confirms the
distributor's feed file has actually arrived on our relay — a data-freshness problem, not a
credentials one.

### Everything else (~18 distributors) — catalog only
**Not validated.** No backend fetch client exists yet for these, so there's nothing to test
a connection against. Connect always succeeds today with `connection_validated: null`.

---

## Ordering credentials — when to show a separate form

Each catalog entry (`GET /api/integrations/catalog/`, and the connect/update/connection-detail
responses) tells you whether a distributor needs its own order-credentials form, reuses the
feed form, or has no ordering at all — via `supports_ordering`, `order_credentials_mirror_feed`,
and `order_connection_required_fields`/`order_connection_optional_fields`:

```json
{
  "supports_ordering": true,
  "order_credentials_mirror_feed": false,
  "order_connection_required_fields": ["client_id", "client_secret"],
  "order_connection_optional_fields": []
}
```

- **`supports_ordering: false`** — this distributor has no in-app ordering at all. Don't show
  any order-credentials UI.
- **`supports_ordering: true`** — show a distinct order-credentials form built from
  `order_connection_required_fields`/`order_connection_optional_fields`, and submit it under the
  `order` key. This applies even to Turn 14, whose order fields (`client_id`, `client_secret`)
  happen to be named and valued identically to its feed fields — the two are still entered and
  validated as separate submissions, because catalog-API and order-API access are independent
  permission grants on Turn 14's side. Pre-filling the order form with the feed values the user
  already entered is a reasonable UX shortcut, but always submit and validate them under `order`
  explicitly; nothing is auto-copied server-side.
  Note: some of these distributors (Meyer, Wheel Pros, Premier/APG as of this writing) report
  `supports_ordering: true` and declare their required order fields even though in-app order
  *placement* isn't wired up on the backend yet — showing the credentials form now just lets
  companies get ahead of it; submitting an actual order for these will fail until the backend
  adapter ships.
- **`order_credentials_mirror_feed`** — reserved for a future distributor whose order
  credentials would be silently auto-derived from `feed` with no separate form at all. Always
  `false` today; every currently order-capable distributor (Turn 14 included, as of this
  change) needs its own explicit `order` submission.

Rotating Turn 14's `feed` credentials (client secret regenerated, etc.) does **not** touch
`order` — PATCH each namespace explicitly, even when you're resubmitting the same new values to
both.

---

## Live status (catalog + connection detail)

Separate from the connect/update-time checks above: every `CompanyProviders` connection
also carries a background-refreshed `status`, exposed directly on the endpoints you already
call — no new endpoint needed.

- `GET /api/integrations/catalog/` — each catalog row includes `status` fields for
  distributors the company has connected.
- `GET /api/integrations/connections/<company_provider_id>/detail/` — same fields on the
  single-connection view.

```json
{
  "status": 1,
  "status_name": "CONNECTED",
  "status_reason": null,
  "status_checked_at": "2026-07-14T08:13:05.321224Z"
}
```

| `status_name` | Meaning |
|---|---|
| `null` | Not connected, or connected but for a distributor with no live check built yet (see below). Treat like "unknown," not an error. |
| `CONNECTED` | Initial sync has completed — live data available. Set once, directly, the moment the first pricing sync job finishes; never re-checked after that. |
| `INGESTING` | Connectivity confirmed good (or, for relay distributors, the feed file has arrived), but the initial sync hasn't finished processing it yet. |
| `WAITING` | Relay-provisioned distributor (Meyer, A-Tech) — connectivity to our own relay is fine, but the distributor's feed file hasn't arrived yet. Nothing wrong on the customer's end; `status_reason` says as much. |
| `FAILING` | Live check failed — bad credentials, or (rarely, for relay distributors) our own relay account is broken. `status_reason` has the detail, same messages as the `error_code` table above for the five validated distributors. |

**Set immediately on connect/update, then kept fresh by cron.** `connect_provider`/
`update_connection` compute and save `status` the moment the connection is created or
patched — for the five validated distributors it's a free byproduct of the validation that
already ran to accept the request (no extra network call); for Meyer/A-Tech it runs one relay
check. So the response from the connect button itself already carries a real status, not
`null` while waiting for the next cron tick. After that, a cron-scheduled command
(`check_company_provider_connections`, every ~5 minutes) re-checks every connection where
the initial sync hasn't completed yet, the same way. Once a connection reaches `CONNECTED`
it's no longer touched by this job — if credentials break *after* the first successful sync,
`status` will not reflect that (a known gap, not yet built). Everything outside those seven
kinds is left with `status: null` — no live check exists for it yet.

---

## Types

```ts
export type ConnectionErrorCode =
  | "missing_fields"
  | "invalid_input"
  | "invalid_credentials"
  | "permission_denied"
  | "connection_failed"
  | "not_found";

export interface ConnectErrorResponse {
  message: string;
  error_code: ConnectionErrorCode;
}

export interface ConnectSuccessResponse {
  data: {
    id: number;
    company_provider_id: number;
    company_id: number;
    provider_id: number;
    provider_name: string;
    // Sensitive fields (password/secret/key/token in the name) are always null here —
    // check secrets_configured[field] instead.
    feed_credentials: Record<string, unknown>;
    secrets_configured: Record<string, boolean>;
    order_credentials: Record<string, unknown>;
    order_secrets_configured: Record<string, boolean>;
    primary: boolean;
    // true = tested and passed · null = not validated for this distributor/namespace
    connection_validated: boolean | null;
    order_connection_validated: boolean | null;
    created_at: string;
    updated_at: string;
  };
}

export type ConnectionStatusName = "CONNECTED" | "INGESTING" | "WAITING" | "FAILING";

export interface ConnectionStatusFields {
  status: 1 | 2 | 3 | 4 | null;
  status_name: ConnectionStatusName | null;
  status_reason: string | null;
  status_checked_at: string | null;
}
```

---

## UI / UX Notes

1. **Branch on `error_code`, display `message`.** Don't parse `message` text — it's
   distributor-specific and will change wording over time; `error_code` is the stable contract.
2. **`permission_denied` is not a form error.** The user's credentials are correct; showing
   a red outline on the password field would be misleading. Show it as an informational
   banner instead.
3. **PATCH re-validates the whole connection, not just changed fields.** If only a
   non-secret field changes (e.g. Wheel Pros markup %), the stored password is reused and
   re-validated anyway — every successful patch re-confirms the connection still works.
   Occasionally slower than a pure field update, but a connection can't silently go stale
   between edits.
4. **`connection_validated: null` on success is normal**, not a partial failure — most of
   the catalog (relay-provisioned and not-yet-built distributors) isn't validated yet. Don't
   show an error state for `null`; a neutral "connected" is correct.

---

## Error Response Format

All errors return JSON:

```json
{
  "message": "Human-readable error message",
  "error_code": "invalid_credentials"
}
```

`error_code` is present on `connect`/`update` failures from this spec; other endpoints in
the API may return `{"message": "..."}` without `error_code` (see `ONBOARDING_API_SPEC.md`).

Common status codes:
- `400` – Bad request / validation / connection check failed
- `401` – Unauthorized
- `404` – Not found (`not_found` error_code on the PATCH endpoint)
