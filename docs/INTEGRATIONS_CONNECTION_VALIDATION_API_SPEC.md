# Distributor Connection Validation – Frontend API Specification

Base URL: `{API_BASE}/api` (e.g. `https://your-backend.com/api`)

Covers `POST /integrations/catalog/<id>/connect/` and `PATCH /integrations/connections/<id>/`
(see `ONBOARDING_API_SPEC.md` for where these fit relative to onboarding — real credential
connection is a separate, post-onboarding flow).

As of this build, five distributors are checked live before credentials are saved — bad
input now fails the request with a specific error instead of silently failing the first
background sync.

---

## Endpoints

| Action | Endpoint | Auth |
|--------|----------|------|
| Connect (create) | `POST /api/integrations/catalog/<provider_id>/connect/` | Yes (Bearer) |
| Update (partial patch) | `PATCH /api/integrations/connections/<company_provider_id>/` | Yes (Bearer) |

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
    "credentials": { "client_id": "...", "client_secret": "..." },
    "primary": false,
    "connection_validated": true,
    "created_at": "2026-07-14T09:12:03Z",
    "updated_at": "2026-07-14T09:12:03Z"
  }
}
```

`connection_validated` is:
- `true` — we actually tested the connection and it passed
- `null` — this distributor isn't validated yet (see Coverage below). A save can succeed
  with `null`; that's expected, not a bug.

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
**Not validated.** Credentials are ours, generated at connect time — there's no user-entered
password to get wrong. `connection_validated: null`. A future check will confirm the
distributor's feed actually arrives on our relay, which is a data-freshness problem, not a
credentials one.

### Everything else (~18 distributors) — catalog only
**Not validated.** No backend fetch client exists yet for these, so there's nothing to test
a connection against. Connect always succeeds today with `connection_validated: null`.

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
    credentials: Record<string, unknown>;
    primary: boolean;
    // true = tested and passed · null = not validated for this distributor
    connection_validated: boolean | null;
    created_at: string;
    updated_at: string;
  };
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
