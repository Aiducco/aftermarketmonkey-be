# Admin Panel — New Endpoints (Create Company / Reset Password / Extend Trial)

Handoff spec for the frontend. These three endpoints are **live in production now**
(deployed, backend commit `d6efa70`). Nothing in the frontend calls them yet — the
existing `admin.ts` only implements the four read-only GETs (`getAdminCompanies`,
`getAdminCompanyDetail`, `getAdminProviders`, `getAdminProviderDetail`), so this is a
new contract to add, not something already partially wired.

## Auth (same as every existing admin-panel call)

All three require the normal `Authorization: Bearer <JWT>` header already used for
every other authenticated request, **and** the logged-in user must have `is_staff`
on the backend (checked server-side against a live DB row, not the JWT claim). A
request that fails this returns:

- `401` `{"message": "User not authenticated"}` — no/invalid token
- `403` `{"message": "Staff access required"}` — valid user, not staff

Nothing new to add on the frontend auth side if the admin pages already gate on the
same staff flag used for the existing read endpoints.

## Base path

Same flat namespace as the existing admin routes — no separate prefix:

```
https://api.aftermarketscout.com/admin/companies/...
```

(swap host for whatever `VITE_API_BASE_URL`/equivalent already points at in each env).

---

## 1. Create company

```
POST /admin/companies/create/
Content-Type: application/json
```

### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `company_name` | string | yes | 1–255 chars |
| `email` | string | yes | must be a valid email; must not already belong to any user |
| `first_name` | string | yes | 1–150 chars |
| `last_name` | string | yes | 1–150 chars |
| `password` | string | no | min 8 chars if given. **Omit it to have the backend generate a secure one** — recommended for the admin-panel flow, see response notes below |
| `trial_days` | integer | no | 1–365, default 21 |
| `subscription_plan` | string | no | max 64 chars, default `"hunter"` |

```json
{
  "company_name": "Example Off-Road",
  "email": "owner@example.com",
  "first_name": "Jane",
  "last_name": "Doe"
}
```

### Success response — `201`

```json
{
  "data": {
    "company_id": 73,
    "company_slug": "example-off-road-87fd9972",
    "user_id": 80,
    "email": "owner@example.com",
    "password": "gIamOVTwzC7m4PTD4Ny2rLeX",
    "subscription_plan": "hunter",
    "subscription_period_end": "2026-11-09T08:24:42.600983"
  }
}
```

**Important: `password` is the plaintext password for the new account** — either what
you sent, or the one the backend generated if you omitted it. **This is the only time
it is ever returned.** It is not stored anywhere retrievable and there is no
"forgot password" recovery for it beyond the reset-password endpoint below. The UI
must show this to the admin once (e.g. a one-time "copy/save this now" dialog) so
they can relay it to the customer — treat it the same way you'd treat a revealed API
key or a generated SFTP password elsewhere in the app, if there's an existing
"reveal-once" pattern to reuse.

### Error responses

- `400` `{"message": "Invalid payload", "data": "..."}` — schema validation failed (e.g. missing field, bad email format, password under 8 chars)
- `400` `{"message": "Company name is required."}` — blank company name after trimming
- `400` `{"message": "A user with this email already exists."}` — email already taken
- `401` / `403` — auth, see above
- `500` `{"message": "Error creating company"}` — unexpected server error

---

## 2. Reset a user's password

```
POST /admin/companies/<company_id>/users/<user_id>/reset-password/
Content-Type: application/json
```

Path params: `company_id` and `user_id` are both integers — `user_id` is the id shown
as `"id"` on each entry in the `users` array of the existing `GET
/admin/companies/<id>/` detail response (that field is literally the user's id, see
that endpoint's existing shape — it's named `id` there but it's `UserProfile.user_id`
under the hood, which is what this endpoint expects as `user_id`).

### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `new_password` | string | no | min 8 chars if given. **Omit to have the backend generate one** — same reveal-once treatment as create-company |

An empty body `{}` is valid (generates a password).

### Success response — `200`

```json
{
  "data": {
    "user_id": 80,
    "email": "owner@example.com",
    "password": "KOADJrBff3e7NA7OaBXTbycT"
  }
}
```

Same plaintext-once rule as create-company above: show it once, don't expect to
retrieve it again.

### Error responses

- `400` `{"message": "Invalid payload", "data": "..."}` — e.g. `new_password` under 8 chars
- `400` `{"message": "Invalid JSON body"}`
- `401` / `403` — auth
- `404` `{"message": "User not found in company"}` — the (company_id, user_id) pair doesn't match any `UserProfile` — e.g. stale UI state, user removed from the company since the page loaded

---

## 3. Extend a company's trial

```
POST /admin/companies/<company_id>/extend-trial/
Content-Type: application/json
```

Path param: `company_id` (integer).

### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `days` | integer | yes | 1–365 — how many days to add |
| `plan` | string | no | max 64 chars — overrides the company's `subscription_plan` if given; otherwise left as-is (or defaulted to `"hunter"` if the company had no plan at all) |

```json
{ "days": 30 }
```

### Success response — `200`

```json
{
  "data": {
    "company_id": 73,
    "subscription_plan": "hunter",
    "subscription_status": "trialing",
    "subscription_period_end": "2026-12-09T08:24:42.600983"
  }
}
```

**Behavior note for the UI**: the extension is added from whichever is later — *now*,
or the company's *current* `subscription_period_end` — not from today. So extending
an already-active trial by 30 days adds a full 30 days on top of its real remaining
time; it does not reset the clock to "30 days from today" and potentially shorten a
trial that had more than 30 days left. Worth surfacing the *resulting*
`subscription_period_end` back to the admin after the call (shown above) rather than
just a toast saying "extended by 30 days", so they can see the real new expiry date,
especially since it may not be exactly today+30.

### Error responses

- `400` `{"message": "Invalid payload", "data": "..."}` — e.g. missing/out-of-range `days`
- `401` / `403` — auth
- `404` `{"message": "Company not found"}`

---

## Suggested UI placement

All three read naturally as actions on the existing `AdminCompanyDetailPage` /
`AdminCompaniesPage`:

- **Create company**: a button/modal on the companies list page (`AdminCompaniesPage`), since it's not scoped to an existing company.
- **Reset password**: a per-row action next to each entry in the company detail page's existing `users` list (`AdminCompanyDetailPage`) — you already render `email`/`is_company_admin`/`is_active` etc. for each user there.
- **Extend trial**: an action on the company detail page itself, near where `subscription_plan`/`subscription_status`/`subscription_period_end` are already shown.

None of these need new GETs — reuse the existing `getAdminCompanies()` /
`getAdminCompanyDetail()` to refresh the list/detail view after a successful call
(e.g. re-fetch company detail after create/extend-trial to show the new state, or
after reset-password to confirm nothing else changed).
