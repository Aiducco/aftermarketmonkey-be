# Onboarding Flow – Frontend API Specification

Base URL: `{API_BASE}/api` (e.g. `https://your-backend.com/api`)

---

## Flow Overview

| Step | Screen | Endpoint | Auth |
|------|--------|----------|------|
| 1+2 | Account + Company (atomic) | `POST /onboarding/register/` | No |
| 3 | Personalization | `POST /onboarding/personalization/` | Yes (Bearer) |
| - | Status / resume | `GET /onboarding/status/` | Yes (Bearer) |
| - | Credential info | `GET /onboarding/distributor-credentials-info/` | No |

**Atomic design:** Steps 1 and 2 are combined. The user is never created without company details. No partial state.

**Progress bar:** Show "Step 1 of 2" (register) then "Step 2 of 2" (personalization). Or show 3 screens but submit Step 1+2 together on "Continue" from screen 2.

---

## Authentication

- **Header:** `Authorization: Bearer <access_token>`
- After register, store `access_token` from the response and use it for Step 3.
- Same token format as login; can be used for all other authenticated endpoints.

---

## Endpoints

### 1. Register (Step 1+2 – Atomic)

**POST** `/api/onboarding/register/`

**Auth:** None

**Request:** Account + company in one request. All required for account creation.
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "email": "john@company.com",
  "password": "securepass123",
  "role": "owner",
  "company_name": "Acme Parts LLC",
  "business_type": ["retail_store", "dealership"],
  "country": "US",
  "state_province": "California",
  "city": "Los Angeles",
  "postal_code": "90001",
  "tax_id": "12-3456789"
}
```

| Field | Type | Required | Validation |
|-------|------|----------|------------|
| first_name | string | Yes | 1–150 chars |
| last_name | string | Yes | 1–150 chars |
| email | string | Yes | Valid email |
| password | string | Yes | Min 8 chars |
| role | string | No | See role options below |
| company_name | string | Yes | 1–255 chars |
| business_type | string[] | No | Multi-select. See options below |
| country | string | No | Max 64 chars |
| state_province | string | No | Max 128 chars |
| city | string | No | Max 128 chars |
| postal_code | string | No | Max 32 chars |
| tax_id | string | No | EIN/VAT. "Skip for now" OK |

**Role options:**
```json
[
  {"value": "owner", "label": "Owner"},
  {"value": "parts_manager", "label": "Parts Manager"},
  {"value": "service_advisor", "label": "Service Advisor"},
  {"value": "technician", "label": "Technician"},
  {"value": "other", "label": "Other"}
]
```

**Business type options (multi-select):**
```json
[
  {"value": "retail_store", "label": "Retail Store"},
  {"value": "installation_repair_shop", "label": "Installation/Repair Shop"},
  {"value": "ecommerce", "label": "E-commerce solely"},
  {"value": "dealership", "label": "Dealership"},
  {"value": "fleet_manager", "label": "Fleet Manager"}
]
```

**Success (201):**
```json
{
  "message": "Account created",
  "data": {
    "user_id": 1,
    "access_token": "eyJ...",
    "company_id": 1,
    "onboarding_step": 2,
    "is_company_admin": true
  }
}
```

**Errors:**
- `400` – Validation error, email already exists, or company name missing
  ```json
  {"message": "A user with this email already exists."}
  ```
  ```json
  {"message": "Company name is required."}
  ```

---

### 2. Company Details (Legacy / Update)

**POST** `/api/onboarding/company-details/`

**Auth:** `Authorization: Bearer <access_token>`

Use only for legacy users with incomplete company (onboarding_step=1) or to update company during onboarding. New users should use the combined register above.

**Request:** Same as before (company_name, business_type[], country, state_province, city, postal_code, tax_id).

---

### 3. Personalization (Step 3) — "Which distributors do you work with?"

**POST** `/api/onboarding/personalization/`

**Auth:** `Authorization: Bearer <access_token>`

This step captures **preference only** — which distributors/categories the company is
interested in, used to personalize search & prompts. It does **not** connect real
distributor credentials or unlock live pricing/inventory. Do not label this step or its
copy as "connect your distributors" — use "Which distributors do you work with?" with
subcopy "We'll prioritize these in your search results. You can connect your accounts
later to unlock live pricing and inventory."

Real credential connection happens later, per-distributor, via
`POST /integrations/catalog/<id>/connect/` (see the integrations endpoints) — not during
onboarding.

**Request:**
```json
{
  "preferred_distributor_ids": [1, 3],
  "top_categories": ["Suspension/Lift Kits", "Lighting"]
}
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| preferred_distributor_ids | number[] | No | Provider IDs, see `available_distributors` from status (each includes `icon_url` for the distributor logo) |
| top_categories | string[] | No | See options below |

**Category options:**
```json
[
  "Suspension/Lift Kits",
  "Tonneau Covers",
  "Lighting",
  "Exterior Armor",
  "Performance Tuning",
  "Wheels & Tires",
  "Interior Accessories",
  "Bed Accessories"
]
```

**Success (200):**
```json
{
  "message": "Personalization saved",
  "data": {
    "company_id": 1,
    "onboarding_step": 4
  }
}
```

**Errors:**
- `401` – Not authenticated
- `400` – No company in token or validation error

---

### 4. Onboarding Status

**GET** `/api/onboarding/status/`

**Auth:** `Authorization: Bearer <access_token>`

Use to:
- Resume onboarding (e.g. after refresh)
- Show current step and progress
- Pre-fill forms with saved data

**Success (200):**
```json
{
  "data": {
    "company_id": 1,
    "onboarding_step": 2,
    "company_name": "Acme Parts LLC",
    "business_type": ["retail_store"],
    "country": "US",
    "state_province": "California",
    "city": "Los Angeles",
    "postal_code": "90001",
    "role": "owner",
    "preferred_distributor_ids": [],
    "top_categories": [],
    "available_distributors": [
      {"id": 1, "name": "Turn 14", "kind_name": "TURN_14", "display_name": "Turn 14", "icon_url": "https://api.aftermarketscout.com/uploads/t14_logo.png"},
      {"id": 3, "name": "Keystone", "kind_name": "KEYSTONE", "display_name": "Keystone", "icon_url": "https://api.aftermarketscout.com/uploads/keystone.png"}
    ],
    "business_types": [
      {"value": "retail_store", "label": "Retail Store"},
      ...
    ],
    "categories_options": [
      "Suspension/Lift Kits",
      "Tonneau Covers",
      ...
    ],
    "roles": [
      {"value": "owner", "label": "Owner"},
      ...
    ]
  }
}
```

`available_distributors[].icon_url` is the distributor logo — use it in the Step 3
checkbox list next to each distributor name.

**Errors:**
- `401` – Not authenticated
- `400` – No company in token

---

### 5. Distributor Credentials Info

**GET** `/api/onboarding/distributor-credentials-info/`

**Auth:** None

Not part of onboarding — this describes what the **separate, post-onboarding**
integrations "connect" flow (`POST /integrations/catalog/<id>/connect/`) needs for each
distributor. Use it for labels, placeholders, and help text on that connect screen, not
during the onboarding personalization step.

**Success (200):**
```json
{
  "data": {
    "turn_14": {
      "required": ["client_id", "client_secret"],
      "description": "OAuth2 credentials from Turn 14 API access"
    },
    "keystone": {
      "required": ["ftp_user", "ftp_password"],
      "description": "FTP credentials for Keystone inventory access"
    }
  }
}
```

---

## UI / UX Notes

1. **Atomic Step 1+2:** Show two screens (Account, Company) but submit both in one request when the user clicks "Create Account" on screen 2. Hold Step 1 data in form state; do not call the API until Step 2 is complete.
2. **Skip for now:** Tax ID and Step 3 distributor/category selections can be optional; add "Skip for now" / "Do this later".
3. **Progress:** Show "Step 1 of 2" (Account+Company) then "Step 2 of 2" (Personalization).
4. **Explain why:** Add short copy for fields like Tax ID (e.g. "We need this to securely connect your wholesale pricing.").
5. **Routing:** After register, store `access_token` and redirect to Step 3 (Personalization). After Step 3, redirect to the main app.
6. **Resume:** On load, call `GET /onboarding/status/`. If `onboarding_step` is 1, redirect to company-details. If 2, redirect to personalization. If 4, redirect to app.
7. **Email:** Prefer business domains; avoid Gmail/Yahoo for B2B.
8. **No confirm-password field:** Password has a show/hide (eye) toggle instead of a separate confirm field.
9. **Business type is multi-select:** shops can span multiple categories (e.g. Truck/Jeep + Wheel & Tire).
10. **Step 3 is preference-only:** never imply real authentication is happening in this step's copy — it stores intent (`preferred_distributor_ids`, `top_categories`) for personalizing search/prompts. Actual credential connection is a separate, later flow via the integrations "connect" endpoints.

---

## Error Response Format

All errors return JSON:

```json
{
  "message": "Human-readable error message",
  "data": "Optional validation details"
}
```

Common status codes:
- `400` – Bad request / validation
- `401` – unauthorized
