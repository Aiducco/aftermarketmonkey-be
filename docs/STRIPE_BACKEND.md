# Stripe Backend Integration

## Overview

The backend integrates with Stripe for:
1. **Checkout** – New plan subscriptions via Stripe Checkout
2. **Billing Portal** – Manage existing subscriptions (upgrade, cancel, payment methods)

## Plan Mapping

| plan_id | Stripe Product ID | Price |
|---------|-------------------|-------|
| starter | prod_UAb7IngirFX1mo | $49/mo |
| pro | prod_UAb7rv9k8zoPwE | $99/mo |
| growth | prod_UAb7VPi46dLlIo | $199/mo |

Configuration lives in `conf/settings_base.py`:
- `STRIPE_PLANS` – plan_id → Stripe Product ID
- `STRIPE_PLAN_AMOUNTS` – plan_id → amount in cents (monthly)

## Endpoints

### GET /api/billing/subscription/

Returns the company's current subscription or null if none.

**Response (with subscription):**
```json
{
  "subscription": {
    "plan_id": "pro",
    "plan": "Pro",
    "price": "$99/mo",
    "renewal_date": "2025-04-18",
    "status": "active"
  }
}
```

**Response (no subscription):**
```json
{
  "subscription": null
}
```

### POST /api/billing/create-checkout-session/

Creates a Stripe Checkout session for plan subscription.

**Request:**
```json
{
  "plan_id": "starter" | "pro" | "growth",
  "success_url": "https://yourapp.com/parts",
  "cancel_url": "https://yourapp.com/plans"
}
```

**Response:**
```json
{
  "url": "https://checkout.stripe.com/c/pay/cs_xxx"
}
```

**Flow:**
1. User authenticates (Bearer token)
2. Backend resolves company from token
3. Gets or creates Stripe Customer for company
4. Creates Checkout session with `mode=subscription`, `line_items` using `price_data` (Product ID + amount)
5. Returns checkout URL; frontend redirects user

### POST /api/billing/create-portal-session/

Creates a Stripe Billing Portal session for managing subscription.

**Request:**
```json
{
  "return_url": "https://yourapp.com/settings"
}
```

**Response:**
```json
{
  "url": "https://billing.stripe.com/session/xxx"
}
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| STRIPE_SECRET_KEY | Stripe secret key (sk_test_... or sk_live_...) |
| BILLING_PORTAL_RETURN_URL | Default return URL when not provided in portal request |

## Checkout Session Creation

The backend uses `stripe.checkout.Session.create()` with:

- `mode: "subscription"`
- `line_items` with `price_data`:
  - `product`: Stripe Product ID from STRIPE_PLANS
  - `unit_amount`: Amount in cents from STRIPE_PLAN_AMOUNTS
  - `recurring.interval`: `"month"`
- `success_url` / `cancel_url` from request
- `customer`: Stripe Customer ID (created or existing for company)
- `metadata`: `company_id`, `plan_id`

## Redirect Flow

- **Success**: User completes payment → redirected to `success_url` (e.g. `/parts`)
- **Cancel**: User cancels → redirected to `cancel_url` (e.g. `/plans`)
