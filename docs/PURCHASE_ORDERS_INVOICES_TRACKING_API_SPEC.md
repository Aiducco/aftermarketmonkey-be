# Purchase Orders / Invoices / Open Orders / Tracking – Frontend API Specification

Base URL: `{API_BASE}/api` (e.g. `https://your-backend.com/api`)

All endpoints below require a Bearer token; company scoping is derived from the token
(`request.company_id`), same as every other authenticated endpoint in this API. All are under
the `purchase-orders/` prefix.

This doc covers the existing Purchase Orders screen (already live) plus what's new: an **Open
Orders** filter, a flat **Invoices** view, and a flat **Tracking** view — each with click-through
detail. Nothing here changes the existing cart/checkout flow (`cart/`, `cart/review/`,
`<id>/submit/`, etc.) — that's unchanged and out of scope for this doc.

---

## 1. Data model, in one picture

```
PurchaseOrder (one per distributor per checkout — the "PO" the FE already lists)
 ├─ line_items[]              PurchaseOrderLineItem   (parts ordered, qty, pricing, backorder qty)
 ├─ distributor_orders[]      PurchaseOrderDistributorOrder  (one per distributor-side order/shipment slice)
 │                             → tracking_numbers[], carrier, ship_date, estimated_delivery_date,
 │                               delivery_status
 └─ invoices[]                PurchaseOrderInvoice    (one per shipment that's actually billed)
                                → total/freight/discount/paid/amount_due, tracking[], line_items[]
```

A PO commonly has **one line_items list**, but **multiple distributor_orders** (split shipments)
and **multiple invoices** (a shipment that goes out today, plus a backordered release next week,
are two separate invoices on the same PO). This is why Invoices and Tracking are exposed both
nested (inside PO detail) and as their own flat, cross-PO endpoints — see §3–4.

### Status enums

**`PurchaseOrder.status` / `status_name`** — drives the existing tab bar:

| Value | Name | Meaning |
|---|---|---|
| 1 | `DRAFT` | Cart row — never shown in order history (filtered out server-side) |
| 2 | `QUOTED` | Cart row, quoted — also filtered out of order history |
| 3 | `SUBMITTING` | Mid-submit |
| 4 | `SUBMITTED` | Sent to distributor, no confirmation yet |
| 5 | `CONFIRMED` | Distributor confirmed the order |
| 6 | `PARTIALLY_FULFILLED` | Some lines shipped/confirmed, others still open |
| 7 | `FULFILLED` | Fully shipped |
| 8 | `CANCELLED` | Cancelled |
| 9 | `FAILED` | Submission or quote attempt failed — see `error_message` |

**`PurchaseOrderDistributorOrder.delivery_status`** (new, string, nullable) — normalized across
all 5 distributors, independent of the distributor-specific `status`/`status_code`:

- `"in_transit"` — picked/packaged/shipped, not yet confirmed delivered
- `"delivered"` — confirmed delivered
- `"cancelled"` — distributor-side cancellation, detected without us calling cancel ourselves
  (currently only WheelPros' tracking feed can tell us this)
- `null` — no signal yet, or this distributor doesn't report one (see the capability matrix in §6)

**`PurchaseOrderLineItemStatus`** (line-item level): `1 PENDING, 2 CONFIRMED, 3 BACKORDERED,
4 REJECTED, 5 PARTIALLY_SHIPPED, 6 SHIPPED, 7 CANCELLED`.

---

## 2. Purchase Orders (existing + Open filter)

### `GET purchase-orders/`

Query params, all optional:

| Param | Type | Notes |
|---|---|---|
| `status` | string/int | A `PurchaseOrderStatus` name (`CONFIRMED`, case-insensitive) or numeric code, **or the new literal `"open"`** |
| `company_provider_id` | int | Filter to one connected distributor |

**`status=open` is new** — maps server-side to `status IN (SUBMITTED, CONFIRMED,
PARTIALLY_FULFILLED)`: placed but not yet fully closed out (excludes `FULFILLED`, `CANCELLED`,
`FAILED`, same as it already excludes cart-territory `DRAFT`/`QUOTED`). Use this to add an
**"Open"** tab alongside the existing All/Confirmed/Partially fulfilled/Failed tabs.

Response: array of PO summaries (line items omitted — use the detail endpoint for those):

```json
{
  "data": [
    {
      "id": 421,
      "po_number": "AMS-000421",
      "status": 5,
      "status_name": "CONFIRMED",
      "provider_name": "Turn 14",
      "provider_kind_name": "TURN_14",
      "company_provider_id": 12,
      "total": 1284.50,
      "subtotal": 1240.00,
      "estimated_shipping": 44.50,
      "submitted_at": "2026-07-22T18:21:00Z",
      "created_at": "2026-07-22T18:19:40Z",
      "distributor_orders": [ /* see §5 shape */ ],
      "invoices": [ /* see §3 shape */ ],
      "error_message": null,
      "ship_method": "7002",
      "po_name": null
    }
  ]
}
```

### `GET purchase-orders/<id>/`

Same shape, plus `line_items[]` and `item_count`. Unchanged — this is the PO detail / "click into
a PO" page.

### `GET purchase-orders/capabilities/`

Per connected distributor. **New field: `supports_invoices`** — use it to hide/grey the Invoices
tab's content for a distributor with no invoice data (today: WheelPros) instead of just showing
an empty list with no explanation.

```json
{
  "data": [
    {
      "company_provider_id": 12,
      "provider_id": 4,
      "provider_kind_name": "TURN_14",
      "provider_name": "Turn 14",
      "can_order_in_app": true,
      "supports_shipping_method_selection": true,
      "supports_cancel": false,
      "supports_invoices": true
    }
  ]
}
```

---

## 3. Invoices (new)

### `GET purchase-orders/invoices/`

Flat, cross-distributor, cross-PO invoice list — the same rows already nested under each PO's
`invoices[]`, but independently queryable/sortable. Use this for a standalone **Invoices** tab.

Query params, all optional: `company_provider_id` (int), `start_date` / `end_date`
(`YYYY-MM-DD`, filtered on `invoice_date`).

```json
{
  "data": [
    {
      "id": 88,
      "invoice_number": "INV-9982331",
      "invoice_date": "2026-07-21",
      "purchase_order_id": 421,
      "po_number": "AMS-000421",
      "company_provider_id": 12,
      "provider_name": "Turn 14",
      "provider_kind_name": "TURN_14",
      "distributor_order_number": "1302153",
      "website_order_number": "9981234",
      "total_price": 640.20,
      "freight": 12.00,
      "discount_amount": 0,
      "paid_amount": null,
      "amount_due": 640.20,
      "tracking": [
        {"ship_method": "UPS Ground", "tracking_number": "1Z999AA10123456784"}
      ],
      "line_items": [
        {
          "part_number": "MSH-1234",
          "description": "Cold Air Intake Kit",
          "quantity": 2,
          "unit_price": 305.10,
          "total_price": 610.20,
          "warehouse_code": "01"
        }
      ],
      "comments": null
    }
  ]
}
```

### `GET purchase-orders/invoices/<id>/`

Same shape as one row above — the invoice detail / "click into an invoice" page.

**`line_items[]` is new on every invoice, everywhere it appears** (this endpoint, the detail
endpoint, and the nested `invoices[]` on PO list/detail) — empty array where the distributor's
invoice data is header-only (see §6).

---

## 4. Tracking (new)

### `GET purchase-orders/tracking/`

Flat, cross-distributor, cross-PO tracking list — **one row per tracking number** (a distributor
order can carry several packages), for a standalone **Tracking** tab.

Query params, all optional: `company_provider_id` (int), `delivery_status`
(`in_transit` | `delivered` | `cancelled`).

```json
{
  "data": [
    {
      "distributor_order_id": 733,
      "purchase_order_id": 421,
      "po_number": "AMS-000421",
      "company_provider_id": 12,
      "provider_name": "Turn 14",
      "provider_kind_name": "TURN_14",
      "distributor_order_number": "1302153",
      "tracking_number": "1Z999AA10123456784",
      "warehouse_code": "01",
      "carrier": "UPS Ground",
      "ship_date": "2026-07-21",
      "estimated_delivery_date": "2026-07-24",
      "delivery_status": "in_transit",
      "status": 2,
      "status_name": "CONFIRMED",
      "updated_at": "2026-07-22T09:03:11Z"
    }
  ]
}
```

If a distributor order has no tracking numbers yet, it still appears once with
`"tracking_number": null` (so an order that hasn't shipped isn't invisible in this view — surface
it as "awaiting tracking" rather than filtering it out).

`status` / `status_name` here is the distributor-order-level status (`SUBMITTED / CONFIRMED /
PARTIALLY_SHIPPED / SHIPPED / CANCELLED`) — different from `delivery_status`, which is the
carrier/shipment-level signal described in §1. Show both if there's room; `delivery_status` is
the more useful one for a tracking-focused view.

---

## 5. `distributor_orders[]` shape (nested under PO list/detail, and the basis for §4)

```json
{
  "id": 733,
  "distributor_order_number": "1302153",
  "warehouse_code": "01",
  "status": 2,
  "status_name": "CONFIRMED",
  "tracking_numbers": ["1Z999AA10123456784"],
  "carrier": "UPS Ground",
  "ship_date": "2026-07-21",
  "estimated_delivery_date": "2026-07-24",
  "delivery_status": "in_transit"
}
```

`ship_date`, `estimated_delivery_date`, `delivery_status` are the three new fields — all
nullable, populated only when that distributor's status/tracking response actually carries the
signal (see the matrix below).

---

## 6. Per-distributor data availability

Don't build the UI assuming every distributor fills in every field — design for graceful
degradation (blank/"—" is expected and correct for a `null`, not a bug):

| | Turn 14 | Meyer | Premier | Keystone | Wheel Pros |
|---|---|---|---|---|---|
| **`supports_invoices`** | ✅ | ✅ | ✅ | ✅ (synthesized, see below) | ❌ never — no invoice API exists |
| Invoice `line_items[]` | ✅ itemized | ✅ itemized | ✅ itemized | ✅ itemized (from order-history rows) | n/a |
| Invoice `invoice_date` | ✅ | ✅ | ✅ (best-effort parse) | ❌ always null | n/a |
| Invoice `paid_amount` | ✅ | ❌ null | ❌ null (only `amount_due`) | ❌ null | n/a |
| `delivery_status` | — (only tracking #s today) | ✅ `in_transit`/`delivered` | ❌ always null (Premier's tracking API has no status field at all) | ✅ from `EKSTAT` for package/out-for-delivery/cancel stages | ✅ incl. distributor-side `cancelled` detection |
| `ship_date` | ❌ not wired yet | ❌ Meyer gives ETA/delivered-date, not ship date | ❌ not available | ✅ proxy from the transaction date at ship-stage | ❌ not available (only event dates) |
| `estimated_delivery_date` | — | ✅ | ❌ | ❌ | ✅ (latest tracking event date) |
| Cancel via app | ❌ no cancel endpoint | ✅ | ❌ no cancel endpoint | ❌ no cancel endpoint | ❌ no pre-shipment cancel (RMA only, not implemented) |

**Keystone's invoices are synthesized, not fetched** — Keystone has no dedicated invoice
endpoint; its "invoice" here is built by grouping `GetOrderHistory` transaction rows that share
an invoice number. Every such invoice's `comments` field says so explicitly
(`"Synthesized from Keystone GetOrderHistory transaction rows..."`) — consider surfacing that
string (or a badge keyed off its presence) so users don't mistake it for a real distributor
invoice document.

**Premier's invoice discovery is best-effort** — Premier's Invoice API can't be filtered by our
own PO number at all, only by Premier's own invoice/sales-order number or a date range. Invoice
numbers are discovered from the tracking-by-PO-number response, which is not fully confirmed to
carry an `invoiceNumber` field on every entry (flagged in the backend code as pending
confirmation against Premier's test environment). In practice this means: don't be alarmed if
Premier's Invoices tab is sparse relative to Turn14/Meyer even for POs that have clearly shipped
— it may need a follow-up backend pass once confirmed live.

---

## 7. Suggested page structure

A single **Orders** section with four tabs, all sharing one distributor-connection filter
dropdown (`company_provider_id`, from `GET purchase-orders/capabilities/`):

1. **Purchase Orders** (existing) — `GET purchase-orders/` with the existing status tabs
   (All / Confirmed / Partially fulfilled / Failed) **plus a new Open tab**
   (`?status=open`). Row click → PO detail (`GET purchase-orders/<id>/`), which already shows
   line items, distributor orders, and invoices nested — this detail page doesn't need to change.
2. **Invoices** (new) — `GET purchase-orders/invoices/`, filterable by distributor and date
   range. Grey out or hide for a distributor whose `supports_invoices` is `false`. Row click →
   `GET purchase-orders/invoices/<id>/` for line items, or just expand inline (the list response
   already includes `line_items[]`, so a detail call isn't strictly required — use it mainly for
   a stable deep-link URL).
3. **Tracking** (new) — `GET purchase-orders/tracking/`, filterable by distributor and
   `delivery_status`. Row click → the parent PO detail (`purchase_order_id` is on every row).
4. **Open Orders** — this can either be its own tab (`GET purchase-orders/?status=open`) or
   folded into tab 1 as described above; either is a legitimate reading of the requirement,
   pick whichever fits the existing tab-bar UX better.

All four tabs' rows carry `po_number` and `purchase_order_id`/`purchase_order_id` — always wire
a click-through back to the PO detail page from Invoices and Tracking, since that's where the
full line-item context lives.
