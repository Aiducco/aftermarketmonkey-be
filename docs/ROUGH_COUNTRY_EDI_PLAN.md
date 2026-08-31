# Rough Country EDI (X12 005010) — sample review & implementation plan

Status: **review / pre-implementation.** Nothing is built yet. This document records (a) what the
four Alluvia sample files actually contain, (b) the defects and ambiguities we must resolve with
Rough Country / Alluvia before writing code, and (c) how the integration would fit onto the
existing purchase-order stack.

Source: Michael McBride email, Wed 26 Aug 2026, with four `.X12` samples (810, 850, 855, 856).

| Doc | Direction | Meaning |
|-----|-----------|---------|
| 850 | us → RC | Purchase Order |
| 855 | RC → us | PO Acknowledgment |
| 856 | RC → us | Advance Ship Notice (tracking) |
| 810 | RC → us | Invoice |

Trading partner IDs (both `ZZ` qualifier): RC production `64ROUGHC`, RC test `64ROUGHCT`.
Our ID appears in the samples as `TRIDENT` — **not yet confirmed by Alluvia** (see Q1).
Transport: **plain FTP**, hosted by Alluvia; credentials not yet provided.

---

## 1. The headline problem: EDI is asynchronous, our order stack is not

Every adapter we have today (Turn14, Keystone, Meyer, Premier, WheelPros) is synchronous
request/response HTTP. `DistributorOrderAdapter.submit_order()` calls an API and comes back, in the
same function call, with a distributor order number, per-line confirmed/backordered quantities and
pricing — which is what `purchase_order_jobs._run_submit` writes straight into
`PurchaseOrderDistributorOrder` / `PurchaseOrderLineItem` and why it can flip the PO to `CONFIRMED`
on the spot.

EDI over FTP gives us none of that. We drop an 850 file into a directory. Minutes to hours later,
three separate files come back in a directory we have to poll. So Rough Country needs a shape no
existing distributor has:

- **No live availability or shipping quote at all.** There is no 846 (inventory) or 832
  (price/catalog) in the sample set, and no synchronous stock check. `get_shipping_quote()` has
  nothing to call. Stock comes from the existing `jobber_pc2A.xlsx` feed
  (`src/integrations/clients/rough_country/client.py`), which is a periodic file, not a live check.
  Freight cost is not known until the 810 invoice arrives, *after* the goods ship.
- **Submit does not confirm.** The 850 write only means "the file was delivered", not "RC accepted
  the order". Confirmation is the 855, which arrives later and out-of-band.
- **A new inbound pipeline.** Someone has to poll FTP, parse envelopes, match documents back to
  `PurchaseOrder` rows, apply them, and archive. Nothing like this exists in the codebase today.

The good news is that the abstraction already has the two escape hatches we need:

- `DistributorOrderResult.distributor_confirmed = False` — already used by `EmailOrderAdapter` so a
  PO stays `SUBMITTED` (not `CONFIRMED`) when the submit call didn't actually confirm anything.
  Exactly right for "the 850 was delivered, the 855 hasn't come back yet".
- `DistributorOrderAdapter.fulfillment_channel()` — currently `"api"` / `"email"`. Add `"edi"` so
  the FE can say "Rough Country confirms this order asynchronously" instead of hardcoding kinds.
- `EmailOrderAdapter.get_shipping_quote()` already establishes the "return an empty `lines` list"
  precedent; `_run_quote` and `PurchaseOrderReviewPage.vue` handle it.

---

## 2. What the samples actually say, document by document

Encoding across all four: element separator `*`, component separator `>` (ISA16), segment
terminator `~`. Repetition separator (ISA11) is **inconsistent** — see §3.

### 2.1 — 850 Purchase Order (the one we generate)

```
BEG*00*DS*PONUMBER1**20260826    Original / Dropship / our PO number / (blank) / PO date
PER*OC*John Smith*TE*999...*EM*  Order contact — name, phone, email
SAC*C*D500***895                 Charge, service code D500, $8.95 (2 implied decimals)
DTM*038*20260826                 "Ship not later than"
TD5****ZZ*FG                     Ship method, mutually-defined code "FG"
N1*ST*Tommy Smith*92*DROPSHIP CUSTOMER + N3/N4   Drop-ship consignee
PO1*1*1*EA*50**VC*922*SK*TridentItemCode1        line / qty / UOM / price / RC SKU / our SKU
PID*F****Rough Country Leveling Kit
CTT*1                            (wrong — see below)
AMT*TT*1849.97                   1×50 + 3×599.99 — consistent
```

Maps cleanly onto what we already have: `BEG03` ← `base.resolve_po_number(po)`, `N1*ST/N3/N4` ←
`ShipToAddress`, `PER` ← the shop contact, `PO1` ← `OrderLineItemRequest` (`VC` = RC part number
from `ProviderPart.provider_external_id`, `SK` = our part number), `TD5-05` ← the selected ship
method.

**Three defects in this sample — do not copy it verbatim:**

1. `CTT*1` is wrong. CTT01 is the number of `PO1` line items, and there are 2. (The 855 and 810
   samples correctly say `CTT*2`.) If RC's translator validates CTT, a copied generator fails.
2. `ISA11 = U`. In 005010 ISA11 is the **repetition separator**; `U` is the 4010-era usage
   indicator. This file is a converted 4010 document. The other three samples use `^` (810) and
   `|` (855, 856).
3. `GS04 = 20260825` while `ISA09 = 260826` — the functional-group date is a day before the
   interchange date, and `GS05 = 0336` (HHMM) where the other three use HHMMSS. Both are legal;
   just note the samples are not machine-consistent.

`SE*16` is correct (ST…SE inclusive), so their generator does compute segment counts properly —
which is what makes the `CTT` value look like a hand-edit rather than a real behavior.

### 2.2 — 855 PO Acknowledgment (RC → us)

```
BAK*00*AD*PONUMBER1*20260826     Original / "Acknowledge with detail and change" / our PO / date
PO1*1*1*EA*50**VC*922*SK*...  →  ACK*IA*1*EA      Item Accepted, qty 1
PO1*2*1*EA*599.99**VC*70920BDA*SK*...  →  ACK*IA*3*EA
CTT*2
```

This is the document that flips a PO to `CONFIRMED` and sets per-line
`quantity_confirmed` / `quantity_backordered`. Two blockers:

- **We don't have their `ACK01` code list.** The sample only ever shows `IA` (Item Accepted). Our
  `PurchaseOrderLineItemStatus` needs to distinguish accepted / backordered / rejected /
  quantity-changed, which means we need every `ACK01` value they emit (`IA`, `IB`, `IR`, `IQ`,
  `IP`, …) and every `BAK02` (`AD`, `AC`, `AE`, `AH`, `RD`, …). Without the list, the status
  mapping is guesswork. **Blocking.**
- **`PO1-02` says 1 but `ACK02` says 3** on the second line, where the 850 ordered 3. Presumably
  `PO1-02` should echo the ordered quantity and `ACK02` is authoritative for what was
  acknowledged — but the sample contradicts itself, so this must be confirmed. Our parser should
  read quantity from `ACK02` regardless.
- **There is no RC-side order number anywhere in the 855.** `BAK03` is our own PO number. Our data
  model is keyed on `PurchaseOrderDistributorOrder.distributor_order_number`. If the real 855
  carries RC's sales-order number (usually `BAK08`/`BAK09` or a `REF*ON`), we use it; if it does
  not, we set `distributor_order_number` to our own PO reference the same way `EmailOrderAdapter`
  does, and the only RC-side identifiers we ever learn are the ASN's `BSN02` and the invoice
  number.

### 2.3 — 856 ASN (RC → us)

Hierarchy `BSN05 = 0001` → Shipment → Order → Pack → Item, and the sample follows it correctly:

```
HL*1**S    TD5****ZZ*FEDEX_GROUND, REF*CN*TrackingNumber, DTM*011*20260826 (ship date)
           N1*ST (consignee), N1*SE*Rough Country*92*435 + Dyersburg TN  ← warehouse code 435
HL*2*1*O   PRF*PONUMBER1***20260826                                       ← join key to our PO
HL*3*2*P   MAN*CP*TrackingNumber                                          ← package tracking
HL*4*3*I   LIN*22696824*VC*922*SK*TridentItemCode1,      SN1**1*EA
HL*5*3*I   LIN*22696825*VC*70920BDA*SK*TridentItemCode2, SN1**3*EA
```

Feeds `DistributorOrderStatus.tracking_numbers` / `carrier` / `ship_date` and
`ShippingQuoteLine.warehouse_code` (`435`). Notes and risks:

- The parser must **walk the HL tree**, not scan flat: a real multi-carton shipment has several
  `P` levels each with its own `MAN*CP`, and items hang off whichever pack actually contains them.
  Collect every `MAN*CP` into `tracking_numbers`.
- Both `REF*CN` (shipment level) and `MAN*CP` (pack level) carry a tracking number, and in this
  single-package sample they are identical. On a multi-package shipment they cannot both be right —
  need to know which is authoritative (`REF*CN` may become a master/BOL number).
- **The ASN carries no PO line number.** No `PO1-01` echo, no line reference on `LIN`. Matching ASN
  items back to `PurchaseOrderLineItem` is therefore by part number (`VC`/`SK`) only. That breaks
  if the same SKU appears on two lines of one PO — which our kit expansion
  (`PurchaseOrderLineItem.kit_source_provider_part`) can produce. Mitigation: aggregate by SKU
  before applying. Better: ask RC to echo the PO line number.
- No estimated delivery date is present, so `estimated_delivery_date` stays null for RC.
- `LIN01` (`22696824`) looks like an RC-internal item/line id; not joinable to anything of ours.

### 2.4 — 810 Invoice (RC → us)

```
BIG*20260826*88812345*20260826*PONUMBER1   invoice date / invoice # / PO date / our PO number
ITD*******30                               net 30
DTM*011*20260826*1750                      ship date/time
IT1**1*EA*50**VC*922*SK*TridentItemCode1
IT1**3*EA*599.99**VC*922*SK*TridentItemCode2   ← wrong VC, see below
TDS*184997                                 $1,849.97 (2 implied decimals)
CAD*ZZ***FG*FEDEX_GROUND                   carrier
SAC*C*D240***895**********Freight          $8.95 freight
```

Maps to `PurchaseOrderInvoice` (`invoice_number` ← `BIG02`, PO match ← `BIG04`, `freight` ← the
`SAC`, `line_items` ← the `IT1`/`PID` pairs, `tracking` empty — the 810 carries no tracking).

- **`VC` on line 2 is `922`, the same as line 1** — but the 850, 855 and 856 all say line 2's RC
  part is `70920BDA`. Either the sample is a copy/paste error or their real 810 does not carry a
  correct per-line vendor part, in which case invoice lines cannot be matched to parts at all.
  **Must confirm.**
- **`IT1-01` (line number) is empty on both lines**, so — like the ASN — invoice lines match by
  part number only. Same duplicate-SKU risk.
- **`TDS*184997` excludes the $8.95 freight** (it is exactly 1×50 + 3×599.99). X12 defines TDS01 as
  the total invoice amount *including* charges and allowances, so is the amount due $1,849.97 or
  $1,858.92? This feeds `DistributorInvoice.total_price` / `amount_due` and any reconciliation, so
  it has to be pinned down. **Blocking for invoice accuracy.**
- The `SAC` service code differs between directions: we are shown sending `D500` on the 850 and
  receiving `D240` (labelled "Freight") on the 810. Need their code list, and need to know what a
  `SAC` on our *outbound* 850 is even supposed to mean — us dictating an $8.95 charge to ourselves
  is unusual, and if we don't understand it we should omit it.
- `CAD04 = FG` sits in the SCAC element but `FG` is not a real SCAC (FedEx Ground is `FDEG`/`FXFE`).
  Treat all carrier/method values as RC's private code list, not standard codes.
- Bill-to is identified as `N1*BT*Trident*12*5559998888` — qualifier `12` is a telephone number.
  Confirm what our real account identifier will be.

---

## 3. Questions for Rough Country / Alluvia (send these before we write code)

**Blocking — cannot implement correctly without answers**

1. **Our trading-partner ID.** Is `TRIDENT` our real assigned ISA/GS ID, and is it the same for
   test and production, or does Alluvia assign a separate test ID (as RC has `64ROUGHC` vs
   `64ROUGHCT`)?
2. **Functional acknowledgments.** None appear in the sample set. Does RC/Alluvia send a **997 or
   999** for our 850s? Do they expect 997s back from us for their 810/855/856? Over plain FTP with
   no acknowledgment at all, a syntactically bad 850 is silently discarded and the order simply
   never happens — we would have no way to detect it. This is the single biggest operational risk.
3. **`ACK01` and `BAK02` code lists** for the 855 (§2.2) — the accepted / backordered / rejected /
   quantity-changed vocabulary, so we can map to line-item statuses.
4. **RC's own order number.** Does the production 855 carry RC's sales-order number (`BAK08`/`BAK09`
   /`REF*ON`)? If not, we key everything on our own PO number.
5. **Ship-method code list.** The full set of mutually-defined `TD5-05` / `CAD04` codes (`FG`,
   `FEDEX_GROUND`, …), including expedited air and LTL/freight for heavy items (bumpers, long-arm
   kits). This becomes our `list_shipping_methods()`.
6. **FTP details and security.** Host, port, credentials, inbound/outbound directory names, file
   naming convention. Is the file written to a temp name and renamed on completion (otherwise we
   will eventually read a half-written file)? Who deletes files after pickup? And: **can we get
   FTPS or SFTP instead of plain FTP?** Plain FTP puts our credentials and end-customer names and
   addresses on the wire in cleartext. We already run SFTP elsewhere (`relay_sftp_provisioning`,
   the DLG feed client) so this is no extra work on our side.
7. **Test environment loop.** We need a test mailbox against `64ROUGHCT` where an 850 we submit
   produces a real 855, 856 and 810 back. Without a round-trip we are writing parsers against four
   hand-edited files.

**Important — affects design, not blocking**

8. **Pricing authority.** Are the `PO1-04` prices on our 850 informational, or does RC validate
   them? Does RC price the order from our 850 or from our jobber price file? (The sample 810 bills
   at exactly the prices the 850 sent, which would be unusual.)
9. **`TDS` vs `SAC`** — is the invoice total inclusive of freight? (§2.4)
10. **`SAC` on our outbound 850** — what is `D500`, and should we send a `SAC` at all?
11. **Order cancellation.** Is there any cancel path — an 850 with `BEG01=01`, or an 860? Our
    adapter interface has `cancel_order()` / `supports_cancel()`; if there is no path, RC's adapter
    returns `supports_cancel() = False`.
12. **Order changes.** No 860/865 in the sample set — confirm changes are not supported at all.
13. **Delivery contact.** The `N1*ST` block has no phone or email for the consignee, only a name and
    address. Carriers need a phone for residential and LTL deliveries. Can we send a `PER` under
    `N1*ST`? We already carry `ShipToAddress.phone` / `.email`.
14. **Non-dropship orders.** `BEG02 = DS` is dropship. How do we express a stock order shipped to
    the shop's own address (our `ShipToAddress.is_shop_address`)? `BEG02 = SA`?
15. **`N1*ST` identifier.** Is `92*DROPSHIP CUSTOMER` a required literal marker, or a real
    identifier we are expected to populate?
16. **Line matching.** Can the 856 and 810 echo the PO line number (`PO1-01`)? Matching on part
    number alone breaks when the same SKU appears on two lines (§2.3).
17. **Duplicate/resubmission handling.** If we resend an 850 with the same `BEG03` PO number (e.g.
    after a timeout), does RC dedup or create a second order? Determines whether we can retry
    safely or must rely purely on our own job state.
18. **Field limits.** Max length RC accepts for `BEG03` (X12 allows 22; our `po_number` field is 64)
    and for the name/address elements. Also confirm USD-only (no `CUR` segment appears) and how
    sales tax on dropships is handled (no tax segments appear anywhere).
19. **Volume/timing.** How often does RC drop 855/856/810 files — event-driven or batched on a
    schedule? Sets our poll interval and the user's expectation for "how long until confirmed".

**Sample defects to flag back to them** (so we're not building against known-bad examples):
850 `CTT*1` should be `2`; 850 `ISA11 = U` is 4010-style in a 005010 document; 850 `GS04` is a day
before `ISA09`; 855 line 2 `PO1-02 = 1` vs `ACK02 = 3`; 810 line 2 `VC = 922` should be `70920BDA`;
856 `GS03 = "TRIDENT "` has a trailing space.

---

## 4. Multi-tenancy: whose account is this?

Worth deciding early because it shapes the credential model. Every other distributor integration is
per-company: `CompanyProviderOrderAccount` holds that shop's own credentials, and orders are placed
under the shop's own account with the distributor.

EDI here is **one interchange ID (`TRIDENT`) and one FTP mailbox for everything**. There is no
`N1*BY` buyer segment and no account-number `REF` in the 850, so Rough Country sees exactly one
customer — us — regardless of which shop placed the order. Implications:

- Trident is the merchant of record for every RC order; RC invoices Trident (the 810's `N1*BT` is
  "Trident"), and Trident bills the shop.
- FTP/ISA credentials belong in Django settings, not in per-company `CompanyProviderOrderAccount.
  credentials` — with, at most, an optional per-shop RC dealer reference if RC wants one on the PO.
- If shops are instead meant to keep their own RC accounts, we need RC to tell us which segment
  carries the dealer account number, and the whole design changes.

**This is a business decision, not a technical one — confirm with Michael before Phase 2.**

---

## 5. Proposed implementation

### 5.1 New shared EDI layer — `src/integrations/edi/`

Nothing EDI-shaped exists in the codebase. Built generically (not under
`clients/rough_country/`) because ATD and other traditional distributors will want the same thing.

- `envelope.py` — ISA/GS/ST reader and writer. Delimiters read from the ISA itself (never assumed),
  fixed-width ISA fields, whitespace-stripped partner IDs (the 856's `GS03` has a trailing space),
  correct `SE`/`GE`/`IEA` counts, and tolerant line handling: the samples are variously CRLF, and the
  850 is *mixed* CRLF **and** bare CR, with segment breaks appearing both mid-line and at line ends.
  The reader must split on `~` and strip all surrounding CR/LF rather than treating the file as lines.
- `sanitize.py` — strip/replace `*`, `~`, `>`, `^`, `|` from every free-text value we emit. A
  customer named "A*B" or an address with a `~` corrupts the whole interchange today.
- `control_numbers.py` — row-locked per-partner counters for ISA13 / GS06 / ST02. Must be unique
  and monotonic; a duplicate ISA13 is a rejected interchange.
- `transport.py` — FTPS/FTP/SFTP client for put/list/get/archive, defensively validating that a
  downloaded payload ends with `IEA` before it is processed (guards against half-written files).

### 5.2 New model — `EdiDocument`

FTP has no delivery semantics, so at-least-once delivery and our own dedup are mandatory.

```
direction (in/out), partner_id, doc_type (850/855/856/810/997),
interchange_control_number, group_control_number, transaction_control_number,
filename, raw_payload, parsed_payload (JSON), purchase_order (FK, nullable),
status (received/parsed/applied/unmatched/error), error_message, timestamps
unique: (partner_id, direction, interchange_control_number)
```

This is also the audit trail — the EDI equivalent of `PurchaseOrderSubmissionAttempt.
request_payload` / `response_payload`, which for RC would otherwise be empty.

### 5.3 `RoughCountryEdiOrderAdapter` — `src/integrations/orders/rough_country.py`

Registered in `registry.py` against `BrandProviderKind.ROUGH_COUNTRY` (currently listed there as
having no order API).

| Method | Behavior |
|---|---|
| `get_shipping_quote()` | Empty `lines`, like `EmailOrderAdapter` — no live quote exists. `po.subtotal` falls back to our frozen catalog pricing. |
| `submit_order()` | Build the 850, write it to the FTP outbound directory, record an `EdiDocument`, return `distributor_order_numbers=[resolve_po_number(po)]` with **`distributor_confirmed=False`** so the PO stays `SUBMITTED`. |
| `get_order_status()` | Reads state the inbound poller has already applied — not a live call. |
| `supports_invoices()` | `True`; `get_invoices()` reads stored 810 data. |
| `supports_cancel()` | `False` pending Q11. |
| `supports_shipping_method_selection()` | `True` once we have the code list (Q5). |
| `fulfillment_channel()` | New `"edi"` value. |

### 5.4 Inbound poller — `manage.py process_rough_country_edi` (cron)

List the FTP inbound directory → download → persist as `EdiDocument` (dedup on control number) →
parse envelope → route by `ST01`:

- **855** → match PO by `BAK03`; apply `ACK` per line to `quantity_confirmed` /
  `quantity_backordered` / line status; flip the PO to `CONFIRMED` (or `FAILED` on a rejection).
- **856** → match by `PRF01`; walk the HL tree; write `tracking_numbers`, `carrier`, `ship_date`,
  `warehouse_code` onto `PurchaseOrderDistributorOrder`; roll line items up to `SHIPPED`.
- **810** → match by `BIG04`; create/update `PurchaseOrderInvoice` (unique on
  `purchase_order` + `invoice_number`, so replays are safe).
- **997/999** (if Q2 says they exist) → mark the corresponding outbound `EdiDocument`
  accepted/rejected, and alert loudly on a reject — that is a lost order.

Unmatched or failed documents are stored with `status=unmatched/error` and alerted, never dropped.
Files are archived remotely (or moved to a `processed/` directory) only after a successful commit.

### 5.5 Phasing

| Phase | Scope | Est. |
|---|---|---|
| 0 | Answers to §3, FTP credentials, test mailbox | **blocked on RC/Alluvia** |
| 1 | `src/integrations/edi/` core + `EdiDocument` + FTP transport | 3–4 d |
| 2 | 850 generator + adapter `submit_order()` + registry + settings | 2–3 d |
| 3 | Inbound poller + 855 parser (→ `CONFIRMED`, line statuses) | 3 d |
| 4 | 856 parser (tracking) + 810 parser (invoices) | 2–3 d |
| 5 | 997 handling, retries, unmatched-document alerting, admin visibility | 2 d |
| 6 | End-to-end against `64ROUGHCT`, then one supervised real order | depends on Alluvia |

Roughly **two weeks of development** once Phase 0 unblocks, plus round-trip testing time with
Alluvia. Phases 1–2 could start on the sample files alone, at the risk of rework if the answers to
Q3/Q5 change the mapping.

### 5.6 Front-end / UX consequences

Worth raising with the product side now, because RC will not behave like Turn14 in the UI:

- **No live stock or freight at review time.** The review page must say availability and shipping
  are confirmed by Rough Country after submission — the `fulfillment_channel() == "edi"` path.
- **A pending state between submit and confirm.** POs sit at `SUBMITTED` for however long RC takes
  (Q19). The UI needs to explain that rather than looking stuck.
- **Rejection can arrive late.** A line rejected on the 855 shows up minutes or hours after the user
  thought the order was placed, so a notification path matters more here than for API distributors.
