# Thoroughbred Diesel — Ingest Spec

Target: `thoroughbreddiesel.com` (diesel performance retailer, Winchester KY).
Everything below was measured live on **2026-10-26**. Counts are exact, not estimates.

**TL;DR** — One open search API gives you the entire catalog *and* the entire fitment table
in 712 requests / ~3 minutes, with no auth, no cookies and no login. Live per-SKU stock is
the only thing that needs page scraping.

---

## 0. What you get / don't get

| | |
|---|---|
| Products (orderable SKUs) | **51,245** |
| Fitment rows | **676,553** (99.98% resolve to a real YMM) |
| Vehicles | **1,251** (1982–2027, 8 makes) |
| Brands | **234** |
| Category tree | 4 levels — 13 / 90 / 413 / 119 |
| Images | ✅ 100% |
| Price | ✅ 100% (retail/MAP) |
| Inventory | ✅ 100%, but a **~93% snapshot** (see §4) |
| **Descriptions** | ❌ not in the API — **using `title` instead** (see §5) |
| **Wholesale/dealer price** | ❌ **not exposed anywhere public** (see §5) |

---

## 1. Transport rules (read first — these will bite you)

### 1.1 User-Agent is backwards from what you expect

The origin (`www.thoroughbreddiesel.com`) sits behind Cloudflare with a UA-sensitive rule.
Measured on the same URL:

| User-Agent | Result |
|---|---|
| *(none — curl default)* | **200** |
| `curl/8.7.1` | **200** |
| Android Chrome mobile | **200** |
| Googlebot | **200** |
| **Desktop Mac Chrome** | **403** (4,556-byte Cloudflare page) |
| **`Python-urllib/3.12`** | **403** (17-byte stub) |

**Do NOT spoof a desktop browser UA — that is the thing that gets blocked.** Send no UA, or
`curl/8.x`. If you use `requests`/`urllib`, you must override the default UA or you get 403.

> This is the *opposite* of SuspensionMAXX, where urllib's default UA was blocked and a browser
> UA fixed it. Probe per-target; do not carry the fix across sites.

The HawkSearch host (§2) is a **different domain and is not behind this Cloudflare at all** —
UA is irrelevant there.

### 1.2 No auth of any kind
No login, no cookies, no session, no CSRF token, no API key beyond the public `ClientGuid`
that is printed in the page HTML. PDPs render `AnonymousView` and still expose stock + price.

### 1.3 robots.txt
```
Disallow: /search/      Disallow: /admin/      Disallow: /my-account/
Disallow: /filemerge.axd?   Disallow: *view-all   Disallow: *?b=
```
Product PDPs and category pages are **not** disallowed. Do not crawl `/search/` or
`/filemerge.axd?`. The HawkSearch host is not covered by this robots.txt.

---

## 2. PRIMARY ENDPOINT — HawkSearch (catalog + fitment)

```
POST https://thoroughbreddiesel.searchapi-na.hawksearch.com/api/v2/search
Content-Type: application/json
```
`/api/search` also works. `/api/v1/search`, `/api/search/v2`, `/v2/search` → 404.

### 2.1 Request

```json
{
  "ClientGuid": "362f7bd3bfc14e378f68a0f5745aa87d",
  "IndexName": "thoroughbreddiesel",
  "MaxPerPage": 72,
  "PageNo": 1
}
```

**Omit `Query` entirely to get the full catalog.** 51,245 docs / 712 pages @ 72.

#### `MaxPerPage` accepts ONLY 24, 48, 72
Send 96, 120 or 500 and it does **not** error — it silently falls back to **24**, quadrupling
your page count. Always assert `response.Pagination.MaxPerPage == 72`.

#### No deep-paging cap
Page 712 returns the tail; page 713 returns an empty success. You can genuinely walk it all.

#### Optional `Query` (Lucene syntax)
| Query | Returns | Use |
|---|---|---|
| *(omitted)* | **51,245** | ✅ full catalog — use this |
| `-it.keyword: child` | 26,251 | PDP-group level only |
| `parentitem.keyword: true` | 16,654 | ⚠️ **do not use** — see below |
| `it.keyword: regular` | 17,856 | |
| `b.keyword: Fleece` | per-brand | incremental refresh |
| `vehicleid.keyword: 1057 OR isuniversal.keyword:True` | per-vehicle | what the site itself uses |

> ⚠️ **`parentitem` is a display flag, not a product filter.** The site's own JS uses it, which
> is misleading. It omits 6,111 real standalone SKUs. Also `parentitem.keyword: false` returns
> **0** (the field is *absent*, not false) — so a sanity check reads as "none exist".
> Likewise `_exists_:` / `-_exists_:` are **no-ops** here and return the full 51,245.

### 2.2 Item types (`it`)

| `it` | count | meaning |
|---|---|---|
| `regular` | 17,856 | simple product |
| `child` | 24,994 | **a real orderable SKU** — own sku, own PDP url, own price, own stock, own fitment |
| `parent` | 6,754 | configurable product |
| `group` | 1,641 | kit/group |
| | **51,245** | every doc has a unique id **and** sku **and** url (clean 1:1:1) |

**Do not discard `child`.** Half the fitment lives there (child 345,041 rows vs regular 331,034).

### 2.3 Response shape

```json
{
  "Success": true,
  "Pagination": { "NofResults": 51245, "CurrentPage": 1, "MaxPerPage": 72, "NofPages": 712 },
  "Results": [ { "Document": { ...36 fields... } } ],
  "Facets": [ {"Name":"Department",...}, {"Name":"Brand",...} ]
}
```

### 2.4 Sample `Document` (real, `it=regular`)

```json
{
  "id":        ["90740"],
  "sku":       ["SSD6.7F-DCR-1"],
  "title":     ["S&S Ford 6.7L CP4 to DCR Conversion - 11-26 Powerstroke"],
  "url":       ["https://www.thoroughbreddiesel.com/67f-dcr-1/"],
  "image":     ["https://www.thoroughbreddiesel.com/assets/1/20/dimthumbnail/ss-diesel-67f-dcr-1-1-1.jpg"],
  "imagealttag":["S&S Ford 6.7L CP4 to DCR Conversion - 11-26 Powerstroke"],
  "b":         ["SSDiesel"],
  "price":     ["+0000000000002400.0000"],
  "custom_price_range": ["> $1,000"],
  "inventory": ["252"],
  "it":        ["regular"],
  "type":      ["regular"],
  "d":         ["Performance"],
  "d2":        ["Performance/InjectionPumps"],
  "d3":        ["Performance/InjectionPumps/CP4Pump"],
  "d4":        ["Performance/InjectionPumps/CP4Pump/ConversionKits"],
  "vehicleid": ["1057","1058","1059","1060","1061","1062", "...68 total"],
  "bucketid":  ["31","30"],
  "hasvehicles":["true"],
  "isonsale":  ["false"],
  "ishotseller":["true"],
  "hasmailinrebate":["false"],
  "global":    ["true"],
  "parentitem":["true"],
  "kitshortdescription": ["Emissions Compliance: CARB EO # D756-7"],
  "_hawksalesboost":     1.551306,
  "_hawkadd2cartboost":  8.510676,
  "_hawklearningboost":  1.7121849
}
```

Sample `it=child` (proves children are real products):
```json
{ "id":["4305"], "sku":["ARP247-4202"], "it":["child"],
  "url":["https://www.thoroughbreddiesel.com/247-4202/"],
  "title":["ARP Head Studs - 98-24 Dodge Cummins (ARP 2000 Material) - 247-4202"],
  "b":["ARP"], "price":["+0000000000000502.7000"], "inventory":["47"],
  "m":["ARP2000MaterialMOSTPOPULAR"], "vehicleid":["926","927","928","929","...88"] }
```

### 2.5 Field reference (36 fields, fill rate over all 51,245)

| Field | Fill | Notes |
|---|---|---|
| `id` `sku` `url` `title` `image` `imagealttag` | 100% | all unique per doc |
| `it` `type` `inventory` `isonsale` `ishotseller` `hasmailinrebate` `hasvehicles` | 100% | |
| `price` | 100% (51,242) | **Solr sortable string** — see §2.6 |
| `custom_price_range` | 100% | display bucket, e.g. `"> $1,000"` |
| `b` | 99.9% | brand; 31 docs unbranded |
| `d` / `d2` / `d3` / `d4` | 99.7 / 98.5 / 85.5 / 9.2% | slash-delimited path |
| `global` | 81.5% | |
| `bucketid` | 69.8% | engine-family nav bucket |
| **`vehicleid`** | **65.2%** | **full fitment array — §3** |
| `parentitem` | 32.5% | display flag only, absent elsewhere |
| `kitshortdescription` | 28.8% | fitment/compliance notes, NOT marketing copy |
| `minprice` / `maxprice` | 16.4% | ranged parents |
| `_hawklearningboost` | 5.4% | ML rank signal |
| `_hawkadd2cartboost` | 5.2% | add-to-cart proxy |
| `_hawksalesboost` | 2.9% | **best-seller proxy** (max 10.0) |
| `f` `ipcf` `ft` `fr` `fity` `m` | 0.1–3.9% | variant options: finish(127) / color(53) / filter type(55) / flow rate(83) / (4) / material(11) |

### 2.6 PARSING GOTCHAS

**Every field is an array of strings**, even scalars → `doc["id"][0]`.
**Except** `_hawksalesboost` / `_hawkadd2cartboost` / `_hawklearningboost`, which come back as
**raw floats**. Use a tolerant accessor:
```python
def g(doc, k):
    v = doc.get(k)
    return (v[0] if v else None) if isinstance(v, list) else v
```

**Price is a zero-padded Solr sortable string**, not a number:
```python
price = float(raw.lstrip('+'))   # "+0000000000002400.0000" -> 2400.00
```
Verified against the live PDP: index `$2,400.00` == page `$2,400.00`. ✅
Range: $0.81 – $28,994.99, median ≈ $215.

**`hasvehicles` over-claims.** 11,707 docs flag it true but only 8,549 (of the 26,251 PDP-level
set) carry a `vehicleid` array. The reverse never happens. **Filter on the array, not the flag.**

**Image placeholders:** 327 image URLs contain `no-image`. Filter them.

### 2.7 Measured crawl cost
712 pages @ 72, 5 workers → **51,245 docs in 169s, 0 failed pages.**

---

## 3. FITMENT

`vehicleid` on each doc **is the complete fitment list for that SKU** — no per-vehicle crawl
needed. One hitch SKU carries 405 vehicles. Totals: **676,553 rows**, 33,429 docs with fitment.

### 3.1 Decoding `vehicleid` → Year/Make/Model

Needs the origin (§1.1 UA rules apply). Three chained GETs:

```
GET /ajax/store/ajax.aspx?f=GetYears&makeId=206
    -> {"Success":true,"Years":[1983,...,2027]}

GET /ajax/store/ajax.aspx?f=GetModels&makeId=206&year=2021
    -> {"Success":true,"Models":[{"Name":"F250","VehicleModelId":427}, ...]}

GET /ajax/store/ajax.aspx?f=GetVehicle&makeId=206&year=2021&modelId=427
    -> {"Success":true,"Vehicle":{"VehicleId":...,"CustomUrl":"/2021-ford-f250/"}}
```

Makes are hardcoded in the page (do not crawl for them):
```python
MAKES = {"Chevy":204,"Dodge":205,"Ford":206,"GMC":203,
         "Jeep":207,"Nissan":208,"Toyota":210,"Volkswagen":209}
```

**Cost:** 8 + 286 + 1,251 = 1,545 requests, ~2 min. Yields **1,251 vehicles, 0 failures.**

```json
"1092": {"make":"Chevy","year":"1982","model":"C/K Series 1500","url":"/1982-chevy-c-k-series-1500/"}
```

Per-make vehicle counts: Ford 316, Chevy 236, GMC 232, Dodge 170, Jeep 125, VW 63, Toyota 59, Nissan 50.

### 3.2 Fitment gotchas
- **1 orphan.** `vehicleid` 1678 is referenced by products (e.g. `EDG84130-4`) but unreachable
  via the picker. **LEFT JOIN**, never inner — an inner join silently drops those rows.
- This is **spec/YMM fitment, not ACES `BaseVehicleID`.** Do not cross-product the flat
  year/make/model lists to synthesise fitment — walk the real combos only.
- 99.98% of rows (676,413 / 676,553) resolve.

---

## 4. LIVE INVENTORY

### 4.1 There is no inventory API — proven, not assumed

I enumerated the real `ajax.aspx` function list out of the client bundle. It contains exactly
two: `AddImpression` and `CheckAvailability`. **`CheckAvailability` is a red herring** — its
signature is `?f=CheckAvailability&Username=...`; it is the account-registration *username*
check, nothing to do with stock.

Unknown function names return **HTTP 200 with 0 bytes** — silently, on both GET and POST, and
with either `f=` or `F=`. Known functions return JSON or a System Error. So 0 bytes is the
"no such function" tell, and you cannot discover endpoints by guessing.

Known-valid functions: `GetSummary`, `GetYears`, `GetModels`, `GetVehicle`, `ClearVehicle`,
`GetYearsByItem`, `GetModelsItem`, `GetVehicleItem`, `AskProductQuestion`, `ValidateReCaptcha`,
`CheckAvailability`, `AddImpression`.

**Why none exists:** the stock logic is entirely client-side over data already server-rendered
into the PDP. The bundle reads `CurrentItem.q` / `.c` out of `options.Items` with no fetch.

### 4.2 Option A — index `inventory` (recommended for bulk)

Free, in the same 712-request pull, 100% coverage. **Accuracy ~93%**: on 30 exact-ID
comparisons of `it=regular` docs vs the live PDP, 28 matched; both misses were 1→2 drift on
low-qty items. Good for "is it zero or not", not authoritative for exact counts.

- 3,054 of 26,251 PDP-level products are in stock (11.6%). Zero stock is the norm — drop-ship heavy.
- **One doc uses `99999999` as an unlimited/drop-ship sentinel.** Clamp it or it injects 100M
  phantom units into any sum.
- Stock depth varies wildly by brand: Fleece 325 of 439 in stock; Dorman **6 of 2,249**.

> When benchmarking accuracy yourself: compare **only `it=regular`** docs and match on `"i"`.
> On variant parents the `Items[]` array doesn't line up and you'll measure your own bug —
> my first attempt reported a false 5/15 mismatch that way.

### 4.3 Option B — PDP scrape (authoritative)

`GET {doc.url}` with the §1.1 UA rules, no cookies. Extract from the inline selector config:

```js
window["sel"] = new IdevSelections({ ...,"EnableInventory":true,
  "Items":[{"q":252,"c":"Backorder","b":null,"i":90740,"s":[]}],
  "SelectedItem":90740, ... });
```

```python
import re, json
m = re.search(r'"Items":\[(.*?)\]', html, re.S)
for it in re.finditer(r'\{"q":(-?\d+),"c":"([^"]*)"[^}]*?"i":(\d+)', m.group(1)):
    qty, cond, item_id = int(it.group(1)), it.group(2), it.group(3)
```
Match `item_id` against `doc["id"][0]`.

#### The exact availability rule (read from their source)

```js
if (CurrentItem.q <= 0) { if (CurrentItem.c != 'Backorder') { /* out of stock */ } }
```

→ A SKU is genuinely unavailable only when **`q <= 0` AND `c != 'Backorder'`**.
If `c == 'Backorder'` it stays purchasable at zero qty. Replicate both conditions.

**Never use `c` alone as an in-stock test** — all 30 sampled read `"Backorder"` at every qty.
Observed values so far: `Backorder`, `OutOfStock`.

#### Cost
**One fetch per SKU — parent PDPs do NOT bundle their children's stock** (verified: 6 of 6
parent PDPs returned exactly one `Items[]` entry). So ~51,245 page loads, ~270 KB each
(≈13 GB), 2+ hours at 4 concurrent.

The PDP also carries two warehouse placeholders (`spanInventoryQty`, `spanInventoryQtyTX` —
main + TX) but both render empty for anonymous visitors.

### 4.4 Recommended strategy

| | Coverage | Accuracy | Cost |
|---|---|---|---|
| **Index (daily/hourly)** | all 51,245 | ~93% | 712 reqs, ~3 min |
| **PDP (watchlist only)** | 1 per fetch | authoritative | ~51k reqs, 2h+ |

Re-pull the index often — it's cheap enough to run hourly, which keeps you inside that 93%.
Reserve PDP scraping for high-value SKUs (turbos, injection pumps) where exact qty matters.

---

## 5. What is NOT available

**Descriptions — DECISION: use `title` as the description for v1.**

The index has no description field. `kitshortdescription` (28.8% fill, median 89 chars) is
fitment/compliance notes, not prose — e.g. the whole value for SSD6.7F-DCR-1 is
*"Emissions Compliance: CARB EO # D756-7"*. Real copy lives only on the PDP
(`<span id="longDescription">`, ~6,000 words for that same SKU), alongside OEM cross-refs,
40+ Q&A entries, YouTube IDs and 9 alternate images.

`title` is a viable substitute and is **100% populated**:

| | |
|---|---|
| Length | min 7 / **median 75** / max 255 chars |
| Distinct | 50,344 of 51,245 (**98.2% unique**) |
| Contain a year or year-range | 33,445 (65%) |
| Contain an engine displacement | 13,664 (27%) |

Typical: `No Limit Fabrication Billet Transmission Pan (Black Finish) - 20-26 Ford PowerStroke 6.7L`
— brand, part, finish, year range and engine in one string.

Two things to handle:
- **HTML-unescape it.** At least one title contains `&amp;`. Run `html.unescape()`.
- **446 titles are under 25 chars** (`"Whistle"`, `"Edge EvoHT2"`, `"Smarty Tuner"`). These are
  too thin to read as a description — consider falling back to
  `"{b} {title} — {d3}"` or flagging them for a later PDP pass.
- `title` is **not unique** (901 collisions) — never use it as a key. Key on `sku`.

If richer copy is needed later, it's a PDP scrape — same fetch as authoritative stock (§4.3),
so **do description + live stock in one pass** rather than scraping the catalog twice.

**Wholesale / dealer pricing.** Not exposed. The DWD B2B site (`/dwd/`) ships the *identical*
`clientId` and index with `isB2BMember` hardcoded `false`; the 36-field schema has only
`price` / `minprice` / `maxprice` / `custom_price_range`. Dealer pricing is applied server-side
after login and is **not reachable from public data** — don't go looking for it there.

---

## 6. Suggested implementation order

1. `fetch_thoroughbred_catalog` — 712 pages, no Query, `MaxPerPage=72`. Assert
   `Pagination.MaxPerPage == 72` and `NofResults == NofPages*72 ± 72`. Upsert by `sku`.
2. `fetch_thoroughbred_vehicles` — 1,545 reqs → `vehicle_id → (year, make, model)`.
3. Explode `vehicleid[]` → fitment rows, **LEFT JOIN** the vehicle map (orphan 1678).
4. Inventory refresh — re-run step 1 on a schedule; clamp the `99999999` sentinel.
5. *(optional)* PDP pass for description + authoritative stock on a watchlist.

### Repo conventions to honour
- Command naming: `src/management/commands/fetch_thoroughbred_feed.py`
  (matches `fetch_motorstate_feed.py`, `fetch_keystone_parts.py`).
- Wrap every `cache.set` / `cache.get` — the cache is optional repo-wide.
- Commands run locally but **write to prod**: check `df` before the first bulk ingest
  (676k fitment rows + 51k products is a real write).
- Do **not** push to `main` — commit locally, Gojko pushes.

### Validation checks worth asserting
- 51,245 distinct ids == 51,245 distinct skus == 51,245 distinct urls
- `it` counts sum to 51,245 (17,856 + 24,994 + 6,754 + 1,641)
- fitment rows ≈ 676,553; ≥99.9% resolve against the 1,251-vehicle map
- every parsed price > 0 and < 30,000
