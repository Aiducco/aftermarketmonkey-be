# Tire API — Frontend Specification

Two endpoints, one contract. Part 1 is the tire spec card on a part detail page; part 2 is the
filter rail on tire search. They share a vocabulary on purpose — a tread category, a load range or
a vehicle class is labelled from the same table in both places, so a chip in the rail and a row on
the card can never disagree.

| | endpoint | payload |
|---|---|---|
| **Part 1** | `GET /api/parts/{id}/` | `data.tire_specs` |
| **Part 2** | `POST /api/search` | `data.facets` |

Three rules govern both halves and are worth reading before either:

1. **Render the label, filter on the code.** Every coded value ships with a resolved label. `104`,
   `T`, `XL` and `AT` mean nothing to a buyer.
2. **`null` / absent means "we don't know" — never "no".** This is load-bearing on `is_3pmsf`, a
   certification with legal weight, and on a facet that is omitted rather than sent empty.
3. **The server owns presentation order and visibility.** Don't hardcode a facet list, a label, or
   a sort — all of them are data and change without a client deploy.

---

# Part 1 — Detail panel

**GET** `/api/parts/{id}/` → `data.tire_specs`

`tire_specs` is **`null` for anything that is not a tire** (check `data.product_type == "tire"`,
but the null is authoritative — a tire whose enrichment hasn't run yet also returns null rather
than a card full of holes). When it is an object, **every key below is always present.** Render
from a fixed template and blank out nulls; never probe for keys.

---

## The two rules the payload is built around

**1. A code alone is not information.** Every coded field ships as a `<field>` / `<field>_label`
pair. **Render the label, filter on the code.** `104` and `T` and `XL` mean nothing to a buyer;
`1,984 lb`, `118 mph` and `Extra load` do.

**2. `null` means "we don't know". It never means "no".** This matters most on `is_3pmsf`, which
is a severe-snow certification with legal weight in some jurisdictions:

| value | render | meaning |
|---|---|---|
| `true` | ✓ badge | rated |
| `false` | ✗ / "Not rated" | checked, not rated |
| `null` | — (em dash) or omit the row | nobody has told us |

Never coerce `null` to `false` and never show a "not rated" state for it.

---

## Full field reference

Types are what `simplejson` emits: decimals arrive as JSON numbers, not strings.

### Identity

| field | type | notes |
|---|---|---|
| `size_display` | string | Always present, never empty. The headline: `215/70R16`. |
| `model_name` | string \| null | `Terra Grappler G3`. |
| `sub_model` | string \| null | OE / Front / Rear variant. Usually null. |

### Size

| field | type | notes |
|---|---|---|
| `notation` | string \| null | `metric` / `flotation` / `numeric`. **Internal — do not render.** |
| `service_type` | string \| null | Sidewall stamp: `P`, `LT`, `ST`, `T`, `C`. **`null` is normal**, see "Gotchas". |
| `service_type_label` | string \| null | `Light truck`, `Special trailer`, `Temporary spare`… |
| `section_width_mm` | int \| null | |
| `section_width_in` | float \| null | Render together: `215 mm (8.46")`. |
| `aspect_ratio` | int \| null | Null on flotation/numeric sizes — those have no aspect ratio. |
| `construction` | string \| null | `R`, `ZR`, `D`, `B`. |
| `construction_label` | string \| null | `Radial`, `ZR radial`, `Bias ply`, `Belted bias`. |
| `overall_diameter_in` | float | |
| `overall_diameter_is_nominal` | bool | **Internal.** When `true` the diameter is a nominal figure (numeric sizes carry no aspect ratio) — don't print it to a tenth as if it were measured. |
| `rim_diameter_in` | float | |
| `revolutions_per_mile` | float \| null | See "Gotchas" before labelling it. |

### Ratings

| field | type | notes |
|---|---|---|
| `load_index` | int \| null | The code. Show `load_index` + `max_load_lb` together. |
| `load_index_dual` | int \| null | Dual-wheel rating; truck tires only. Null on most tires. |
| `max_load_lb` | int \| null | Per tire. |
| `set_of_four_max_load_lb` | int \| null | Per set. **Null on motorcycle tires by design** (a bike runs two) — don't compute it yourself. |
| `speed_rating` | string \| null | `T`, `H`, `Y`… |
| `max_speed_mph` | int \| null | Render as `T — 118 mph`. |
| `load_range` | string \| null | `SL`, `XL`, `RF`, or a letter `C`–`N`. |
| `load_range_label` | string \| null | `Extra load`, `Load range E`… **Always render `XL — Extra load`, never `XL (4 ply)`.** |
| `ply_rating` | int \| null | Bias-strength *equivalence*, never a count of physical layers. **Null on XL/SL is an answer, not a gap** — passenger load ranges have no ply equivalent. Render `load_range_label` there. |
| `max_psi` | int \| null | Per tire, from the product's own data. Never derived from load range. |
| `tread_depth_32nds` | number \| null | Fractional (`12.70`). Render as `12.7/32"`. |
| `utqg_treadwear` | int \| null | |
| `utqg_traction` | string \| null | `AA`/`A`/`B`/`C`. |
| `utqg_temperature` | string \| null | `A`/`B`/`C`. Compose the three as `600 A B`. |

### Type & capability

| field | type | notes |
|---|---|---|
| `tread_category` | string \| null | Code — filtering only. |
| `tread_category_label` | string \| null | `All Terrain`. **This is the one to render.** |
| `season_category` | string \| null | `ALL_SEASON` / `ALL_WEATHER` / `SUMMER` / `WINTER`. |
| `season_category_label` | string \| null | `All Weather`. A **second, independent axis** — a tire is All Terrain *and* All Weather. Render both; neither replaces the other. |
| `vehicle_class` | string \| null | `passenger`, `light_truck`, `trailer`, `commercial`, `motorcycle`, `atv_utv`. |
| `vehicle_class_label` | string \| null | `Light truck`. The intended *application*, not the sidewall stamp. |
| `tier` | string \| null | `budget` / `mid` / `premium` / `flagship`. |
| `noise_level` | string \| null | `quiet` / `moderate` / `loud`. |
| `use_case_tags` | string[] | **Do not render yet.** The vocabulary is unstable and currently duplicates `tread_category_label` + `vehicle_class_label` on the same card (`["on/off road", "pickup", "SUV"]` next to "All Terrain / Light truck" reads as a data bug). Always an array, never null. |

### Tri-state flags — `true` / `false` / `null`

| field | notes |
|---|---|
| `is_3pmsf` | Three-Peak Mountain Snowflake. **Treat `null` strictly as unknown** — see rule 2. |
| `is_ms` | M+S marking. |
| `is_run_flat` | Buyers filter on it. |
| `is_studdable` | Matters in winter markets. |
| `is_tubeless` | Usually true; load-bearing on trailer and powersports tires. |
| `has_reinforced_sidewall` | |

### Construction & appearance

| field | type | notes |
|---|---|---|
| `sidewall_style` | string \| null | `Blackwall`, `Outlined White Lettering`, `Raised White Lettering`. Already display-ready — no mapping needed. |
| `tread_design` | string \| null | `Symmetrical` / `Asymmetrical` / `Directional`. Cost-of-ownership fact: directional and asymmetrical tires can't be rotated freely. |
| `commercial_position` | string \| null | `Steer` / `Drive` / `Trailer` / `All Position`. Commercial tires only; null elsewhere. |
| `oe_marking` | string \| null | `N0 - Porsche`, `MO - Mercedes-Benz`. Comma-separated when a tire carries more than one. Worth surfacing prominently — it's the difference between a tire that fits the car and one the manufacturer approved for it. |

### Ownership

| field | type | notes |
|---|---|---|
| `mileage_warranty_miles` | int \| null | Render as `70,000 mi`. One of the strongest purchase factors — give it real weight in the layout. |
| `tire_weight_lb` | float \| null | |

### Fitment

| field | type | notes |
|---|---|---|
| `rim_width_min_in` | float \| null | Render as a range: `5.55" – 7.05"`. |
| `rim_width_max_in` | float \| null | |
| `equivalent_sizes_count` | int \| null | How many **other** sizes we carry stand the same height (±3%) on the same rim diameter. `0` is a real answer; `null` means it wasn't counted. Present it as an action — "36 other sizes fit this wheel" — not as a spec row. There is no list endpoint for these yet. |

### Provenance

| field | type | notes |
|---|---|---|
| `spec_source` | string \| null | `parser` / `simpletire` / `tdg`. |
| `spec_source_label` | string \| null | `Parsed from the distributor title` / `SimpleTire catalog` / `TDG catalog`. A quality signal: catalog-sourced specs are measured facts; parser-sourced ones are decoded from a distributor's title. A small "verified" mark on `simpletire`/`tdg` is justified. |
| `simpletire_match_tier` | int \| null | How the catalog row was matched: `1` = brand + part number, `2` = part number + agreeing size, `3` = brand + model + size. **Lower is stronger.** Internal/dashboard-grade. |
| `tdg_match_tier` | int \| null | Same scale. |
| `size_disputed` | bool | **Internal.** Parser and model disagreed. Caveat sizing claims when true; don't show the flag itself. |
| `enriched_at` | ISO 8601 string \| null | **Internal.** |

---

## What not to render

`notation`, `overall_diameter_is_nominal`, `size_disputed`, `enriched_at`, both `*_match_tier`
fields — kept in the API for debugging and dashboards. `use_case_tags` — until the vocabulary is
fixed.

---

## Gotchas that will look like bugs and are not

**`service_type: null` on a tire whose `vehicle_class` is `light_truck`.** These are different
facts, not a contradiction. `service_type` is the sidewall stamp; a Euro-metric size carries no
prefix whatever it's built for. `vehicle_class` is the application the catalog assigns.
`215/70R16 104T XL` at 50 psi with 12.7/32" tread, sold as a light truck tire, is a normal
Euro-metric XL — verified against the manufacturer catalog. Don't render "P-metric" as a fallback
when `service_type` is null; render nothing.

**`revolutions_per_mile` is the unloaded geometric figure**, `63360 / (π × overall_diameter_in)`.
A manufacturer's published revs/mile is measured under load and runs roughly 3% higher. Do not
label it "revolutions per mile (manufacturer)" or place it beside a spec-sheet figure as if they
were the same number. What it *is* exact for is the ratio between two tires — which is precisely
how far a speedometer reads off after a size change. Use it comparatively.

**`ply_rating: null` with `load_range: "XL"`** is correct, not missing data. See the table above.

**`equivalent_sizes_count` counts sizes, not products**, and excludes the tire's own size.

---

## Breaking changes from the previous payload

If the panel already renders the old shape, three things changed:

1. **Nullable strings are now `null`, never `""`.** `service_type`, `model_name`, `sub_model`,
   `construction`, `speed_rating`, `load_range`, `utqg_*`, `tread_category`, `vehicle_class`,
   `tier`, `noise_level`. `size_display` is the exception — still always a non-empty string.
2. **The tri-state flags are now always present as `null`** instead of being omitted from the
   object. Code that did `if ('is_3pmsf' in specs)` to mean "known" must become
   `if (specs.is_3pmsf !== null)`.
3. **20 new keys** (39 guaranteed keys → 59) — everything in Construction & appearance,
   Ownership and Provenance, plus `season_category(_label)`, `revolutions_per_mile`,
   `equivalent_sizes_count`, and the six tri-state flags now that they are always present.

---

## Live sample response

Real output for Nitto Terra Grappler G3, `215/70R16` (MasterPart 248606784) — a fully enriched,
catalog-matched tire. This is the maximal case; a parser-only tire has far more nulls.

```json
{
  "size_display": "215/70R16",
  "model_name": "Terra Grappler G3",
  "sub_model": null,
  "notation": "metric",
  "service_type": null,
  "service_type_label": null,
  "section_width_mm": 215,
  "section_width_in": 8.46,
  "aspect_ratio": 70,
  "construction": "R",
  "construction_label": "Radial",
  "overall_diameter_in": 27.9,
  "overall_diameter_is_nominal": false,
  "rim_diameter_in": 16.0,
  "revolutions_per_mile": 722.9,
  "load_index": 104,
  "load_index_dual": null,
  "max_load_lb": 1984,
  "set_of_four_max_load_lb": 7936,
  "speed_rating": "T",
  "max_speed_mph": 118,
  "load_range": "XL",
  "load_range_label": "Extra load",
  "ply_rating": null,
  "max_psi": 50,
  "tread_depth_32nds": 12.70,
  "utqg_treadwear": 600,
  "utqg_traction": "A",
  "utqg_temperature": "B",
  "tread_category": "AT",
  "tread_category_label": "All Terrain",
  "season_category": "ALL_WEATHER",
  "season_category_label": "All Weather",
  "vehicle_class": "light_truck",
  "vehicle_class_label": "Light truck",
  "tier": "mid",
  "noise_level": "moderate",
  "use_case_tags": [
    "on/off road",
    "pickup",
    "SUV"
  ],
  "sidewall_style": "Blackwall",
  "tread_design": "Symmetrical",
  "commercial_position": null,
  "oe_marking": null,
  "mileage_warranty_miles": 70000,
  "tire_weight_lb": 30.42,
  "rim_width_min_in": 5.55,
  "rim_width_max_in": 7.05,
  "equivalent_sizes_count": 36,
  "spec_source": "simpletire",
  "spec_source_label": "SimpleTire catalog",
  "simpletire_match_tier": 3,
  "tdg_match_tier": null,
  "size_disputed": false,
  "enriched_at": "2026-08-25T19:28:06.899086",
  "is_3pmsf": null,
  "is_ms": true,
  "is_run_flat": null,
  "is_studdable": null,
  "is_tubeless": null,
  "has_reinforced_sidewall": null
}
```

---

# Part 2 — Search facets

**POST** `/api/search` → `data.facets`

The rail is **server-owned**. Order, labels, widgets, value order and *whether a facet appears at
all* come from the server on every response. Render `facets` top to bottom exactly as sent. Do not
hardcode a facet list, a label, or a sort — all four are data (`facet_config`) and change without
a client deploy.

---

## Response shape

```json
"facets": [
  {
    "field": "vehicle_class",
    "label": "Vehicle type",
    "widget": "multiselect",
    "unit": null,
    "collapse_after": 8,
    "values": [
      { "value": "light_truck", "label": "Light truck", "count": 12043 },
      { "value": "passenger",   "label": "Passenger",   "count": 8811 }
    ]
  },
  {
    "field": "overall_diameter_in",
    "label": "Overall diameter",
    "widget": "range",
    "unit": "in",
    "collapse_after": 8,
    "values": [ { "value": 31.6, "label": "31.6", "count": 42 } ],
    "stats": { "min": 29.1, "max": 34.8 }
  }
]
```

| key | notes |
|---|---|
| `field` | The filter key. Send it back verbatim in `filters`. |
| `label` | Render this. Never derive a label from `field`. |
| `widget` | `multiselect` \| `range` \| `toggle`. |
| `unit` | `in`, `mm`, `%` or null. Suffix, not part of the label. |
| `collapse_after` | Show this many values, then a "show more" control. |
| `values` | Already ordered — **do not re-sort**. See "Value order". |
| `stats` | **`range` widgets only**: `{min, max}` for the slider bounds. May be null if the engine sent no stats and no value parsed as a number. |

`values[].value` arrives **in the type the index stores** — number for `rim_diameter_in`,
`section_width_mm`, `aspect_ratio`, `overall_diameter_in`, `distributor_ids`; boolean for
`in_stock` and `is_3pmsf`; string for everything else. Send it back **unchanged**. Do not
stringify it: `rim_diameter_in = "18"` is a string comparison against a numeric field in
Meilisearch and matches nothing. (This is a change — these used to arrive as strings.)

`values[].label` is always a string, so use `label` for display and `value` only as the filter
payload and the React key.

---

## The rail

Order below is the order the server sends. The **Always** group renders whenever the result set
has any values for it; the **Conditional** group appears only when its rule is met — the server
applies the rule and omits the facet entirely, so there is nothing to implement client-side
beyond "render what arrives".

### Always shown

| # | field | widget | label | notes |
|---|---|---|---|---|
| 1 | `vehicle_class` | multiselect | Vehicle type | Highest-value split: car / light truck / trailer / commercial / motorcycle / ATV-UTV. Labels come from the same table the part detail panel uses, so the two can't disagree. |
| 2 | `tread_category` | multiselect | Tread type | Labels from `tread_category.label`, ordered by its `sort_order` (terrain first). **Turf lives here**, as a tread category — there is no turf vehicle class. |
| 3 | `rim_diameter_in` | multiselect | Wheel size | Discrete values, ordered numerically ascending. Unit `in`. |
| 4 | `section_width_mm` | multiselect | Section width | Numeric ascending. Unit `mm`. |
| 5 | `aspect_ratio` | multiselect | Aspect ratio | Numeric ascending. Unit `%`. |
| 6 | `load_range` | multiselect | Load range | Labelled `E — 10 ply` for LT ranges and `XL — Extra load` for passenger ones, from `load_range`'s own table. Never render "XL (4 ply)" — XL has no ply equivalent. |
| 7 | `brand_name` | multiselect | Brand | Ordered by count. `collapse_after: 8`. |
| 8 | `in_stock` | toggle | In stock | **One-way.** On → send `"in_stock": true`. Off → **remove the key**; never send `false`. `false` would mean "show me only what is out of stock". |
| 9 | `distributor_ids` | multiselect | Distributor | **Changed from `distributor_names`.** Values are provider ids (numbers), labels are provider names. Ids are stable across a rename; names are not. |

### Conditional

| # | field | widget | appears when |
|---|---|---|---|
| 10 | `overall_diameter_in` | range | `rim_diameter_in` is already in `filters`. Unscoped it spans 14.7″–37.4″ (lawn tires to 37s) and means nothing. Use `stats` for the slider bounds. |
| 11 | `service_type` | multiselect | the result set has more than one distinct value. |
| 12 | `speed_rating` | multiselect | more than one distinct value. Ordered by the speed-rating table, **never alphabetically** — H is 130 mph and sits between U and V. Labels carry the speed (`H — 130 mph`). |
| 13 | `is_3pmsf` | toggle | at least one row in the result set is actually `true`. Today no tire has a known-true 3PMSF, so **expect this facet to be absent**; it will start appearing on its own when the data lands. |
| 14 | `tier` | multiselect | the result set has any tier at all. `Budget` / `Mid` / `Premium` / `Flagship`. |
| 15 | `oe_marking` | multiselect | any row carries an OE approval. ~3,986 tires do. Values are as published (`N0 - Porsche`, `MO - Mercedes-Benz`); a tire with two is indexed as two values, so each is separately selectable. |

---

## Sending filters back

`filters` is a flat object keyed by `field`:

```json
{
  "q": "",
  "filters": {
    "vehicle_class": ["light_truck"],
    "rim_diameter_in": 17,
    "distributor_ids": [1, 4],
    "overall_diameter_in": { "min": 31, "max": 34 },
    "in_stock": true
  }
}
```

- **multiselect** → an array. Multiple values in one facet are OR'd; different facets are AND'd.
- **range** → `{"min": n, "max": n}`; either bound may be omitted.
- **toggle** → `true`, or drop the key. Never `false`.
- An unknown key is a **400**, not a silent drop.

**Once you send `filters`, `q` is no longer parsed** — it is used verbatim as free text. That is
deliberate: re-reading "mud terrain" out of the query box every time the user removes the Mud
terrain chip is how a filter panel becomes unusable. So the `filters` you send must be the
complete state, not a delta.

---

## Empty and zero-result behaviour

- Values whose index value is empty (`""`) are **never sent**. 38,199 of 47,655 tires have no
  service type; a blank checkbox meaning "we don't know" is not a filter.
- A **zero-result** search still returns a populated rail: the server re-fetches facet counts for
  the same text with the filters dropped, so the user can see what to relax. Render the rail
  normally on an empty result set.
- A facet the result set can't populate is **omitted**, not sent empty. Don't render placeholders
  for facets you expect but don't receive.

---

## Chips

`interpretation.chips` describes the applied filters and reuses the same labels, so a chip and its
facet can't disagree. Each chip carries `field`, `value`, `label`, `display`, `unit`, `removable`.
Removing a chip means resending `filters` without that key.
