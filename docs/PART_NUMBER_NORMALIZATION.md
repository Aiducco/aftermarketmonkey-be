# Cross-provider part-number inconsistencies

Research pass, run against production (`master_parts` = 2,990,906 rows, `provider_parts` = 4,128,240 rows).
No data was modified. Analysis scripts live in the session scratchpad (`analyze1.py` … `analyze6.py`).

---

## 1. The problem

`MasterPart` is keyed on `unique_together = ["brand", "part_number"]`, and every provider ingest
resolves its rows with an **exact string match** on `(brand_id, part_number)`:

```sql
SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s
```

`src/integrations/services/master_parts.py` — Keystone `_ingest_keystone_parts_for_mapped_brands:855`,
Turn14 `_ingest_turn14_items_for_mapped_brands:678`, and the same shape for A-Tech / DLG /
Rough Country / Vossen. The only cleaning applied to the incoming value is `.strip()`
(e.g. `master_parts.py:827-831`). Meyer and WheelPros additionally try `(brand_id, sku)` first,
which rescues *some* cases but not this one.

Distributors do **not** agree on how to spell a manufacturer part number. Verified directly
against the raw feed tables — this is their data, not something our ingest introduces:

| distributor | raw column | Brembo rotor | FEL-PRO gasket |
|---|---|---|---|
| A-Tech | `atech_parts.part_number` | `09-5843-11` | `MS96587` |
| Meyer | `meyer_parts.mfg_item_number` | `09.5843.11` | `MS96587` |
| Keystone | `keystone_parts.manufacturer_part_no` | `09584311C02` | `MS 96587` |
| Turn14 | `turn14_items.mfr_part_number` | — | `MS96587` |

Because the lookup is exact, each spelling creates its **own** `MasterPart`. The consequence is
the one that actually hurts: a part carried by five distributors gets split into two or three
master parts, each holding a disjoint subset of `ProviderPart` rows. The catalog shows the same
part twice, and price/availability comparison silently only ever sees a subset of the sources.

Real example (GTIN `00614046867581` on both rows, providers disjoint):

```
mp=37924945  pn='MS 96587'  sku='F10MS96587'   providers=[Keystone]
mp=37697443  pn='MS96587'   sku='FELMS96587'   providers=[A-Tech, Meyer, Turn14]
```

---

## 2. What was found

Grouping `master_parts` by `(brand_id, upper(part_number) with all non-alphanumerics removed)`
and keeping groups with >1 distinct `part_number`:

| tier | what differs | gtin AGREE | gtin CONFLICT | gtin 1-side | no gtin | **total** |
|---|---|---|---|---|---|---|
| T1 | case only | 331 / 11 | 6 / 1 | 18 / 8 | 9 / 2 | **386** |
| T2 | whitespace only | 4673 / 51 | 134 / 3 | 201 / 22 | 17 / 19 | **5,120** |
| T3 | hyphen / dot / space | 27019 / 204 | 1397 / 453 | 1859 / 919 | 634 / 1459 | **33,944** |
| T4 | other punctuation (`+`, `/`, `_`, …) | 3364 / 3 | 61 / 91 | 487 / 13 | 31 / 37 | **4,087** |

Cells are **providers-disjoint / providers-overlap**. Total: **43,537 groups, 88,119 master-part rows.**

Two independent corroborators were computed per group:

- **GTIN agreement.** Normalized to a validated GTIN-14 (see §4 — the raw `gtin` column is itself
  dirty). Agreement across a group is strong evidence the rows are the same physical part.
  **Not available for every provider — see §2b.**
- **Provider disjointness.** If the same distributor appears on *both* sides of a group, that
  distributor is deliberately listing two different parts — a strong signal *not* to merge.
  This one works for every provider.

### Additional classes the string rules do not catch

Anchoring instead on `(brand_id, validated GTIN-14)` — 87,985 groups with >1 master part — surfaces
distinct failure modes:

| class | groups | example |
|---|---|---|
| **B. extra prefix on one side** | 9,494 | ATI `916910-10` (A-Tech) vs `ATI916910-10` (Turn14) |
| **C. extra suffix on one side** | 4,968 | Brembo `09-5843-11` (A-Tech) vs `09584311C02` (Keystone) |
| **D. leading zeros differ** | 982 | DEI `010140` (A-Tech) vs `10140` (Meyer) |
| A. already covered by T1–T4 | 32,105 | |
| F. genuinely different / bad GTIN | 38,177 | needs the GTIN cleanup in §4 before it means anything |

The prefix/suffix classes have no single culprit — every distributor writes the bare form
sometimes and the decorated form other times (A-Tech 3,994 bare vs 2,770 decorated; Turn14
1,624 vs 4,421). So they cannot be fixed with a per-provider rule; they need the GTIN anchor.

## 2b. Not every provider has a GTIN

Four of the eleven feeds have **no UPC/GTIN column at all** — there is nothing to normalize:

| feed | identifier column | rows | filled |
|---|---|---|---|
| A-Tech | `atech_parts.gtin` | 1,433,936 | 1,433,933 |
| Meyer | `meyer_parts.upc` | 956,304 | 775,458 |
| Turn14 | `turn14_items.barcode` | 788,087 | 618,062 |
| Premier | `premier_parts.upc_code` | 686,761 | 686,702 |
| Keystone | `keystone_parts.upc_code` | 145,586 | 137,922 |
| Rough Country | `rough_country_parts.upc` | 15,691 | 15,366 |
| SDC | `sdc_parts.gtin` | 12,336 | 12,309 |
| **TireRack** | **— none —** | 85,843 | 0 |
| **WheelPros** | **— none —** | 68,938 | 0 |
| **DLG** | **— none —** | 5,873 | 0 |
| **Vossen** | **— none —** | 3,255 | 0 |

A `MasterPart` still ends up with a GTIN whenever *any* provider on it supplied one, so the gap
only bites when a whole side of a collision group is made up exclusively of GTIN-less providers.
Counting those "blind" groups:

| tier | sighted (GTIN usable) | **blind** |
|---|---|---|
| T1 case | 380 | 6 |
| T2 whitespace | 5,120 | 0 |
| T3 hyphen/dot | 33,043 | 901 |
| T4 other punct | 3,677 | 410 |
| **total** | **42,220** | **1,317** |

So the GTIN anchor covers **97%** of the collision set. The blind 3% is almost entirely TireRack
(5,271 rows across collision groups; WheelPros 300, DLG 159, Vossen 54) and is small enough to
review by hand — but it cannot be auto-merged on GTIN, and these are exactly the wheel SKUs where
punctuation carries offset meaning.

**Description does not substitute for GTIN.** Of 1,238 blind groups tested, **zero** had matching
normalized descriptions — the feeds describe the same part in incompatible styles:

```
HOOSIER 46735-A7   A-Tech:   'TIRE 315/35-17'
        46735A7    TireRack: 'P315/35R17 HO A7 RADIAL     LL'
```

Fuzzy description matching would be a new, weaker heuristic, not a corroborator of the same class.

### Where the damage concentrates

Top splits, by count of collision groups:

```
5,898  A-Tech      <->  Meyer
5,383  A-Tech      <->  Turn14
3,403  A-Tech      <->  Meyer/Turn14
3,333  A-Tech      <->  Premier
2,484  Meyer       <->  Turn14
```

---

## 3. What must NOT be merged

Stripping *all* punctuation is unsafe. Punctuation is load-bearing in this domain — `+`/`-`
encode wheel offset and piston/bearing sizing:

```
RACELINE   942B-89060+12  gtin=…255294   vs  942B-89060-12  gtin=…255300   (different offsets)
SIMPSON    XBCPXL+5       gtin=…867914   vs  XBCPXL-5       gtin=…867921   (both from Meyer)
BOOSTLINE  NI5886-866S    gtin=…266163   vs  NI5886-866S+   gtin=…266170   (both from Turn14)
WEATHERTECH 4516412       gtin=…331678   vs  45164-1-2      gtin=…204095   (both from Turn14+Premier)
```

Every one of these shows the two negative signals together: **GTINs differ** and **the same
provider sits on both sides**. That is exactly the filter the merge should apply.

Note this also means `merge_flip_flop_master_parts.py` / `merge_double_prefix_master_parts.py`
are narrower than what is needed here — they key on `sku`, not on a normalized part number.

### The sign-conflict test rescues most of T4

T4 is not uniformly unsafe. On wheels, A-Tech strips the `+` that TireRack keeps, and those *are*
the same part — the description confirms the offset:

```
NICHE  M1471880F842    A-Tech    gtin=00194933091072
       M1471880F8+42   TireRack  desc='18X8  5-112 ET42 NICHE ESSEN'   <- ET42 matches
```

The genuinely dangerous case is different: it needs **both** a `+N` and a `-N` spelling present in
the same group. Testing for a sign disagreement at the same alphanumeric offset splits T4 cleanly:

| bucket | gtin AGREE | CONFLICT | 1-side | none | total |
|---|---|---|---|---|---|
| `+` present, **unambiguous** | 1288 / 2 | 29 / 58 | 452 / 5 | 2 / 7 | 1,843 |
| `+` present, **AMBIGUOUS** | 2 / 0 | 0 / 27 | 3 / 1 | 1 / 7 | **41** |
| no `+` at all | 2074 / 1 | 32 / 6 | 34 / 13 | 28 / 25 | 2,213 |

Only **41 groups** are truly ambiguous, and the test lands on precisely the right ones — Simpson
`XBCPXL+5`/`XBCPXL-5`, Raceline `942B-89060+12`/`-12`, Ultra `116-8998M+12`/`-12`. Everything else
in T4 becomes eligible under the normal GTIN + disjointness gate.

Incidental find in that output: Premier ships **mojibake** in part numbers —
`PP-HG-6.4+010ÿ ÿ`, `PP-HG-6.0/20+010ÿÿ` — creating duplicates *within a single provider*. Only
**31 rows** repo-wide have non-printable characters in `part_number` (27 Premier, 2 Turn14, 1 Meyer,
1 TireRack), so this is a one-line strip in ingest, not a project. Leading/trailing whitespace is
already clean (0 rows) thanks to the existing `.strip()`.

---

## 4. Secondary finding: the `gtin` column is dirty

Of 2,667,894 non-null `gtin` values on `master_parts`:

| | count |
|---|---|
| clean | 1,794,887 |
| leading-zero padded (`00787765…` vs `787765…`) | 642,214 |
| placeholder junk (`NA`, `N/A`, `NONE`, …) | 153,987 |
| contains non-digits | 74,151 |
| float artifact — trailing `.0` | 2,655 |

Two more defects that only show up on comparison:

- **Check digit dropped by the feed.** Dynamat `XFOM3U` is `769103920355` from A-Tech and
  `76910392035` from Meyer — the same GTIN minus its check digit. Recomputing the GS1 mod-10
  check digit repairs these.
- **Short junk passing validation by luck.** Walbro has `gtin='3926'`, which satisfies the check
  digit once zero-padded. A minimum of 11 significant digits is required before a GTIN can be
  trusted as a match anchor.

After normalization + repair: 2,513,378 usable, 154,516 unusable, 323,012 null.

This matters beyond dedup — `gtin` is the only cross-distributor identifier we have that is not
a free-text string, so it is worth storing normalized.

---

## 5. Unused matching key: Keystone's second column

`keystone_parts` has both `manufacturer_part_no` (which the ingest uses) and `part_number`
(which it ignores). They differ on **54,194 of 145,586** rows, and **39,600** of those differ
*only by punctuation* — Keystone is already handing us a second spelling for free:

```
vcpn=BRM09584311   manufacturer_part_no='09584311C02'   part_number='09584311'
```

Using it as an additional lookup key would resolve a chunk of the T3 and class-C cases at ingest
time with no fuzzy matching at all.

---

## 6. Proposed fix

Three separable pieces. I'd do them in this order.

### 6a. Add a normalized match key (prevents new duplicates)

Add to `MasterPart`, populated in `save()`/ingest and backfilled by migration:

```python
normalized_part_number = CharField(max_length=255, db_index=True)  # upper, [^A-Z0-9] stripped
normalized_gtin        = CharField(max_length=14, null=True, db_index=True)  # validated GTIN-14
```

Keep `unique_together = ["brand", "part_number"]` as-is — the canonical display value stays
whatever the primary source says. Add a **non-unique** index on
`(brand_id, normalized_part_number)`.

Then change each provider ingest's resolution to a fallback ladder:

1. exact `(brand_id, part_number)` — unchanged, still wins
2. `(brand_id, normalized_gtin)` — **only for the 7 feeds that carry a UPC/GTIN** (§2b)
3. `(brand_id, normalized_part_number)` — **only** when all three guards hold:
   - it resolves to exactly one existing row (ambiguous → skip),
   - that row has no `ProviderPart` for the provider being ingested,
   - no sign conflict in the group (§3).

Step 3's guards are what keep `XBCPXL+5` and `XBCPXL-5` apart: they normalize to the same key so
the lookup is ambiguous, both come from Meyer so the provider guard fires, and the sign test fires
as well. Three independent reasons to skip.

**TireRack, WheelPros, DLG and Vossen skip step 2 entirely** and reach the catalog only through
step 3. That is acceptable — step 3's guards do not depend on GTIN — but it means these four get
*no* second opinion, so their step-3 matches should be logged for spot-checking rather than
trusted silently. TireRack is the one that matters (5,271 rows in collision groups; the other
three total 513).

Normalization must be shared code (one helper module), not copy-pasted per provider — there are
9 ingest functions and they will drift otherwise.

Cheap wins alongside this: feed Keystone's `part_number` in as an extra candidate key (§5), and
strip non-printable characters from `part_number` (31 rows, §3).

### 6b. Backfill script to merge what already exists

New `scripts/merge_normalized_part_number_duplicates.py`, following the shape of the existing
`merge_flip_flop_master_parts.py` (find pairs → dry-run → batch merge, each pair in its own
transaction). Auto-merge only where **both** corroborators are satisfied:

| set | groups |
|---|---|
| T1 + T2, providers disjoint, no GTIN conflict | 5,249 |
| T3, providers disjoint, GTIN agrees | 27,019 |
| T4, providers disjoint, GTIN agrees, no sign conflict | 3,362 |
| **total auto-mergeable** | **35,630** |

The T1–T3 portion alone collapses 64,697 rows into 32,268 (**32,429 deleted**) and reunites
**32,739 provider links** onto a single master part — 15,686 parts gain a 2nd source, 9,806 a 3rd,
4,708 a 4th, 1,972 a 5th. T4 adds roughly 3,400 more.

The remaining **~7,900** groups get exported to CSV for review rather than merged. They split into
two piles worth handling differently:

- **~6,600 sighted** — GTIN conflicts or one-sided GTIN. A conflicting GTIN is a real signal;
  most of these are probably genuinely distinct parts.
- **1,317 blind** (§2b) — no GTIN available on one side, mostly TireRack wheels. These need a
  human or a spec-level comparison (parse `ET`/size out of the description), not a string rule.

Two things the existing merge scripts do **not** handle and this one must:

- `PurchaseOrderLineItem.provider_part` is `on_delete=PROTECT` (`src/models.py:2508`). Deleting the
  losing `ProviderPart` on a per-provider conflict will raise `ProtectedError`. Only 37 line items
  exist today, but the script must repoint them rather than blow up mid-batch.
- Beyond `provider_parts`, these also reference the rows and need merging/dedup:
  `master_part_data` (OneToOne — needs a field-level merge), `master_part_fitments`
  (`unique_together` — needs dedup on reassign), `part_request_audit`,
  `provider_part_company_pricing`, `provider_part_inventory`, `provider_part_kit_components`.

Meilisearch documents for the deleted ids need removal — `SEARCHABLE_ATTRIBUTES` includes
`part_number` (`src/search/meilisearch_client.py:43`).

### 6c. GTIN normalization

Backfill `normalized_gtin` using: strip `.0`, digits only, drop placeholders, require ≥11
significant digits, zero-pad to 14, validate GS1 check digit, and if that fails, recompute the
check digit from the body and re-validate. Leave the raw `gtin` untouched.

### Not proposed

- Merging on prefix/suffix similarity alone (classes B/C, ~14,500 groups). Without a GTIN anchor
  the false-positive rate is unacceptable — `JE PISTONS 361310` vs `361310-6` and
  `DIABLO PKITJGCV817` vs `PKITJGCV817-I3` are indistinguishable from real variants by string
  shape alone. These should ride on the GTIN path in 6a, or wait for a review queue.
- Changing `unique_together` to use the normalized key. It would reject legitimately distinct
  parts (`XBCPXL+5` / `XBCPXL-5`) at write time.
- Auto-merging the 1,317 GTIN-blind groups (§2b) on string shape alone. These are predominantly
  TireRack wheels, where the punctuation being normalized away *is* the offset — the one place a
  false merge produces a wrong physical part on a customer's car.

---

## 7. Open questions

1. **Canonical spelling.** When merging `MS 96587` and `MS96587`, which becomes the displayed
   `part_number`? Proposal: keep the row with more `ProviderPart` links (matches the existing
   scripts' `_pick_canonical`), tie-break on the manufacturer's own convention if known.
2. **The 11,269 review-queue groups** — CSV export for manual review, or a small admin UI?
3. **Should 6b run before or after 6a ships?** Running the backfill first means the next sync
   re-creates a portion of what was just merged.
4. **TireRack has no GTIN and no second opinion.** Options: accept step-3 matching with logging,
   ask TireRack whether a UPC field is available on their feed, or parse wheel specs
   (`ET`, diameter, bolt pattern) out of their description as a substitute corroborator. Their
   descriptions are consistently formatted (`20X9  5-150 ET0   RACE VECTOR`), so parsing is viable
   if it's worth the effort.
