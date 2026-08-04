# Part-number normalization — runbook

How to re-check for cross-provider duplicate `MasterPart` rows, what the numbers should look
like, and how to merge them. Background and the original survey: [PART_NUMBER_NORMALIZATION.md](PART_NUMBER_NORMALIZATION.md).

**Run this quarterly, and after onboarding any new distributor.** A new feed brings a new house
style for spelling manufacturer part numbers, which is exactly what creates these duplicates.

---

## 1. The moving parts

| file | what it does |
|---|---|
| `src/integrations/utils/part_numbers.py` | Pure functions: `normalize_part_number`, `normalize_gtin`, `has_sign_conflict`, `classify_tier`. No DB access. |
| `src/integrations/utils/master_part_matching.py` | `resolve_normalized_matches` — the guarded lookup used by the ingests. |
| `src/integrations/services/master_parts.py` | 9 provider ingests call `mp_matching.extend_with_normalized_matches(...)` right after their exact-match lookup. |
| `scripts/merge_normalized_part_number_duplicates.py` | Finds and merges duplicates that already exist. |
| `migrations/0143_master_parts_normalized_part_number_index.py` | Expression index that makes the normalized lookup fast. |

**The index expression and `_NORMALIZED_PART_NUMBER_SQL` must stay character-for-character
identical.** If they drift, Postgres silently stops using the index and every ingest batch turns
into a sequential scan over ~3M rows. To check:

```bash
psql "$DATABASE_URL" -c "EXPLAIN SELECT id FROM master_parts WHERE (brand_id, upper(regexp_replace(part_number, '[^A-Za-z0-9]', '', 'g'))) IN ((1,'ABC123'))"
```

Expect `Index Scan using master_parts_normalized_part_number_idx`. A `Seq Scan` means they drifted.

---

## 2. Which providers are wired

All ingests **except Turn14** resolve on the normalized key. Turn14 is deliberately left as the
seed provider: it bulk-upserts on `(brand, part_number)` without a new-vs-existing split, it is
the largest and cleanest catalog, and restructuring it is the riskiest change available for the
smallest gain.

Wired: Keystone, Meyer, A-Tech, Rough Country, DLG, Vossen, WheelPros, TireRack, Premier.

**Four feeds carry no UPC/GTIN column at all** — TireRack, WheelPros, DLG, Vossen. For those,
only case/whitespace differences can ever clear the guards; punctuation differences require a
matching barcode and will correctly fall through to creating a new row. If a new distributor
ships no barcode, add its kind to `GTIN_LESS_PROVIDER_KINDS` in **both**
`master_part_matching.py` and the merge script.

---

## 3. Re-checking for duplicates

```bash
python manage.py shell
```
```python
import runpy
ns = runpy.run_path("scripts/merge_normalized_part_number_duplicates.py", run_name="merge_loader")
report = ns["find_merge_candidates"]()
ns["print_summary"](report)
```

Read-only, takes a few minutes. Baseline as of the 2026-08-04 survey (2,990,906 master parts):

| | groups |
|---|---|
| auto-mergeable | 35,595 |
| needs review | 7,942 |
| provider links that would be reunited | 36,961 |

**After the first merge runs, auto-mergeable should drop to near zero and stay there.** If it
climbs back into the thousands, the ingest guards are not doing their job — check the
`[MASTER_PART_MATCHING]` log lines (see §6) before re-merging.

The `needs review` count should stay roughly flat. It is not a backlog to burn down; most of
those groups are genuinely different parts.

---

## 4. Merging

```python
ns["merge_batch"](report.auto_mergeable, dry_run=True)     # preview — prints every action
ns["merge_batch"](report.auto_mergeable, dry_run=False)    # apply
```

Each group runs in its own transaction, so one failure cannot roll back the rest. Everything is
printed. After merging, **reindex Meilisearch** — deleted master part ids remain in the search
index otherwise (`part_number` is a searchable attribute, see `src/search/meilisearch_client.py`).

For the review pile:

```python
ns["export_review_csv"](report, "/tmp/part_number_review.csv")
```

One row per MasterPart, grouped, with the reason it was held back and the evidence (providers,
normalized GTIN, tier). When you have decided a group really is one part:

```python
ns["merge_ids"]([37924945, 37697443], dry_run=False)
```

`merge_ids` applies **no** safety gates beyond "same brand" — only pass ids you have looked at.

---

## 5. Test cases — before / after

Pulled from production on 2026-08-04. Use these to verify a merge run did what it should.

### Should merge

| brand | before | after |
|---|---|---|
| BILSTEIN | `24-238526` [37124996] *(Premier, Keystone, Meyer, TireRack, Turn14, WheelPros)*<br>`24238526` [74895731] *(A-Tech)* | `24-238526` [37124996] — **7 providers** |
| YUKON GEAR & AXLE | `AK 1559` [37189412] *(Keystone, Turn14)*<br>`AK1559` [39342004] *(Premier, Meyer)* | `AK 1559` [37189412] — 4 providers |
| RACELINE | `953BZ-89088+18` [37550844] *(Keystone, Meyer, TireRack, Turn14)*<br>`953BZ8908818` [74998916] *(A-Tech)*<br>`953BZ-8908818` [242223764] *(Premier)* | `953BZ-89088+18` [37550844] — 6 providers |
| ARB | `100/117KIT1` [37393440] *(Keystone, Meyer, Turn14)*<br>`100-117KIT1` [74126951] *(A-Tech)*<br>`100117KIT1` [242137705] *(Premier)* | `100/117KIT1` [37393440] — 5 providers |
| FEL-PRO | `MS 96587` [37924945] *(Keystone)*<br>`MS96587` [37697443] *(A-Tech, Meyer, Turn14)* | `MS96587` [37697443] — 4 providers |

### Must NOT merge

| brand | rows | why |
|---|---|---|
| NOMAD | `N501SA-78551+25` [37552527] *(Turn14)*<br>`N501SA-78551-25` [365906293] *(TireRack)* | `+/-` sign conflict — different offsets |
| ULTRA | `116-2983M-18` [74752121] *(A-Tech)*<br>`116-2983M+18` [365903535] *(TireRack)* | `+/-` sign conflict |
| SIMPSON RACING | `XBCPXL+5` [39198993]<br>`XBCPXL-5` [39198994] | both from Meyer — that distributor lists them as separate parts |
| BRIAN CROWER | `BC3831-N` [37102294]<br>`BC3831N` [37102295] | both from Turn14 |
| KING ENGINE BEARINGS | `CR4033XP0.25` [37104475] *(Meyer, Turn14)*<br>`CR4033XP025` [74182561] *(A-Tech)* | GTIN conflict — `0.25` is a size |
| KONI | `26 1209Sport` [37134122] *(Turn14)*<br>`261209SPORT` [74110687] *(A-Tech)*<br>`26-1209SPORT` [365930487] *(TireRack)* | TireRack side has no barcode, so the punctuation match has no second opinion |

Verify a specific case:

```sql
SELECT mp.id, mp.part_number, mp.gtin, string_agg(p.name, ',' ORDER BY p.name) AS providers
FROM master_parts mp
LEFT JOIN provider_parts pp ON pp.master_part_id = mp.id
LEFT JOIN providers p ON p.id = pp.provider_id
WHERE mp.brand_id = (SELECT id FROM brands WHERE name = 'BILSTEIN')
  AND upper(regexp_replace(mp.part_number, '[^A-Za-z0-9]', '', 'g')) = '24238526'
GROUP BY mp.id, mp.part_number, mp.gtin;
```

Before: 2 rows. After: 1 row with all 7 providers.

---

## 6. Watching the ingests

Each sync logs one line per batch:

```
[MASTER_PART_MATCHING] provider_id=4: 812 feed rows -> 37 matched to existing master parts; skipped {'ambiguous': 12, 'gtin_missing_or_mismatched': 704, ...}
```

Skip reasons and what they mean:

| reason | meaning |
|---|---|
| `no_candidate` | Genuinely new part. Normal, and the bulk of the volume. |
| `ambiguous` | >1 existing row shares the normalized key. Expected before the first merge; should fall to near zero after. |
| `provider_already_on_candidate` | This distributor already lists the candidate separately — deliberately not merged. |
| `gtin_missing_or_mismatched` | Punctuation-tier match without a matching barcode. Normal and high for barcode-less feeds. |
| `sign_conflict` | `+N` vs `-N`. Should be rare; a spike means a feed changed its offset convention. |
| `candidate_has_no_barcode_source` | Candidate is backed only by barcode-less providers. |

**Warning signs:** `ambiguous` staying high after a merge run (guards are refusing work the merge
should have cleaned up), or `sign_conflict` rising (a feed changed convention).

---

## 7. Things deliberately not handled

- **Prefix/suffix differences** — ATI `916910-10` vs `ATI916910-10`, Brembo `09-5843-11` vs
  `09584311C02`. ~14,500 groups. String shape alone cannot separate these from real variants
  (`JE PISTONS 361310` vs `361310-6`), so they are out of scope. They resolve only if a shared
  GTIN puts them together.
- **The `gtin` column is not normalized in place.** `normalize_gtin` runs at comparison time.
  642k values are zero-padded differently, 154k are placeholders (`NA`), 2.6k have a `.0` float
  artifact, and some feeds drop the check digit entirely. If a future feature needs barcode
  lookup, normalize on write rather than reusing this.
- **Turn14** — see §2.
- **`PartRequestAudit.master_part_id`** is a plain integer, not an FK. Rows there keep pointing at
  merged-away ids. It is an analytics log; left alone on purpose.

---

## 8. Onboarding a new distributor — checklist

1. Does its feed have a UPC/GTIN column? If **no**, add its kind to `GTIN_LESS_PROVIDER_KINDS` in
   `master_part_matching.py` *and* the merge script.
2. Add the `extend_with_normalized_matches(...)` call right after the ingest's exact-match lookup,
   and make sure `brand_part_to_master` is seeded via `master_part_stubs_for(existing_by_key)` —
   otherwise rows matched on formatting get no `ProviderPart` and the match silently does nothing.
3. Run the sync, then `find_merge_candidates()` and check what the new feed collided with.
4. Spot-check 10 rows from the review CSV for that provider before merging anything.
