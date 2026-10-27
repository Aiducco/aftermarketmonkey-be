# Part-number aliases in the Meilisearch `parts` index — implementation brief

Copy `pace_parts.part_search` onto `master_parts`, index it as a searchable array, and disable
typo tolerance on the identifier attributes. Three steps, in that order.

Background on part-number spelling across providers:
[PART_NUMBER_NORMALIZATION.md](PART_NUMBER_NORMALIZATION.md) and its
[runbook](PART_NUMBER_NORMALIZATION_RUNBOOK.md). Read §1 of the runbook before writing the join —
the expression index it describes is what makes it fast, and it is easy to break.

---

## Why

`part_search` is the alias array AAM/PACE maintains so their storefronts can find a part by any
spelling a counter person types. For Covercraft `80516-00-23` it holds `805160023`,
`COV805160023`, `COV-805160023`, `COV80516-00-23`, `COV-80516-00-23`.

Our index can't match any of those. Measured on a throwaway `getmeili/meilisearch:v1.11`
container, with a document carrying only `part_number`:

| query | stored as `80516-00-23` |
|---|---|
| `80516-00-23`, `80516 00 23`, `80516` | ✅ |
| `805160023` | ❌ |
| `COV805160023` | ❌ |

Meilisearch splits on punctuation on both sides, so punctuation *style* is already free — a query
of `09-5843-11` finds a stored `09.5843.11`. What fails is the glued spelling, once a number has
3+ segments (a two-segment query Meilisearch re-concatenates itself, so `24143936` does find
`24-143936`). **14.1% of `master_parts` have a 3+ segment part number** — ~915,000 of 6.49M — and
essentially none are reachable glued. No line-code spelling is reachable at all.

Coverage, measured from our side on a sample of `master_parts`: **44% are in a PACE-mapped brand,
and 38% match a `pace_parts` row carrying `part_search`** — about 2.4M parts would gain aliases,
averaging 3.45 each.

---

## Step 1 — copy `part_search` onto `master_parts`

### Schema

One new field on `MasterPart`, not a side table — the reindex already loads `MasterPart` rows, so
a column needs no extra query and no new bulk-map helper:

```python
# src/models.py, on MasterPart
# Part-number aliases from pace_parts.part_search -- every spelling PACE's own storefronts
# accept for this part: the glued number and the jobber line-code forms (COV805160023,
# COV-80516-00-23). Searchable in Meilisearch; see docs/MEILISEARCH_PART_ALIASES_PLAN.md.
part_aliases = django_db_models.JSONField(default=list, blank=True)
part_aliases_source = django_db_models.CharField(max_length=16, blank=True, default="")
```

`part_aliases_source` is `"pace"` for now — provenance, so a later tier can be added or reverted
on its own, the same reasoning as `MasterPart.product_type_source`. Migration is `0229_*`; adding
a JSONB column with a constant default is metadata-only on PG 11+, so it won't rewrite the 3.4GB
table.

### The join

New management command `backfill_master_part_aliases_from_pace`, with `--dry-run`, `--brand` and
`--limit`. There is already an untracked `backfill_master_part_data_from_pace.py` doing a PACE →
`master_parts` backfill; **read it first and follow its shape** rather than inventing a second one.

Brand mapping already exists — `brand_pace_brand_mapping` → `pace_brands.aaia_brand_code`. Join:

```
master_parts.brand_id
  → brand_pace_brand_mapping.brand_id → pace_brands.aaia_brand_code
  → pace_parts (aaia_brand_code, normalized part)
```

Part-number side of the join:

```sql
upper(regexp_replace(part_number, '[^A-Za-z0-9]', '', 'g'))
```

**This must stay character-for-character identical to `master_parts_normalized_part_number_idx`
and to `_NORMALIZED_PART_NUMBER_SQL`** or the join degrades into a sequential scan over 6.5M rows.
Runbook §1 states the same rule and how to check it — `EXPLAIN` must show
`Index Scan using master_parts_normalized_part_number_idx`.

Note `pace_parts` has no equivalent normalized index (only `pp_brand_part_idx` on
`(aaia_brand_code, part)`), so drive the backfill **from `pace_parts`**, batching by its `id`, and
look each row up in `master_parts`. Driving it the other way makes `pace_parts` the scanned side.

Skip `brand_pace_brand_mapping` rows with `match_phase = 'fuzzy'`. An exact or compact-key brand
match is safe; a fuzzy one can attach another manufacturer's line codes to the wrong part, which
is a precision bug rather than a recall miss. 58% of PACE's codes match ours outright — see the
`BrandPaceBrandMapping` docstring for why the phases exist.

### Four things to get right

1. **Coerce every variant with `str()`.** 4.6% of `part_search` entries are JSON *numbers*, not
   strings (`805160023`, `5189299`, `24143936`). And never `lstrip("0")` — a part number like
   `010140` has a sibling spelling `10140` and the zero is meaningful.
2. **Cap at 25 aliases per part**, shortest first. The average is 3.45 but the tail runs to 209
   variants on a single row, and this index already has a payload-size problem from `fitment_keys`
   — see the comment at [meilisearch_client.py:26](../src/search/meilisearch_client.py:26), where
   one part's 13,036-entry, 459KB array timed out the nightly upload every night for five days.
3. **Drop the 3.2% that are a different part number.** Most entries are the same number
   respelled, but 3.2% are a genuinely different number: Westin `26-3940` carries `203043-2`,
   `4155PS`, `23-73940`. Keeping them makes one part findable by another part's number. Filter to
   variants whose normalized form equals or ends with the normalized part number — a two-line
   condition that keeps every glued and line-code form and drops the rest.
4. **Accept the line-code noise tail.** Rough Country `7611` carries `AIRAID7611`, `QA17611`,
   `UMP7611` prefixes because that bare number also exists under those lines. We inherit it by
   copying wholesale. It is PACE's own behaviour on their storefronts and the harm is low; not
   worth building a per-brand line-code dictionary to filter it.

Also: 263 of 1,380 sampled alternate-number pairs resolve to a *different* `MasterPart` of the
same brand, which looks like ~26k duplicates at full scale. **They are not duplicates** — GTINs
differ and they are family siblings (aFe `54-10072` vs `54-80072-1`, Go Rhino `55182LT` listing
eight other RC2 bars). Do not feed any of this into
`scripts/merge_normalized_part_number_duplicates.py`.

Finally: this writes to ~2.4M rows against production. Check `df` on the server before running it,
and batch with `pgbulk` the way the PACE importers do.

## Step 2 — put it on the index

In [`src/search/meilisearch_client.py`](../src/search/meilisearch_client.py):

```python
SEARCHABLE_ATTRIBUTES = ["part_number", "part_aliases", "sku", "description", "aaia_code", "gtin"]
```

Order matters: Meilisearch's `attribute` ranking rule ranks by declaration order, so `part_number`
stays first and `part_aliases` must sit above `description` so an alias hit outranks a description
mention.

Then add the field in **both** copies of the document shape:

- [`_build_docs_for_batch`](../src/search/meilisearch_client.py:345) — `"part_aliases": part.part_aliases or []`
- [`master_part_to_index_shape`](../src/search/meilisearch_client.py:519) — same line

The second is a hand-maintained duplicate of the first and is the *public* API shape used for
audit/history responses. If they drift, search hits and API responses stop agreeing.

No new bulk-map helper is needed — unlike `fitment_keys` or `distributor_names`, this field lives
on the `MasterPart` row the reindex already selects.

## Step 3 — disable typo tolerance on the identifier attributes

```python
index.update_typo_tolerance({
    "disableOnAttributes": ["part_number", "part_aliases", "sku", "aaia_code", "gtin"],
})
```

**Free-text description search is unaffected** — `disableOnAttributes` is per-attribute. Verified
on two otherwise-identical probe indexes: `sunbrela car cover`, `covercaft custom fit`,
`suspention lift kit jeep` and `manifld gasket` returned byte-identical results with and without
the setting. Prefix matching is unaffected too (`80516` still matches `80516-00-23`) — prefix
search is a separate mechanism. What tightens: a typo'd alias (`805160029`) and a typo'd GTIN stop
returning false hits, and a glued-alias query stops matching sibling parts.

Do **not** raise `minWordSizeForTypos` instead — it is global and would break `sunbrela`.

This step and step 2 depend on each other. Typos-off alone does not make part-number lookup exact:
`80516-00-25` still matches `80516-00-23`, because the query splits to `[80516, 00, 25]` and the
first two tokens match exactly with no typo tolerance involved. The glued alias is what creates an
exact lane — `805160023` returns one hit, `805160029` returns none.

### The gotcha that silently undoes step 3

[`reindex_all_master_parts_zero_downtime`](../src/search/meilisearch_client.py:1241) configures its
staging index **by hand**, duplicating `setup_index()`, then swaps it over the live one. Anything
added only to `setup_index()` is reset by the next nightly reindex. This has already happened twice
in this file — see the comments at lines 1300 and 1306 about `maxValuesPerFacet` and `maxTotalHits`
silently reverting to Meilisearch defaults.

Add the call to [`setup_index()`](../src/search/meilisearch_client.py:114) **and** the staging path.
Better: factor all the settings calls into one `_apply_parts_index_settings(index)` used by both, so
there is no third chance to drift. `SEARCHABLE_ATTRIBUTES` from step 2 is already applied in both
places, so it is safe on its own.

---

## Verification

1. `--dry-run` first, reporting: parts matched, aliases per part (mean and p99), variants dropped
   by each of the four rules above, and aliases that collide across different parts of the same
   brand. A collision makes two distinct parts reachable by one alias — report them, don't
   silently keep them.
2. `EXPLAIN` the lookup and confirm `Index Scan using master_parts_normalized_part_number_idx`.
3. Reindex into staging and query staging **before** the swap. For a known part assert: glued
   number returns exactly one hit; line-code form returns it; a one-character variant of the glued
   number returns nothing; and the four misspelled free-text queries above still behave as they do
   today.
4. Tests next to `src/search/tests/`. The alias filter is pure and testable on fixtures. The repo
   needs a SQLite settings override to run tests — `.env` points at production.

Do not commit to `main` without asking: pushes to `main` auto-deploy to production.
