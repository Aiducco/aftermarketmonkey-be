# Brand tire data

Collecting tire specifications from the manufacturers themselves, and eventually letting them
correct the catalog.

Phase one — the registry, the landing table and the ingest — is built. Phase two, the merge into
`tire_specs`, is designed here and not written.

---

## Why this exists

`tire_specs` is currently filled from three places, in this precedence: a distributor's structured
fields, our own sidewall parser (`src/domain/tire_size.py`), and an LLM reading distributor
titles. Two catalog merges sit on top of that — SimpleTire and TDG — and they are where most of
the real specification data comes from, because no distributor feed we ingest carries a tread
depth, a UTQG grade or a rim-width range.

Those two merges have a ceiling, and it is not a matter of crawling harder:

- **A reseller lists what a reseller stocks.** The sizes nobody stocks, the OE-fitment variants,
  the commercial lines a consumer site does not sell — none of it is there to find.
- **A reseller is not the author.** Where a reseller and the manufacturer disagree about a tread
  depth, that is not two opinions to reconcile. Today we have no way to hold the authoritative
  answer, because there is nowhere to put it that says where it came from.
- **Some fields only a manufacturer publishes at all.** Revolutions per mile, the measuring rim a
  published diameter was taken on, the full OE homologation list, the ply construction behind a
  load range.

`report_tire_catalog_gaps` already splits the unmatched third of the catalog by cause, and its
first and largest cause is *"brand absent from both catalogs — only a new source fixes this"*.
This is that new source, and it is per brand because that is how manufacturers publish.

## Why it is its own department

Everything else under `src/integrations` is a trading partner: a distributor whose feed we ingest
in order to sell their stock, with credentials, pricing, inventory and orders attached. A
manufacturer data source has none of that. It has a brand, a channel, a column map and a refresh
cadence — and the channel is different for every brand and changes without warning. Modelling it
as "another provider" would put a row in `providers` that can never have a connection, an order
adapter or a price.

So: a new package (`src/integrations/brand_data/`), three new tables, three commands, and no
change to anything that already runs.

## Where things live

| | |
|---|---|
| `src/models.py` | `TireBrandSource`, `TireBrandSourceRun`, `RawTireSpec` (at the end of the file, under the *Brand tire data* banner) |
| `migrations/0198_brand_tire_data.py` | the three tables; additive only |
| `src/integrations/brand_data/registry.py` | loader lookup — which code reads which source |
| `src/integrations/brand_data/loaders/` | the transports: `csv`, `http_json`, and brand-specific ones |
| `src/integrations/brand_data/mapping.py` | one source record → one row, driven by the column map |
| `src/integrations/brand_data/normalize.py` | readers for the display strings brands publish |
| `src/integrations/brand_data/ingest.py` | the runner: identity, change detection, run log |
| `src/integrations/brand_data/seeds.py` | declared sources from JSON; planned sources from our catalog |
| `src/integrations/brand_data/coverage.py` | what we carry vs. what we collect |
| `resources/brand_data/` | the files themselves (not committed) and their declarations (committed) |
| `src/management/commands/` | `seed_tire_brand_sources`, `ingest_brand_tire_specs`, `report_brand_tire_sources` |

## The data model

**`tire_brand_sources` — the registry.** One row per *way of getting* one brand's data. It is a
database table rather than a config file because most of what it holds is operational and changes
without a deploy: who sends the file, when it last arrived, whether the portal moved. A brand we
have nothing for is a row too, with `status='planned'` — the table is the work list, not a list of
work already done.

Two separate questions are kept in separate columns:

- `method` — how the data reaches us in the real world: `csv`, `manual`, `api`, `scrape`. This is
  what an operator needs, and what decides whether a source can be scheduled at all.
- `handler` — which loader reads it. Every brand that emails a spreadsheet is `'csv'`; the
  difference between them is the column map in `config`. Blank means no loader yet, which is the
  normal state of a planned row.

Secrets are referenced by the *name* of the setting holding them (`credential_setting`), never by
value. This table is read by support, dumped into fixtures, and printed by the reporting command.

**`tire_brand_source_runs` — the log.** Every attempt, successful or not, with the row counts and
a fingerprint of what was read. The failure modes here are quiet ones: a sheet whose headers
changed still parses, a crawl that starts returning nothing still "succeeds", a portal that logged
you out returns a 200. None of them raise; all of them show up as a row count that fell off a
cliff next to the run before it.

**`raw_tire_specs` — the landing zone.** One row per tire per source, deliberately **not** joined
to `MasterPart` or `TireSpec` — the same choice `simpletire_skus` and `tdg_products` made, for the
same reason: collecting a brand's data and matching it into our catalog are separate jobs, and
making the first depend on the second means a brand we cannot match yet is a brand we do not
collect.

Two rules hold that table together:

1. **Every typed column holds what the source published, and nothing else.** No column is ever
   filled by a parser, a lookup or a model. NULL means "not published" — never 0, never False. Our
   own decode of the size lives in `parsed_size`, which is JSON and obviously ours. The moment a
   derived value can sit in a published column, nobody downstream can tell an authority's figure
   from our guess at it, which is exactly what `tire_specs.spec_source` exists to prevent one
   level up.
2. **A mapping fix must never require a re-pull.** `raw` keeps the record verbatim, `attributes`
   keeps every published label/value pair including the ones no column covers. This matters more
   here than in the reseller tables: a manufacturer file often arrives once, by hand, from a rep
   who has moved on by the time we notice the column we skipped.

Identity is `(source, external_key)`: the brand's own article number where they publish one, and
otherwise a marked digest (`d:…`) of what actually distinguishes a SKU — brand, part number, size,
model, sub-model, load range. A row is written only when its values moved; unchanged rows get
their `last_seen_*` stamped, which is what makes "the brand withdrew this" answerable at all.

## Adding a brand

**They send a spreadsheet (most brands).** No code.

1. Save the file under `resources/brand_data/<brand>/<edition>.xlsx`.
2. Copy `resources/brand_data/sources/example-brand-sheet.json`, set `slug`, `brand_name`,
   `config.path` and the `field_map`, `status: "active"`.
3. `manage.py seed_tire_brand_sources --from-json resources/brand_data/sources/<brand>.json`
4. `manage.py ingest_brand_tire_specs --source <slug> --dry-run` — read the warning histogram and
   the *"published but not mapped"* line, fix the map, repeat.
5. `manage.py ingest_brand_tire_specs --source <slug> --prune`

**They have an API.** Same, with `"handler": "http_json"` and a config naming the URL, the
`records_path`, the pagination and the *name* of the setting holding the key. Put the key in the
environment, never in `config`.

**It has to be scraped.** Write a module in `loaders/`, register it with `@loader("<slug>")`, and
yield the records verbatim — the mapping, identity and writing are already done for you. There is
deliberately no generic scraper: every site's structure is its own, and a "configurable scraper"
is a worse language for expressing it. `src/integrations/services/simpletire.py` is the reference
for how one is written here.

**Nobody has looked yet.** It is still a row: `method='manual'`, `status='planned'`, with whatever
is known in `notes`. `manage.py seed_tire_brand_sources --from-catalog` creates these in bulk from
our own catalog, ordered by how many of that brand's tires no reseller catalog has confirmed.

## The column map

`config.field_map` is `{our column: their key}`. A target takes a single key, a list tried in
order (brands rename columns between editions of the same sheet), or `{"const": "…"}`. Keys match
loosely on case, spacing and punctuation, and a dotted path's last segment resolves on its own, so
a map written against a flat CSV keeps working when the same brand later sends nested JSON.

Four targets name a *cell* rather than a column, because brands routinely pack several facts into
one: `service_description` (→ load index, dual index, speed rating), `load_range` (→ the letter and
the ply count behind it), `rim_width_range` (→ min and max), `utqg` (→ the printed string and its
three grades). An explicitly mapped column always beats a fan-out.

Every target is validated against the real column list before a source pulls anything, so a typo
is an error at seed time rather than a column that is quietly always NULL. The full list of
targets is `mapping.COLUMN_KINDS`.

## Running it

```bash
manage.py ingest_brand_tire_specs --list                       # what the registry can run
manage.py ingest_brand_tire_specs --source michelin --dry-run  # read and map, write nothing
manage.py ingest_brand_tire_specs --source michelin --prune    # the real thing
manage.py ingest_brand_tire_specs --all --skip-unchanged       # everything active
manage.py report_brand_tire_sources --brands                   # what is collected, what is missing
```

`--prune` deletes rows the run did not see — the brand's withdrawals — and is refused with
`--limit`, because a partial pull says nothing about absence.

---

## Phase two: merging into `tire_specs` (not built)

The shape is already fixed by the two merges that exist, and the new one should reuse them rather
than invent a third set of rules.

**Matching** goes through `src/integrations/services/tire_catalog.py` — the same three tiers, the
same size gate on every one of them. That gate is not optional: short numeric MPNs collide across
manufacturers, and the existing merges only survive it because the dimensions have to agree too.
Brand aliases are per-source, inferred from part-number evidence and verified against size
agreement — never from string similarity. A manufacturer source starts with an advantage here:
`TireBrandSource.brand` names our catalog's brand row directly, so tier 1 is brand-exact.

**Precedence.** Manufacturer data outranks both reseller catalogs on any field the manufacturer
publishes, which means `tire_specs.spec_source` needs a fourth value (`brand`) and
`reparse_tire_sizes` needs to skip those rows the way it skips `simpletire` ones. That stamp is
the only thing standing between a merge and a silent revert by the next parser fix.

**What it may write** is a `TAKE_THEIRS`/`FILL_ONLY` split like `simpletire_sync`'s, asserted
against `tire_catalog.NEVER_WRITE` at import. Three cautions carry over unchanged, and one is new:

- `overall_diameter_in` on `raw_tire_specs` is the *measured* diameter on a stated measuring rim.
  `tire_specs.overall_diameter_in` is the diameter the size prints, and it is what "35 inch"
  search matches. Same name, different quantity — it stays in `NEVER_WRITE`.
- `max_speed_mph`, the size block and `sub_model` stay excluded for the reasons already recorded
  in `tire_catalog`.
- A column that is present but never populated is worse than a missing one — check coverage before
  adding a field to the merge, the way `is_run_flat` was excluded from the SimpleTire merge.
- **New:** these files are snapshots with an edition date. A merge should prefer the newest
  edition of a source and should not let a stale sheet overwrite a fresh one.

**Provenance** mirrors what `tire_specs` already does for the two catalogs: a nullable
`raw_tire_spec` FK with `SET_NULL`, a match tier, and a synced-at timestamp. `SET_NULL` because
the landing zone can be re-pulled or pruned, and losing the pointer must not take the specs with
it.

**Before writing it**, run the coverage report per source: how many rows matched, at which tier,
how many of the matched rows disagree with what is in `tire_specs` today and on which fields. The
disagreement histogram is the thing to read — the SimpleTire merge's design came out of measuring
22,155 matched rows first, and the category work came out of noticing that most apparent
disagreements were two axes rather than one error.

## Open questions

- **Legal/contractual.** Some manufacturer data books are dealer-confidential. Worth confirming
  per brand before a scrape, and worth recording the answer in `notes`.
- **Editions.** `RawTireSpec` currently keeps one row per SKU per source, overwritten in place,
  with `last_changed_at` marking movement. If comparing editions ever matters, the cheapest change
  is an `edition` column in the identity rather than history rows — decide before the first
  source has years of data behind it.
- **Model-line data.** Manufacturers publish per *line* facts (warranty terms, technology, tread
  pattern imagery) as well as per SKU. Today they land repeated on every SKU row. A
  `raw_tire_lines` table is the obvious normalisation, and it should wait until a consumer asks
  for it.
