from django.db import migrations


class Migration(migrations.Migration):
    """
    Index ``master_parts`` on the GTIN "digit core" so provider ingests can look up an existing
    MasterPart by barcode -- see ``src.integrations.utils.master_part_matching`` and
    ``docs/PART_NUMBER_NORMALIZATION.md``.

    Until now GTIN was only ever used to *verify* a candidate already found by normalized part
    number, never to find one. That leaves three large families of duplicates undetectable,
    because their part numbers are not string-related at all and only the barcode links them:
    brand prefixes (Premier ships Toyo's '357280' as 'TOY357280'; Turn14, Meyer, A-Tech and
    WheelPros do the same for other brands, ~19,000 instances), A-Tech's ~41,000 'S0845'-style
    placeholder part numbers, and leading-zero differences that also change length.

    The indexed expression is digits-only with leading zeros stripped, which is the first half of
    ``pn_util.normalize_gtin``. The check digit is validated in Python rather than in the index,
    so the index intentionally groups slightly more loosely than the validator -- it only ever
    widens the candidate set, and every candidate is re-validated before use. It must stay
    character-for-character identical to ``master_part_matching._GTIN_CORE_SQL`` or Postgres will
    silently stop using it and each ingest batch becomes a sequential scan over ~3.2M rows.

    Partial (``WHERE gtin IS NOT NULL AND gtin <> ''``) because ~22% of master parts carry no
    barcode at all and would only bloat the index.

    ``atomic = False`` + ``CONCURRENTLY`` so building it takes no write lock while syncs run.
    """

    atomic = False

    # Originally depended on both 0167 (pcdb) and 0166 (realtruck) to rejoin two parallel tips
    # into a single head -- but 0157-0166 (the realtruck branch) were never actually committed
    # to git, only ever present as local uncommitted files, so that second dependency broke
    # every deploy with NodeNotFoundError (same class of bug fixed in commit fff1801 for
    # 0160/0162 earlier). Dropped back to depending on 0167 alone. Whoever commits the realtruck
    # branch will need to either renumber it to descend from here or add a fresh merge migration
    # at that point -- this migration touches neither branch's tables, so nothing here depends
    # on which one wins.
    dependencies = [
        ("src", "0167_pcdb_tables"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS master_parts_gtin_core_idx
            ON master_parts (
                brand_id,
                ltrim(regexp_replace(coalesce(gtin, ''), '\D', '', 'g'), '0')
            )
            WHERE gtin IS NOT NULL AND gtin <> ''
            """,
            reverse_sql="""
            DROP INDEX CONCURRENTLY IF EXISTS master_parts_gtin_core_idx
            """,
        ),
    ]
