from django.db import migrations


class Migration(migrations.Migration):
    """
    Index ``master_parts`` on the punctuation/case-insensitive form of ``part_number`` so provider
    ingests can look up an existing MasterPart whose spelling differs only in formatting -- see
    ``src.integrations.utils.master_part_matching`` and ``docs/PART_NUMBER_NORMALIZATION.md``.

    Deliberately an *expression* index rather than a stored normalized column: no new field, no
    backfill over ~3M rows, and no possibility of the stored value drifting out of sync with
    ``part_number``. The expression here must stay character-for-character identical to the one in
    ``master_part_matching._NORMALIZED_PART_NUMBER_SQL`` or Postgres will silently stop using the
    index and the lookups turn into sequential scans.

    The index is intentionally NOT unique. Distinct parts legitimately share a normalized key
    (``942B-89060+12`` vs ``942B-89060-12`` are different wheel offsets), so uniqueness here would
    reject valid rows at write time; ambiguity is resolved by the guards in the resolver instead.

    ``atomic = False`` + ``CONCURRENTLY`` so building the index does not take a write lock on
    master_parts while syncs are running.
    """

    atomic = False

    dependencies = [
        ("src", "0142_company_provider_order_account_order_method"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS master_parts_normalized_part_number_idx
            ON master_parts (
                brand_id,
                upper(regexp_replace(part_number, '[^A-Za-z0-9]', '', 'g'))
            )
            """,
            reverse_sql="""
            DROP INDEX CONCURRENTLY IF EXISTS master_parts_normalized_part_number_idx
            """,
        ),
    ]
