from django.db import migrations


class Migration(migrations.Migration):
    """
    Two indexes the master part enrichment run needs (``enrich_master_part_data``).

    1. ``provider_parts (provider_id, id)``

    The run pages through one provider's parts with ``WHERE provider_id = %s AND id > %s ORDER BY
    id LIMIT n``. With only the single-column ``provider_parts_provider_id_c1049dbe`` available,
    the planner sees a small LIMIT plus an ordering that the primary key already satisfies, and
    walks ``provider_parts_pkey`` ascending filtering on provider_id -- so for any provider whose
    rows sit at high ids (WheelPros, Helmet House, TireRack, everything onboarded recently) it
    scans millions of rows to return the first page. In production this made a single 50-row page
    exceed 500s; ``EXPLAIN ANALYZE`` of it could not be made to finish. The composite index turns
    the same query into a range scan that starts exactly where the previous page ended.

    Turn14 got away without it only because its rows start near the bottom of the table.

    2. ``atech_parts (brand_id, part_number)``

    Enrichment resolves raw feed rows from the ``{brand_id}_{part_number}`` key its ProviderPart
    carries (``master_parts._atech_provider_external_id``). Every other provider's raw table
    already has a composite unique index on its equivalent pair; A-Tech has ``brand_id`` alone
    plus a unique index on ``feed_part_number``, neither of which serves this lookup, against a
    1.43M-row table.

    Neither index is unique: A-Tech is not declared unique on that pair anywhere, and asserting it
    could reject a legitimate feed row at ingest time.

    ``atomic = False`` + ``CONCURRENTLY`` so neither build takes a write lock while the nightly
    ingest is running.
    """

    atomic = False

    dependencies = [
        ("src", "0156_helmet_house_raw_tables"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS provider_parts_provider_id_id_idx "
                "ON provider_parts (provider_id, id);"
            ),
            reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS provider_parts_provider_id_id_idx;",
        ),
        migrations.RunSQL(
            sql=(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS atech_parts_brand_part_number_idx "
                "ON atech_parts (brand_id, part_number);"
            ),
            reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS atech_parts_brand_part_number_idx;",
        ),
    ]
