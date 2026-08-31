from django.db import migrations


class Migration(migrations.Migration):
    """
    Composite index on ``turn14_brand_pricing`` for the exact filter+sort shape
    ``sync_provider_pricing_from_turn14_for_company`` runs once per 10,000-row batch, per
    brand-partition worker (up to 16 in parallel), roughly 78 times per company:

        WHERE company_id = :cid AND brand_id IN (:catalog_ids) AND id > :last_id
        ORDER BY id LIMIT 10000

    Confirmed live 2026-08-31, investigating why Turn14 pricing jobs take 30-65 minutes: with
    only single-column indexes on ``brand_id`` and ``company_id`` separately, Postgres's only
    option is a ``BitmapAnd`` of the two -- for one real company, a bitmap of ~123k rows
    (brand_id) intersected with one of ~792k rows (company_id, i.e. nearly the whole company) --
    followed by an explicit ``Sort`` on ``id`` that neither index gets to skip. That expensive
    plan runs on every single batch, times every worker, times every company.

    ``(company_id, brand_id, id)`` lets Postgres seek straight to each worker's own slice and
    return it pre-sorted by ``id`` within that slice -- no bitmap intersection, no separate sort.

    ``atomic = False`` + ``CONCURRENTLY``: this table is ~7.7M rows / ~3.3GB total and under
    constant write load from in-flight pricing syncs -- building it without CONCURRENTLY would
    take a write lock for the whole build.
    """

    atomic = False

    dependencies = [
        # Depends on the latest migration actually pushed to origin/main at the time this was
        # written (0191-0193 exist locally in this shared working directory but were not yet
        # committed/pushed by whoever wrote them -- depending on an unpushed migration breaks
        # deploy with NodeNotFoundError the moment this lands before they do). This migration
        # touches an unrelated table (turn14_brand_pricing), so there is no real dependency on
        # the tire-spec chain either way.
        ("src", "0190_tire_spec_catalog_precision"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS turn14_brand_pricing_company_brand_id_idx
            ON turn14_brand_pricing (company_id, brand_id, id)
            """,
            reverse_sql="""
            DROP INDEX CONCURRENTLY IF EXISTS turn14_brand_pricing_company_brand_id_idx
            """,
        ),
    ]
