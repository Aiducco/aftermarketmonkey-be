from django.db import migrations, models


class Migration(migrations.Migration):
    """
    ``MasterPart.product_type`` (wheel / tire / part) plus ``product_type_source`` provenance.

    Both columns are nullable with no default, so the ``ALTER TABLE`` is a catalog-only change --
    no rewrite of the 3.2M-row ``master_parts`` heap, and no long lock. NULL is a real state here
    and means "not classified yet", not "part"; see the field comments in ``src/models.py``.

    The ``product_type`` index is built ``CONCURRENTLY`` for the same reason as the ones in
    ``0157_master_part_enrichment_indexes``: a plain ``CREATE INDEX`` holds a write lock for the
    duration, and this table is written by the nightly ``ingest_all_providers`` run. That forces
    ``atomic = False`` (Postgres rejects ``CONCURRENTLY`` inside a transaction), which in turn
    means the column adds and the index build are not one atomic unit -- on failure partway
    through, re-running is safe because every step is ``IF NOT EXISTS`` / ``IF EXISTS``.

    ``SeparateDatabaseAndState`` is what lets the model keep ``db_index=True`` (so
    ``makemigrations`` sees no drift) while the database side does the concurrent build itself
    instead of Django's locking one.
    """

    atomic = False

    dependencies = [
        ("src", "0169_product_groups"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AddField(
                    model_name="masterpart",
                    name="product_type",
                    field=models.CharField(blank=True, db_index=True, max_length=16, null=True),
                ),
                migrations.AddField(
                    model_name="masterpart",
                    name="product_type_source",
                    field=models.CharField(blank=True, max_length=64, null=True),
                ),
            ],
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        "ALTER TABLE master_parts "
                        "ADD COLUMN IF NOT EXISTS product_type varchar(16) NULL, "
                        "ADD COLUMN IF NOT EXISTS product_type_source varchar(64) NULL;"
                    ),
                    reverse_sql=(
                        "ALTER TABLE master_parts "
                        "DROP COLUMN IF EXISTS product_type, "
                        "DROP COLUMN IF EXISTS product_type_source;"
                    ),
                ),
                migrations.RunSQL(
                    sql=(
                        "CREATE INDEX CONCURRENTLY IF NOT EXISTS master_parts_product_type_idx "
                        "ON master_parts (product_type);"
                    ),
                    reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS master_parts_product_type_idx;",
                ),
            ],
        ),
    ]
