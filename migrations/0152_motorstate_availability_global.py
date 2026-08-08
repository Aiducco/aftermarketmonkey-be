"""
Make MotorStateAvailability distributor-wide instead of per company.

Stock and status are identical whichever dealer's API key fetches them, so the spine belongs
with the shared catalog (maintained from the primary connection) rather than being duplicated
per connected company. Only price is account-specific, and that already lives on
MotorStateCompanyPricing.

Collapses to one row per part number (lowest id wins) and drops the company column.
"""
from django.db import migrations, models


def forwards(apps, schema_editor):
    with schema_editor.connection.cursor() as cursor:
        # Keep the oldest row per part number; the duplicates are the same distributor-wide
        # stock recorded under different companies.
        cursor.execute(
            """
            DELETE FROM motorstate_availability a
            USING (
                SELECT part_number, MIN(id) AS keep_id
                FROM motorstate_availability
                GROUP BY part_number
            ) k
            WHERE k.part_number = a.part_number AND a.id <> k.keep_id
            """
        )


def backwards(apps, schema_editor):
    """
    Re-point every surviving row at the primary Motor State connection's company. The
    per-company duplicates cannot be reconstructed -- they carried identical data by definition.
    """
    with schema_editor.connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE motorstate_availability
            SET company_id = (
                SELECT cp.company_id
                FROM company_providers cp
                JOIN providers p ON p.id = cp.provider_id
                WHERE p.kind = 23
                ORDER BY cp.primary DESC, cp.id
                LIMIT 1
            )
            WHERE company_id IS NULL
            """
        )


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0151_motorstate_company_pricing"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
        # Composite key and per-company indexes must go before the company column does.
        migrations.AlterUniqueTogether(name="motorstateavailability", unique_together=set()),
        migrations.RemoveIndex(model_name="motorstateavailability", name="motorstate__company_a2bf3a_idx"),
        migrations.RemoveIndex(model_name="motorstateavailability", name="motorstate__company_839dcf_idx"),
        migrations.RemoveField(model_name="motorstateavailability", name="company"),
        migrations.AlterField(
            model_name="motorstateavailability",
            name="part_number",
            field=models.CharField(max_length=128, unique=True),
        ),
        migrations.AddIndex(
            model_name="motorstateavailability",
            index=models.Index(fields=["source_updated_on"], name="ms_avail_updated_on_idx"),
        ),
        migrations.AddIndex(
            model_name="motorstateavailability",
            index=models.Index(fields=["brand_code"], name="ms_avail_brand_code_idx"),
        ),
    ]
