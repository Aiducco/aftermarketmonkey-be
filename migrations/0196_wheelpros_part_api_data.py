"""
Adds the Product API enrichment blob to ``wheelpros_parts``.

Kept separate from ``raw_data`` (the SFTP CSV row verbatim) on purpose -- see the field help
text and ``src.integrations.services.wheelpros_products``. Additive and nullable, so it is safe
on the live table; nothing reads these columns until the fetch command populates them.

Hand-written rather than left as makemigrations emitted it: autodetect wanted to sweep in ~30
unrelated operations from pre-existing model/migration drift (BigAutoField id changes, Premier
and CustomIntegrationRequest constraint churn) that have nothing to do with this change.
"""
import django.core.serializers.json
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0195_merge_facet_rail_and_turn14_index"),
    ]

    operations = [
        migrations.AddField(
            model_name="wheelprospart",
            name="api_data",
            field=models.JSONField(
                blank=True,
                null=True,
                encoder=django.core.serializers.json.DjangoJSONEncoder,
                help_text=(
                    "The Product API search row verbatim: upc, title, brand, inventory, "
                    "properties, prices (msrp/map/nip) and the full images list with all four "
                    "size variants. Re-derive columns from here; never re-crawl."
                ),
            ),
        ),
        migrations.AddField(
            model_name="wheelprospart",
            name="api_synced_at",
            field=models.DateTimeField(
                blank=True,
                null=True,
                db_index=True,
                help_text="When api_data was last refreshed. NULL means the API has never matched this row.",
            ),
        ),
        migrations.AlterField(
            model_name="wheelprospart",
            name="raw_data",
            field=models.JSONField(
                blank=True,
                null=True,
                help_text=(
                    "The SFTP CSV row verbatim, warehouse-code columns included. Source-specific: "
                    "API data goes to api_data, never here. NULL means this row was never seen in "
                    "a CSV feed -- i.e. it came from the Product API only (see api_data)."
                ),
            ),
        ),
    ]
