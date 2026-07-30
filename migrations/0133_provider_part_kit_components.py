import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0132_po_distributor_order_processed_order"),
    ]

    operations = [
        migrations.AddField(
            model_name="providerpart",
            name="is_kit",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="kit_source_provider_part",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="expanded_kit_line_items",
                to="src.providerpart",
            ),
        ),
        migrations.CreateModel(
            name="ProviderPartKitComponent",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("quantity", models.PositiveIntegerField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "component_part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="kit_of",
                        to="src.providerpart",
                    ),
                ),
                (
                    "kit_part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="kit_components",
                        to="src.providerpart",
                    ),
                ),
            ],
            options={
                "db_table": "provider_part_kit_components",
                "unique_together": {("kit_part", "component_part")},
            },
        ),
    ]
