# ShopMonkey inventory sync goes multi-vendor -- see the updated module docstring in
# src/models.py just above ShopMonkeySyncVendor. ShopMonkeySyncPart's rekey from provider_part to
# master_part is non-destructive: shopmonkey_sync_part is confirmed empty (count 0) in production.
import django.db.models.deletion
from django.db import migrations, models


def _backfill_entity_to_part(apps, schema_editor):
    ShopMonkeySyncBrand = apps.get_model("src", "ShopMonkeySyncBrand")
    ShopMonkeySyncBrand.objects.filter(entity="Tire").update(entity="Part")


def _revert_entity_to_tire(apps, schema_editor):
    ShopMonkeySyncBrand = apps.get_model("src", "ShopMonkeySyncBrand")
    ShopMonkeySyncBrand.objects.filter(entity="Part").update(entity="Tire")


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0240_external_purchase_orders"),
    ]

    operations = [
        migrations.AddField(
            model_name="shopmonkeysyncvendor",
            name="bin_name",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
        migrations.AlterField(
            model_name="shopmonkeysyncbrand",
            name="entity",
            field=models.CharField(default="Part", max_length=32),
        ),
        migrations.RunPython(_backfill_entity_to_part, _revert_entity_to_tire),
        migrations.CreateModel(
            name="ShopMonkeySyncBrandVendor",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("priority", models.PositiveSmallIntegerField(default=1)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "sync_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="additional_vendors",
                        to="src.shopmonkeysyncbrand",
                    ),
                ),
                (
                    "vendor",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_assignments",
                        to="src.shopmonkeysyncvendor",
                    ),
                ),
            ],
            options={
                "db_table": "shopmonkey_sync_brand_vendor",
                "unique_together": {("sync_brand", "vendor")},
                "ordering": ["priority"],
            },
        ),
        migrations.AlterUniqueTogether(
            name="shopmonkeysyncpart",
            unique_together=set(),
        ),
        migrations.RemoveField(
            model_name="shopmonkeysyncpart",
            name="provider_part",
        ),
        migrations.AddField(
            model_name="shopmonkeysyncpart",
            name="master_part",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="shopmonkey_sync_parts",
                to="src.masterpart",
                default=None,
            ),
            preserve_default=False,
        ),
        migrations.AlterUniqueTogether(
            name="shopmonkeysyncpart",
            unique_together={("sync_brand", "master_part")},
        ),
        migrations.AddField(
            model_name="shopmonkeysyncpart",
            name="last_pushed_sku",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="shopmonkeysyncpart",
            name="last_pushed_quantity",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="shopmonkeysyncpart",
            name="last_pushed_bin_location",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
        migrations.AddField(
            model_name="shopmonkeysyncpart",
            name="sourced_from_vendor",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="sourced_sync_parts",
                to="src.shopmonkeysyncvendor",
            ),
        ),
        migrations.AddField(
            model_name="shopmonkeysyncpart",
            name="last_error",
            field=models.TextField(blank=True, null=True),
        ),
    ]
