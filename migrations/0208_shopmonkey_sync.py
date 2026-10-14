# ShopMonkey inventory sync -- see the module docstring in src/models.py just above
# ShopMonkeySyncVendor for the full picture. Ported from a standalone single-tenant tool built
# for Trident Motorsports; this makes it per-company and reads from data we already have instead
# of re-fetching from each distributor.
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0207_company_feature_flags"),
    ]

    operations = [
        migrations.CreateModel(
            name="ShopMonkeySyncVendor",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("shopmonkey_external_id", models.CharField(blank=True, max_length=255, null=True)),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="shopmonkey_sync_vendors",
                        to="src.company",
                    ),
                ),
                (
                    "provider",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="shopmonkey_sync_vendors",
                        to="src.providers",
                    ),
                ),
            ],
            options={
                "db_table": "shopmonkey_sync_vendor",
                "unique_together": {("company", "provider")},
            },
        ),
        migrations.CreateModel(
            name="ShopMonkeySyncBrand",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("shopmonkey_external_id", models.CharField(blank=True, max_length=255, null=True)),
                ("entity", models.CharField(default="Tire", max_length=32)),
                ("enabled", models.BooleanField(default=True)),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="shopmonkey_sync_brands",
                        to="src.company",
                    ),
                ),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="shopmonkey_sync_brands",
                        to="src.brands",
                    ),
                ),
                (
                    "vendor",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="sync_brands",
                        to="src.shopmonkeysyncvendor",
                    ),
                ),
            ],
            options={
                "db_table": "shopmonkey_sync_brand",
                "unique_together": {("company", "brand")},
            },
        ),
        migrations.CreateModel(
            name="ShopMonkeySyncPart",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("shopmonkey_external_id", models.CharField(blank=True, max_length=255, null=True)),
                ("last_pushed_name", models.CharField(blank=True, max_length=255, null=True)),
                ("last_pushed_retail_cost_cents", models.DecimalField(blank=True, decimal_places=8, max_digits=28, null=True)),
                ("last_pushed_wholesale_cost_cents", models.DecimalField(blank=True, decimal_places=8, max_digits=28, null=True)),
                ("status", models.PositiveSmallIntegerField()),
                ("status_name", models.CharField(max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "sync_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="sync_parts",
                        to="src.shopmonkeysyncbrand",
                    ),
                ),
                (
                    "provider_part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="shopmonkey_sync_parts",
                        to="src.providerpart",
                    ),
                ),
            ],
            options={
                "db_table": "shopmonkey_sync_part",
                "unique_together": {("sync_brand", "provider_part")},
            },
        ),
        migrations.AddIndex(
            model_name="shopmonkeysyncpart",
            index=models.Index(fields=["sync_brand", "status"], name="smsp_sync_brand_status_idx"),
        ),
        migrations.CreateModel(
            name="ShopMonkeySyncExecution",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(max_length=16)),
                ("message", models.TextField(blank=True, null=True)),
                ("metadata", models.JSONField(default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="shopmonkey_sync_executions",
                        to="src.company",
                    ),
                ),
                (
                    "sync_brand",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="sync_executions",
                        to="src.shopmonkeysyncbrand",
                    ),
                ),
            ],
            options={
                "db_table": "shopmonkey_sync_execution",
            },
        ),
        migrations.AddIndex(
            model_name="shopmonkeysyncexecution",
            index=models.Index(fields=["company", "-created_at"], name="smse_company_created_idx"),
        ),
    ]
