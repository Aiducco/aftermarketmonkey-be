import django.db.models.deletion
from django.db import migrations, models


def create_tirerack_provider(apps, schema_editor):
    Providers = apps.get_model("src", "Providers")
    Providers.objects.get_or_create(
        kind=36,
        defaults={
            "name": "TireRack",
            "status": 1,
            "status_name": "ACTIVE",
            "type": 2,
            "type_name": "DISTRIBUTOR",
            "kind_name": "TIRERACK",
        },
    )


def remove_tirerack_provider(apps, schema_editor):
    Providers = apps.get_model("src", "Providers")
    Providers.objects.filter(kind=36).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0137_order_account_is_default_and_backfill"),
    ]

    operations = [
        migrations.CreateModel(
            name="TireRackBrand",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "tirerack_brands",
                "unique_together": {("name",)},
            },
        ),
        migrations.CreateModel(
            name="TireRackParts",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("part_number", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True, null=True)),
                ("quantity", models.IntegerField(blank=True, null=True)),
                ("country_of_origin", models.CharField(blank=True, max_length=255, null=True)),
                ("fet", models.DecimalField(blank=True, decimal_places=5, max_digits=14, null=True)),
                ("base_price", models.DecimalField(blank=True, decimal_places=5, max_digits=14, null=True)),
                ("total_price", models.DecimalField(blank=True, decimal_places=5, max_digits=14, null=True)),
                ("road_hazard_warranty", models.CharField(blank=True, max_length=255, null=True)),
                ("treadlife_warranty_1", models.CharField(blank=True, max_length=255, null=True)),
                ("treadlife_warranty_2", models.CharField(blank=True, max_length=255, null=True)),
                ("treadlife_warranty_3", models.CharField(blank=True, max_length=255, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="parts",
                        to="src.tirerackbrand",
                    ),
                ),
            ],
            options={
                "db_table": "tirerack_parts",
                "unique_together": {("part_number", "brand")},
            },
        ),
        migrations.CreateModel(
            name="BrandTireRackBrandMapping",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="tirerack_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "tirerack_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.tirerackbrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_tirerack_brand_mapping",
                "unique_together": {("brand", "tirerack_brand")},
            },
        ),
        migrations.RunPython(create_tirerack_provider, remove_tirerack_provider),
    ]
