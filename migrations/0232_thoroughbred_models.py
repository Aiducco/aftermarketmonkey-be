from django.db import migrations, models
import django.db.models.deletion


def create_thoroughbred_provider(apps, schema_editor):
    Providers = apps.get_model("src", "Providers")
    Providers.objects.get_or_create(
        kind=40,
        defaults={
            "name": "Thoroughbred Diesel",
            "status": 1,
            "status_name": "ACTIVE",
            "type": 2,
            "type_name": "DISTRIBUTOR",
            "kind_name": "THOROUGHBRED_DIESEL",
        },
    )


def remove_thoroughbred_provider(apps, schema_editor):
    apps.get_model("src", "Providers").objects.filter(kind=40).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0231_edi_documents"),
    ]

    operations = [
        migrations.CreateModel(
            name="ThoroughbredBrand",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("name", models.CharField(max_length=255, unique=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "thoroughbred_brands",
            },
        ),
        migrations.CreateModel(
            name="BrandThoroughbredBrandMapping",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="thoroughbred_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "thoroughbred_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.thoroughbredbrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_thoroughbred_brand_mapping",
                "unique_together": {("brand", "thoroughbred_brand")},
            },
        ),
        migrations.CreateModel(
            name="ThoroughbredParts",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("external_id", models.CharField(max_length=64, unique=True)),
                ("sku", models.CharField(db_index=True, max_length=255)),
                ("url", models.TextField(blank=True, null=True)),
                ("title", models.CharField(blank=True, max_length=512, null=True)),
                ("image_url", models.TextField(blank=True, null=True)),
                (
                    "price",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
                ),
                ("inventory", models.IntegerField(blank=True, null=True)),
                ("item_type", models.CharField(blank=True, max_length=32, null=True)),
                ("kit_short_description", models.TextField(blank=True, null=True)),
                ("vehicleid", models.JSONField(blank=True, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="parts",
                        to="src.thoroughbredbrand",
                    ),
                ),
            ],
            options={
                "db_table": "thoroughbred_parts",
            },
        ),
        migrations.AddIndex(
            model_name="thoroughbredparts",
            index=models.Index(fields=["brand", "sku"], name="tbd_brand_sku_idx"),
        ),
        migrations.CreateModel(
            name="ThoroughbredCompanyPricing",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                (
                    "cost",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
                ),
                (
                    "markup_percent_applied",
                    models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.thoroughbredparts",
                    ),
                ),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="thoroughbred_company_pricing",
                        to="src.company",
                    ),
                ),
            ],
            options={
                "db_table": "thoroughbred_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
        migrations.CreateModel(
            name="ThoroughbredVehicle",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("vehicle_id", models.IntegerField(unique=True)),
                ("year", models.IntegerField()),
                ("make", models.CharField(max_length=128)),
                ("model", models.CharField(max_length=128)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "thoroughbred_vehicles",
            },
        ),
        migrations.AddIndex(
            model_name="thoroughbredvehicle",
            index=models.Index(fields=["make", "model", "year"], name="tbd_veh_mmy_idx"),
        ),
        migrations.RunPython(create_thoroughbred_provider, remove_thoroughbred_provider),
    ]
