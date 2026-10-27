from django.db import migrations, models
import django.db.models.deletion


def create_ntw_provider(apps, schema_editor):
    Providers = apps.get_model("src", "Providers")
    Providers.objects.get_or_create(
        kind=41,
        defaults={
            "name": "NTW",
            "status": 1,
            "status_name": "ACTIVE",
            "type": 2,
            "type_name": "DISTRIBUTOR",
            "kind_name": "NTW",
        },
    )


def remove_ntw_provider(apps, schema_editor):
    apps.get_model("src", "Providers").objects.filter(kind=41).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0232_thoroughbred_models"),
    ]

    operations = [
        migrations.CreateModel(
            name="NtwBrand",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("name", models.CharField(max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "ntw_brands",
                "unique_together": {("name",)},
            },
        ),
        migrations.CreateModel(
            name="NtwParts",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("part_number", models.CharField(max_length=255)),
                ("product_id", models.CharField(blank=True, max_length=255, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("model_name", models.CharField(blank=True, max_length=255, null=True)),
                ("size_description", models.CharField(blank=True, max_length=255, null=True)),
                ("category", models.CharField(blank=True, max_length=255, null=True)),
                ("size_component_1", models.CharField(blank=True, max_length=32, null=True)),
                ("size_component_2", models.CharField(blank=True, max_length=32, null=True)),
                ("size_component_3", models.CharField(blank=True, max_length=32, null=True)),
                ("quantity", models.IntegerField(blank=True, null=True)),
                (
                    "cost",
                    models.DecimalField(blank=True, decimal_places=5, max_digits=14, null=True),
                ),
                ("location_id", models.CharField(blank=True, max_length=64, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="parts",
                        to="src.ntwbrand",
                    ),
                ),
            ],
            options={
                "db_table": "ntw_parts",
                "unique_together": {("part_number", "brand")},
            },
        ),
        migrations.CreateModel(
            name="BrandNtwBrandMapping",
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
                        related_name="ntw_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "ntw_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.ntwbrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_ntw_brand_mapping",
                "unique_together": {("brand", "ntw_brand")},
            },
        ),
        migrations.CreateModel(
            name="NtwCompanyPricing",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                (
                    "cost",
                    models.DecimalField(blank=True, decimal_places=5, max_digits=14, null=True),
                ),
                ("quantity", models.IntegerField(blank=True, null=True)),
                ("location_id", models.CharField(blank=True, max_length=64, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="ntw_company_pricing",
                        to="src.company",
                    ),
                ),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.ntwparts",
                    ),
                ),
            ],
            options={
                "db_table": "ntw_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
        migrations.RunPython(create_ntw_provider, remove_ntw_provider),
    ]
