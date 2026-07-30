import django.db.models.deletion
from django.db import migrations, models


def create_vossen_provider(apps, schema_editor):
    Providers = apps.get_model("src", "Providers")
    Providers.objects.get_or_create(
        kind=35,
        defaults={
            "name": "Vossen",
            "status": 1,
            "status_name": "ACTIVE",
            "type": 2,
            "type_name": "DISTRIBUTOR",
            "kind_name": "VOSSEN",
        },
    )


def remove_vossen_provider(apps, schema_editor):
    Providers = apps.get_model("src", "Providers")
    Providers.objects.filter(kind=35).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0134_company_provider_order_account"),
    ]

    operations = [
        migrations.CreateModel(
            name="VossenBrand",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=255)),
                ("name", models.CharField(max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "vossen_brands",
                "unique_together": {("external_id",)},
            },
        ),
        migrations.CreateModel(
            name="VossenPart",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("sku", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True, null=True)),
                ("available", models.IntegerField(blank=True, null=True)),
                ("diameter", models.CharField(blank=True, max_length=64, null=True)),
                ("width", models.CharField(blank=True, max_length=64, null=True)),
                ("offset", models.CharField(blank=True, max_length=64, null=True)),
                ("bolt_pattern", models.CharField(blank=True, max_length=64, null=True)),
                ("center_bore", models.CharField(blank=True, max_length=64, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="parts",
                        to="src.vossenbrand",
                    ),
                ),
            ],
            options={
                "db_table": "vossen_parts",
                "unique_together": {("brand", "sku")},
            },
        ),
        migrations.CreateModel(
            name="VossenCompanyPricing",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="vossen_company_pricing",
                        to="src.company",
                    ),
                ),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.vossenpart",
                    ),
                ),
            ],
            options={
                "db_table": "vossen_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
        migrations.CreateModel(
            name="BrandVossenBrandMapping",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="vossen_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "vossen_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.vossenbrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_vossen_brand_mapping",
                "unique_together": {("brand", "vossen_brand")},
            },
        ),
        migrations.RunPython(create_vossen_provider, remove_vossen_provider),
    ]
