# Hand-written: only the Quadratec provider models. Kept surgical on purpose -- a plain
# `makemigrations` in this environment also wants to emit unrelated "Alter field id" operations
# (pre-existing BigAutoField drift across many existing tables), which must not be bundled here.

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0144_company_providers_pricing_propagation_watermark"),
    ]

    operations = [
        migrations.CreateModel(
            name="QuadratecBrand",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=255)),
                ("name", models.CharField(max_length=255)),
                ("aaia_code", models.CharField(blank=True, max_length=255, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "quadratec_brands",
                "unique_together": {("external_id",)},
            },
        ),
        migrations.CreateModel(
            name="QuadratecPart",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("sku", models.CharField(max_length=255)),
                ("mpn", models.CharField(blank=True, max_length=255, null=True)),
                ("title", models.CharField(blank=True, max_length=512, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("upc", models.CharField(blank=True, max_length=255, null=True)),
                ("inv_pa1", models.IntegerField(blank=True, null=True)),
                ("inv_pa2", models.IntegerField(blank=True, null=True)),
                ("inv_nv1", models.IntegerField(blank=True, null=True)),
                ("inv_total", models.IntegerField(blank=True, null=True)),
                ("shipping_surcharge", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="parts",
                        to="src.quadratecbrand",
                    ),
                ),
            ],
            options={
                "db_table": "quadratec_parts",
                "unique_together": {("brand", "sku")},
            },
        ),
        migrations.CreateModel(
            name="QuadratecCompanyPricing",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("retail_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("wholesale_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("cost", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("map", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="quadratec_company_pricing",
                        to="src.company",
                    ),
                ),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.quadratecpart",
                    ),
                ),
            ],
            options={
                "db_table": "quadratec_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
        migrations.CreateModel(
            name="BrandQuadratecBrandMapping",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="quadratec_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "quadratec_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.quadratecbrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_quadratec_brand_mapping",
                "unique_together": {("brand", "quadratec_brand")},
            },
        ),
    ]
