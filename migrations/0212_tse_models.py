# Real Tri-State Enterprises (TSE) distributor integration tables -- brand registry, catalog,
# and per-company pricing/inventory. See TSEBrand/TSEParts/TSECompanyPricing docstrings in
# src/models.py. Deliberately TSE-prefixed (not "TriState...") to avoid colliding with the
# already-existing TriStatePart (the one-off Algolia catalog-discovery seed table, pace_catalog's
# sibling -- see that model's own docstring for why it's a separate thing from this).
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0211_tristate_parts"),
    ]

    operations = [
        migrations.CreateModel(
            name="TSEBrand",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("aaia_code", models.CharField(max_length=16, unique=True)),
                ("name", models.CharField(max_length=255)),
                ("warehouse_code", models.CharField(blank=True, max_length=32, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "tse_brands",
            },
        ),
        migrations.CreateModel(
            name="BrandTSEBrandMapping",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="tse_brand_mappings", to="src.brands")),
                ("tse_brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="brand_mappings", to="src.tsebrand")),
            ],
            options={
                "db_table": "brand_tse_brand_mapping",
                "unique_together": {("brand", "tse_brand")},
            },
        ),
        migrations.CreateModel(
            name="TSEParts",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("part_number", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True, null=True)),
                ("image_url", models.TextField(blank=True, null=True)),
                ("category", models.CharField(blank=True, max_length=255, null=True)),
                ("subcategory", models.CharField(blank=True, max_length=255, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="parts", to="src.tsebrand")),
            ],
            options={
                "db_table": "tse_parts",
                "unique_together": {("brand", "part_number")},
            },
        ),
        migrations.CreateModel(
            name="TSECompanyPricing",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("cost", models.DecimalField(decimal_places=2, max_digits=10, null=True)),
                ("list_price", models.DecimalField(decimal_places=2, max_digits=10, null=True)),
                ("quantity_available", models.IntegerField(null=True)),
                ("orderable", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("company", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="tse_company_pricing", to="src.company")),
                ("part", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="company_pricing", to="src.tseparts")),
            ],
            options={
                "db_table": "tse_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
    ]
