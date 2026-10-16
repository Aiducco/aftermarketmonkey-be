# Real AllPro Distributing integration tables -- brand registry, catalog+inventory (bulk FTP
# feed, not per-item API like Tri-State), and per-company pricing. See AllProBrand/AllProParts/
# AllProCompanyPricing docstrings in src/models.py.
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0213_tse_inventory_on_parts"),
    ]

    operations = [
        migrations.CreateModel(
            name="AllProBrand",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("line_code", models.CharField(max_length=16)),
                ("aaia_code", models.CharField(max_length=16, unique=True)),
                ("name", models.CharField(max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "allpro_brands",
            },
        ),
        migrations.CreateModel(
            name="BrandAllProBrandMapping",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="allpro_brand_mappings", to="src.brands")),
                ("allpro_brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="brand_mappings", to="src.allprobrand")),
            ],
            options={
                "db_table": "brand_allpro_brand_mapping",
                "unique_together": {("brand", "allpro_brand")},
            },
        ),
        migrations.CreateModel(
            name="AllProParts",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("full_part_number", models.CharField(max_length=255, unique=True)),
                ("part_number", models.CharField(db_index=True, max_length=255)),
                ("line_code", models.CharField(blank=True, max_length=16)),
                ("length", models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ("width", models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ("height", models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ("weight", models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ("upc", models.CharField(blank=True, max_length=64, null=True)),
                ("warehouse_va_qty", models.IntegerField(blank=True, null=True)),
                ("warehouse_fl_qty", models.IntegerField(blank=True, null=True)),
                ("warehouse_al_qty", models.IntegerField(blank=True, null=True)),
                ("total_qty", models.IntegerField(blank=True, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("image_url", models.TextField(blank=True, null=True)),
                ("category", models.CharField(blank=True, max_length=255, null=True)),
                ("subcategory", models.CharField(blank=True, max_length=255, null=True)),
                ("series", models.CharField(blank=True, max_length=255, null=True)),
                ("enriched_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="parts", to="src.allprobrand")),
            ],
            options={
                "db_table": "allpro_parts",
            },
        ),
        migrations.AddIndex(
            model_name="allproparts",
            index=models.Index(fields=["brand", "part_number"], name="allpro_brand_part_idx"),
        ),
        migrations.CreateModel(
            name="AllProCompanyPricing",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("cost", models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("company", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="allpro_company_pricing", to="src.company")),
                ("part", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="company_pricing", to="src.allproparts")),
            ],
            options={
                "db_table": "allpro_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
    ]
