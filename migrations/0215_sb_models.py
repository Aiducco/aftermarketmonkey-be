# S&B Filters integration tables -- brand registry (S&B / JLT / J&L share one set of workbooks),
# catalog assembled from S&B's two shared dealer sheets, live stock from the forwarded email
# file, per-company resolution of the published dealer price levels, and vehicle fitment
# normalized from the three spellings S&B ship it in. See
# SbBrand/SbPart/SbInventory/SbCompanyPricing/SbFitment docstrings in src/models.py.
#
# Hand-written rather than generated: `makemigrations` on this project also picks up unrelated
# BigAutoField/unique_together drift across a dozen older models, which does not belong in a
# provider migration.
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0214_allpro_models"),
    ]

    operations = [
        migrations.CreateModel(
            name="SbBrand",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=255)),
                ("name", models.CharField(max_length=255)),
                ("aaia_code", models.CharField(blank=True, max_length=255, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "sb_brands",
                "unique_together": {("external_id",)},
            },
        ),
        migrations.CreateModel(
            name="BrandSbBrandMapping",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="sb_brand_mappings", to="src.brands")),
                ("sb_brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="brand_mappings", to="src.sbbrand")),
            ],
            options={
                "db_table": "brand_sb_brand_mapping",
                "unique_together": {("brand", "sb_brand")},
            },
        ),
        migrations.CreateModel(
            name="SbPart",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("sku", models.CharField(max_length=255)),
                ("upc", models.CharField(blank=True, max_length=255, null=True)),
                ("title", models.CharField(blank=True, max_length=512, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("category", models.CharField(blank=True, max_length=255, null=True)),
                ("filter_type", models.CharField(blank=True, max_length=64, null=True)),
                ("vehicle_type", models.CharField(blank=True, max_length=64, null=True)),
                ("shopify_product_type", models.CharField(blank=True, max_length=255, null=True)),
                ("shopify_product_id", models.CharField(blank=True, max_length=64, null=True)),
                ("us_retail_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("canadian_retail_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("level_1_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("level_2_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("weight", models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ("shipping_width", models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ("shipping_height", models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ("shipping_length", models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ("prop_65_warning", models.TextField(blank=True, null=True)),
                ("warning_label_letter", models.CharField(blank=True, max_length=8, null=True)),
                ("emissions_code", models.CharField(blank=True, max_length=16, null=True)),
                ("carb_exempt_id", models.CharField(blank=True, max_length=128, null=True)),
                ("fifty_state_street_legal", models.CharField(blank=True, max_length=16, null=True)),
                ("tune_required", models.CharField(blank=True, max_length=16, null=True)),
                ("iso5011_test_results", models.TextField(blank=True, null=True)),
                ("install_video_url", models.TextField(blank=True, null=True)),
                ("installation_instructions_url", models.TextField(blank=True, null=True)),
                ("quantity_of_oil", models.CharField(blank=True, max_length=64, null=True)),
                ("quantity_of_oil_power_stack", models.CharField(blank=True, max_length=64, null=True)),
                ("image_1", models.TextField(blank=True, null=True)),
                ("image_2", models.TextField(blank=True, null=True)),
                ("image_3", models.TextField(blank=True, null=True)),
                ("image_4", models.TextField(blank=True, null=True)),
                ("image_5", models.TextField(blank=True, null=True)),
                ("image_6", models.TextField(blank=True, null=True)),
                ("images", models.JSONField(blank=True, null=True)),
                ("video", models.TextField(blank=True, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="parts", to="src.sbbrand")),
            ],
            options={
                "db_table": "sb_parts",
                "unique_together": {("brand", "sku")},
            },
        ),
        migrations.AddIndex(
            model_name="sbpart",
            index=models.Index(fields=["sku"], name="sb_parts_sku_idx"),
        ),
        migrations.CreateModel(
            name="SbInventory",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("available_qty", models.IntegerField(blank=True, null=True)),
                ("source_filename", models.CharField(blank=True, max_length=255, null=True)),
                ("last_synced_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("part", models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name="inventory", to="src.sbpart")),
                ("source_company", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="sb_inventory_contributions", to="src.company")),
            ],
            options={
                "db_table": "sb_inventory",
            },
        ),
        migrations.CreateModel(
            name="SbCompanyPricing",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("pricing_level", models.IntegerField(blank=True, null=True)),
                ("cost", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("retail_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("company", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="sb_company_pricing", to="src.company")),
                ("part", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="company_pricing", to="src.sbpart")),
            ],
            options={
                "db_table": "sb_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
        migrations.CreateModel(
            name="SbFitment",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("start_year", models.IntegerField(blank=True, null=True)),
                ("end_year", models.IntegerField(blank=True, null=True)),
                ("make", models.CharField(blank=True, max_length=128, null=True)),
                ("model", models.CharField(blank=True, max_length=128, null=True)),
                ("submodel", models.CharField(blank=True, default="", max_length=255)),
                ("engine", models.CharField(blank=True, default="", max_length=255)),
                ("source", models.CharField(blank=True, max_length=32, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("part", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="fitments", to="src.sbpart")),
            ],
            options={
                "db_table": "sb_fitment",
                "unique_together": {("part", "start_year", "end_year", "make", "model", "submodel", "engine")},
            },
        ),
    ]
