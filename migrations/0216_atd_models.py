# ATD (American Tire Distributors) integration tables -- brand registry (name-matched, no AAIA
# code in ATD's feed), shared catalog (wheel+tire specs), and per-company cost+stock (ATD ties
# BOTH to the dealer's own locationnumber, unlike every other distributor here -- see
# ATDCompanyPricing's docstring in src/models.py).
#
# Depends directly on 0214 (not the concurrently-in-progress 0215) to avoid coupling to another
# session's unmerged migration -- see ATDBrand/ATDPart/ATDCompanyPricing/BrandATDBrandMapping
# docstrings in src/models.py.
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0214_allpro_models"),
    ]

    operations = [
        migrations.CreateModel(
            name="ATDBrand",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=64)),
                ("name", models.CharField(max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "atd_brands",
                "unique_together": {("external_id",)},
            },
        ),
        migrations.CreateModel(
            name="BrandATDBrandMapping",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="atd_brand_mappings", to="src.brands")),
                ("atd_brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="brand_mappings", to="src.atdbrand")),
            ],
            options={
                "db_table": "brand_atd_brand_mapping",
                "unique_together": {("brand", "atd_brand")},
            },
        ),
        migrations.CreateModel(
            name="ATDPart",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("atdproductnumber", models.CharField(max_length=64, unique=True)),
                ("mfgproductnumber", models.CharField(blank=True, max_length=64, null=True)),
                ("style", models.CharField(blank=True, max_length=255, null=True)),
                ("styleid", models.CharField(blank=True, max_length=64, null=True)),
                ("productgroup", models.CharField(blank=True, max_length=64, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("discontinued", models.BooleanField(default=False)),
                ("replaced", models.BooleanField(default=False)),
                ("size", models.CharField(blank=True, max_length=64, null=True)),
                ("upc", models.CharField(blank=True, max_length=64, null=True)),
                ("shipping_weight", models.CharField(blank=True, max_length=32, null=True)),
                ("country_of_origin", models.CharField(blank=True, max_length=64, null=True)),
                ("wheel_diameter", models.CharField(blank=True, max_length=32, null=True)),
                ("wheel_width", models.CharField(blank=True, max_length=32, null=True)),
                ("bolt_pattern_1", models.CharField(blank=True, max_length=64, null=True)),
                ("bolt_pattern_2", models.CharField(blank=True, max_length=64, null=True)),
                ("bolt_pattern_3", models.CharField(blank=True, max_length=64, null=True)),
                ("bolt_pattern_4", models.CharField(blank=True, max_length=64, null=True)),
                ("hub_bore", models.CharField(blank=True, max_length=32, null=True)),
                ("offset", models.CharField(blank=True, max_length=32, null=True)),
                ("backspacing", models.CharField(blank=True, max_length=32, null=True)),
                ("atd_finish", models.CharField(blank=True, max_length=64, null=True)),
                ("mfg_finish", models.CharField(blank=True, max_length=255, null=True)),
                ("load_rating", models.CharField(blank=True, max_length=32, null=True)),
                ("material", models.CharField(blank=True, max_length=64, null=True)),
                ("number_of_lug_holes", models.CharField(blank=True, max_length=16, null=True)),
                ("tire_width", models.CharField(blank=True, max_length=32, null=True)),
                ("aspect_ratio", models.CharField(blank=True, max_length=32, null=True)),
                ("rim_diameter", models.CharField(blank=True, max_length=32, null=True)),
                ("load_index", models.CharField(blank=True, max_length=32, null=True)),
                ("speed_rating", models.CharField(blank=True, max_length=16, null=True)),
                ("load_range", models.CharField(blank=True, max_length=16, null=True)),
                ("sidewall_description", models.CharField(blank=True, max_length=64, null=True)),
                ("winter_designation", models.CharField(blank=True, max_length=32, null=True)),
                ("seasonal_designation", models.CharField(blank=True, max_length=32, null=True)),
                ("run_flat", models.CharField(blank=True, max_length=16, null=True)),
                ("treadwear", models.CharField(blank=True, max_length=16, null=True)),
                ("image_url", models.TextField(blank=True, null=True)),
                ("raw_images", models.JSONField(blank=True, null=True)),
                ("raw_specs", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("brand", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="parts", to="src.atdbrand")),
            ],
            options={
                "db_table": "atd_parts",
            },
        ),
        migrations.CreateModel(
            name="ATDCompanyPricing",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("cost", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("fet", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("map_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("msrp", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("retail_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("local_qty", models.IntegerField(blank=True, null=True)),
                ("localplus_qty", models.IntegerField(blank=True, null=True)),
                ("nationwide_qty", models.IntegerField(blank=True, null=True)),
                ("onhand_qty", models.IntegerField(blank=True, null=True)),
                ("sthinventory_qty", models.IntegerField(blank=True, null=True)),
                ("last_synced_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("company", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="atd_company_pricing", to="src.company")),
                ("part", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="company_pricing", to="src.atdpart")),
            ],
            options={
                "db_table": "atd_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
    ]
