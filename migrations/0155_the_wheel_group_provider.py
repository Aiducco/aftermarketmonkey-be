# Hand-written: only The Wheel Group provider models. Kept surgical on purpose -- a plain
# `makemigrations` in this environment also wants to emit unrelated "Alter field id" operations
# (pre-existing BigAutoField drift across many existing tables), which must not be bundled here.
# Same reasoning as 0149_elite_wheel_provider.

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0154_wps_enrichment"),
    ]

    operations = [
        migrations.CreateModel(
            name="TheWheelGroupBrand",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=255)),
                ("name", models.CharField(max_length=255)),
                ("aaia_code", models.CharField(blank=True, max_length=255, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "thewheelgroup_brands",
                "unique_together": {("external_id",)},
            },
        ),
        migrations.CreateModel(
            name="TheWheelGroupPart",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("sku", models.CharField(max_length=255)),
                ("aaia_code", models.CharField(blank=True, max_length=255, null=True)),
                ("name", models.CharField(blank=True, max_length=255, null=True)),
                ("style_number", models.CharField(blank=True, max_length=64, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("short_description", models.TextField(blank=True, null=True)),
                ("diameter", models.CharField(blank=True, max_length=32, null=True)),
                ("wheel_width", models.CharField(blank=True, max_length=32, null=True)),
                ("hub_bore", models.CharField(blank=True, max_length=32, null=True)),
                ("bolt_pattern_1", models.CharField(blank=True, max_length=64, null=True)),
                ("bolt_pattern_2", models.CharField(blank=True, max_length=64, null=True)),
                ("offset", models.CharField(blank=True, max_length=32, null=True)),
                ("offset_class", models.CharField(blank=True, max_length=32, null=True)),
                ("backspace", models.CharField(blank=True, max_length=32, null=True)),
                ("wheel_lip_size", models.CharField(blank=True, max_length=32, null=True)),
                ("load_rating", models.CharField(blank=True, max_length=32, null=True)),
                ("color", models.CharField(blank=True, max_length=64, null=True)),
                ("finish", models.CharField(blank=True, max_length=128, null=True)),
                ("upc", models.CharField(blank=True, max_length=64, null=True)),
                ("country_of_origin", models.CharField(blank=True, max_length=64, null=True)),
                ("division", models.CharField(blank=True, max_length=64, null=True)),
                ("group_code", models.CharField(blank=True, max_length=64, null=True)),
                ("wheel_cap", models.CharField(blank=True, max_length=64, null=True)),
                ("screw", models.CharField(blank=True, max_length=64, null=True)),
                ("dually_wheel", models.BooleanField(blank=True, null=True)),
                ("winter_approved", models.BooleanField(blank=True, null=True)),
                ("tpms_compatible", models.BooleanField(blank=True, null=True)),
                ("lugnut_open_closed", models.CharField(blank=True, max_length=32, null=True)),
                ("lugnut_type_1", models.CharField(blank=True, max_length=32, null=True)),
                ("lugnut_type_2", models.CharField(blank=True, max_length=32, null=True)),
                ("lugseat_type", models.CharField(blank=True, max_length=32, null=True)),
                ("structure_warranty", models.CharField(blank=True, max_length=128, null=True)),
                ("finish_warranty", models.CharField(blank=True, max_length=128, null=True)),
                ("beadlock_instructions_url", models.CharField(blank=True, max_length=512, null=True)),
                ("box_width", models.CharField(blank=True, max_length=32, null=True)),
                ("box_height", models.CharField(blank=True, max_length=32, null=True)),
                ("box_depth", models.CharField(blank=True, max_length=32, null=True)),
                ("product_weight", models.CharField(blank=True, max_length=32, null=True)),
                ("ship_weight", models.CharField(blank=True, max_length=32, null=True)),
                ("image_1", models.CharField(blank=True, max_length=1024, null=True)),
                ("image_2", models.CharField(blank=True, max_length=1024, null=True)),
                ("image_3", models.CharField(blank=True, max_length=1024, null=True)),
                ("image_4", models.CharField(blank=True, max_length=1024, null=True)),
                ("note", models.TextField(blank=True, null=True)),
                ("comment", models.TextField(blank=True, null=True)),
                ("bullet_points", models.TextField(blank=True, null=True)),
                ("sales_description", models.TextField(blank=True, null=True)),
                ("msrp", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("map_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("map_enforced", models.BooleanField(blank=True, null=True)),
                ("source_filename", models.CharField(blank=True, max_length=255, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="parts",
                        to="src.thewheelgroupbrand",
                    ),
                ),
            ],
            options={
                "db_table": "thewheelgroup_parts",
                "unique_together": {("brand", "sku")},
            },
        ),
        migrations.CreateModel(
            name="TheWheelGroupCompanyPricing",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("cost", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("map", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("retail_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="the_wheel_group_company_pricing",
                        to="src.company",
                    ),
                ),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.thewheelgrouppart",
                    ),
                ),
            ],
            options={
                "db_table": "thewheelgroup_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
        migrations.CreateModel(
            name="BrandTheWheelGroupBrandMapping",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="the_wheel_group_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "the_wheel_group_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.thewheelgroupbrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_the_wheel_group_brand_mapping",
                "unique_together": {("brand", "the_wheel_group_brand")},
            },
        ),
    ]
