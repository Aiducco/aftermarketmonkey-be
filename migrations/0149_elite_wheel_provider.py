# Hand-written: only the Elite Wheel & Tire provider models. Kept surgical on purpose -- a plain
# `makemigrations` in this environment also wants to emit unrelated "Alter field id" operations
# (pre-existing BigAutoField drift across many existing tables), which must not be bundled here.
# Same reasoning as 0145_quadratec_provider.

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0148_motorstateproduct_brand"),
    ]

    operations = [
        migrations.CreateModel(
            name="EliteWheelBrand",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=255)),
                ("name", models.CharField(max_length=255)),
                ("product_type", models.CharField(max_length=16)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "elitewheels_brands",
                "unique_together": {("external_id",)},
            },
        ),
        migrations.CreateModel(
            name="EliteWheelPartWheel",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("part_number", models.CharField(max_length=255)),
                ("group_label", models.CharField(blank=True, max_length=512, null=True)),
                ("size", models.CharField(blank=True, max_length=128, null=True)),
                ("bolt_pattern_1", models.CharField(blank=True, max_length=128, null=True)),
                ("bolt_pattern_2", models.CharField(blank=True, max_length=128, null=True)),
                ("offset", models.CharField(blank=True, max_length=64, null=True)),
                ("center_bore", models.CharField(blank=True, max_length=64, null=True)),
                ("finish", models.CharField(blank=True, max_length=255, null=True)),
                ("qty_tampa", models.IntegerField(blank=True, null=True)),
                ("qty_atlanta", models.IntegerField(blank=True, null=True)),
                ("qty_miami", models.IntegerField(blank=True, null=True)),
                ("qty_decatur", models.IntegerField(blank=True, null=True)),
                ("total_available", models.IntegerField(blank=True, null=True)),
                ("location_availability", models.JSONField(blank=True, null=True)),
                ("as_of_date", models.DateField(blank=True, null=True)),
                ("source_filename", models.CharField(blank=True, max_length=255, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="wheel_parts",
                        to="src.elitewheelbrand",
                    ),
                ),
            ],
            options={
                "db_table": "elitewheels_part_wheels",
                "unique_together": {("brand", "part_number")},
            },
        ),
        migrations.CreateModel(
            name="EliteWheelPartTire",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("part_number", models.CharField(max_length=255)),
                ("model", models.CharField(blank=True, max_length=255, null=True)),
                ("raw_size", models.CharField(blank=True, max_length=64, null=True)),
                ("tire_diameter", models.CharField(blank=True, max_length=16, null=True)),
                ("qty_tampa", models.IntegerField(blank=True, null=True)),
                ("qty_atlanta", models.IntegerField(blank=True, null=True)),
                ("qty_miami", models.IntegerField(blank=True, null=True)),
                ("qty_decatur", models.IntegerField(blank=True, null=True)),
                ("total_available", models.IntegerField(blank=True, null=True)),
                ("location_availability", models.JSONField(blank=True, null=True)),
                ("as_of_date", models.DateField(blank=True, null=True)),
                ("source_filename", models.CharField(blank=True, max_length=255, null=True)),
                ("raw_data", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="tire_parts",
                        to="src.elitewheelbrand",
                    ),
                ),
            ],
            options={
                "db_table": "elitewheels_part_tires",
                "unique_together": {("brand", "part_number")},
            },
        ),
        migrations.CreateModel(
            name="EliteWheelWheelCompanyPricing",
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
                        related_name="elite_wheel_wheel_company_pricing",
                        to="src.company",
                    ),
                ),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.elitewheelpartwheel",
                    ),
                ),
            ],
            options={
                "db_table": "elitewheels_wheel_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
        migrations.CreateModel(
            name="EliteWheelTireCompanyPricing",
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
                        related_name="elite_wheel_tire_company_pricing",
                        to="src.company",
                    ),
                ),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.elitewheelparttire",
                    ),
                ),
            ],
            options={
                "db_table": "elitewheels_tire_company_pricing",
                "unique_together": {("part", "company")},
            },
        ),
        migrations.CreateModel(
            name="BrandEliteWheelBrandMapping",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="elite_wheel_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "elite_wheel_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.elitewheelbrand",
                    ),
                ),
            ],
            options={
                "db_table": "brand_elite_wheel_brand_mapping",
                "unique_together": {("brand", "elite_wheel_brand")},
            },
        ),
    ]
