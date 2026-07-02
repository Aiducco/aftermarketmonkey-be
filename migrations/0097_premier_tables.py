from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0096_company_providers_active"),
    ]

    operations = [
        migrations.CreateModel(
            name="PremierBrand",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=255)),
                ("name", models.CharField(max_length=255)),
                ("line_code", models.CharField(blank=True, max_length=64, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "premier_brands"},
        ),
        migrations.AddConstraint(
            model_name="premierbrand",
            constraint=models.UniqueConstraint(fields=["external_id"], name="premier_brands_external_id_uniq"),
        ),
        migrations.CreateModel(
            name="BrandPremierBrandMapping",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="premier_brand_mappings",
                        to="src.brands",
                    ),
                ),
                (
                    "premier_brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="brand_mappings",
                        to="src.premierbrand",
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "brand_premier_brand_mapping"},
        ),
        migrations.AlterUniqueTogether(
            name="brandpremierbrandmapping",
            unique_together={("brand", "premier_brand")},
        ),
        migrations.CreateModel(
            name="PremierParts",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("premier_part_number", models.CharField(max_length=255)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="parts",
                        to="src.premierbrand",
                    ),
                ),
                ("mfg_part_number", models.CharField(max_length=255, null=True)),
                ("long_description", models.TextField(null=True)),
                ("external_long_description", models.TextField(null=True)),
                ("length", models.DecimalField(decimal_places=3, max_digits=10, null=True)),
                ("width", models.DecimalField(decimal_places=3, max_digits=10, null=True)),
                ("height", models.DecimalField(decimal_places=3, max_digits=10, null=True)),
                ("weight", models.DecimalField(decimal_places=3, max_digits=10, null=True)),
                ("upc_code", models.CharField(max_length=255, null=True)),
                ("usa_item_availability", models.IntegerField(null=True)),
                ("core_charge", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("jobber_price", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("map_price", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("retail_price", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("inventory_status", models.CharField(max_length=64, null=True)),
                ("nv_qty", models.IntegerField(null=True)),
                ("ky_qty", models.IntegerField(null=True)),
                ("mfg_qty", models.IntegerField(null=True)),
                ("wa_qty", models.IntegerField(null=True)),
                ("image_url", models.TextField(null=True)),
                ("ships_ltl", models.BooleanField(default=False)),
                ("item_with_cores", models.BooleanField(default=False)),
                ("prop65_carcinogen", models.BooleanField(default=False)),
                ("prop65_reproductive_harm", models.BooleanField(default=False)),
                ("approved_line", models.BooleanField(default=False)),
                ("california_legal", models.BooleanField(default=False)),
                ("line_code", models.CharField(max_length=64, null=True)),
                ("pies_ems_code", models.CharField(max_length=64, null=True)),
                ("drop_ship_fee", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("canada_map", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("canada_msrp", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("canada_jobber", models.DecimalField(decimal_places=5, max_digits=10, null=True)),
                ("part_category", models.CharField(max_length=255, null=True)),
                ("part_subcategory", models.CharField(max_length=255, null=True)),
                ("part_terminology", models.CharField(max_length=255, null=True)),
                ("freight_cost", models.DecimalField(decimal_places=3, max_digits=10, null=True)),
                ("minimum_order_qty", models.IntegerField(null=True)),
                ("drop_shippable_from_mfg", models.BooleanField(default=False)),
                ("vendor_enhanced_emissions_code", models.CharField(max_length=255, null=True)),
                ("is_kit", models.BooleanField(default=False)),
                ("kit_component_list", models.TextField(null=True)),
                ("raw_data", models.JSONField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "premier_parts"},
        ),
        migrations.AlterUniqueTogether(
            name="premierparts",
            unique_together={("premier_part_number", "brand")},
        ),
        migrations.CreateModel(
            name="PremierCompanyPricing",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "part",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="company_pricing",
                        to="src.premierparts",
                    ),
                ),
                (
                    "company",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="premier_company_pricing",
                        to="src.company",
                    ),
                ),
                ("customer_price", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("jobber_price", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("map_price", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("core_charge", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("customer_cad_price", models.DecimalField(decimal_places=4, max_digits=10, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "premier_company_pricing"},
        ),
        migrations.AlterUniqueTogether(
            name="premiercompanypricing",
            unique_together={("part", "company")},
        ),
    ]
