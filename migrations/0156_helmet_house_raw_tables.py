from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0155_the_wheel_group_provider"),
    ]

    operations = [
        migrations.CreateModel(
            name="BrandHelmetHouseBrandMapping",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "brand_helmet_house_brand_mapping",
            },
        ),
        migrations.CreateModel(
            name="HelmetHouseBrand",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("external_id", models.CharField(max_length=255)),
                ("name", models.CharField(max_length=255)),
                ("source_name", models.CharField(max_length=255, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "helmet_house_brands",
            },
        ),
        migrations.CreateModel(
            name="HelmetHouseCompanyPricing",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "dealer_price",
                    models.DecimalField(decimal_places=2, max_digits=12, null=True),
                ),
                (
                    "retail_price",
                    models.DecimalField(decimal_places=2, max_digits=12, null=True),
                ),
                (
                    "map_price",
                    models.DecimalField(decimal_places=2, max_digits=12, null=True),
                ),
                ("has_map_policy", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "helmet_house_company_pricing",
            },
        ),
        migrations.CreateModel(
            name="HelmetHousePart",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("sku", models.CharField(max_length=255)),
                ("alt_part_number", models.CharField(max_length=255, null=True)),
                ("vendor_part_number", models.CharField(max_length=255, null=True)),
                ("description", models.CharField(max_length=512, null=True)),
                ("long_description", models.TextField(null=True)),
                ("upc", models.CharField(max_length=64, null=True)),
                ("status", models.CharField(max_length=32, null=True)),
                ("category", models.CharField(max_length=255, null=True)),
                ("product_class", models.CharField(max_length=255, null=True)),
                ("size", models.CharField(max_length=64, null=True)),
                ("color", models.CharField(max_length=128, null=True)),
                ("model", models.CharField(max_length=255, null=True)),
                ("country_of_origin", models.CharField(max_length=16, null=True)),
                ("weight", models.FloatField(null=True)),
                ("length", models.FloatField(null=True)),
                ("width", models.FloatField(null=True)),
                ("depth", models.FloatField(null=True)),
                ("photo_filename", models.CharField(max_length=255, null=True)),
                ("alt_photo_filenames", models.JSONField(null=True)),
                ("west_qty", models.IntegerField(default=0)),
                ("east_qty", models.IntegerField(default=0)),
                ("total_qty", models.IntegerField(default=0)),
                ("has_map_policy", models.BooleanField(default=False)),
                ("source_filename", models.CharField(max_length=64, null=True)),
                ("raw_data", models.JSONField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "helmet_house_parts",
            },
        ),
        migrations.AddField(
            model_name="helmethousepart",
            name="brand",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="parts",
                to="src.helmethousebrand",
            ),
        ),
        migrations.AddField(
            model_name="helmethousecompanypricing",
            name="company",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="helmet_house_company_pricing",
                to="src.company",
            ),
        ),
        migrations.AddField(
            model_name="helmethousecompanypricing",
            name="part",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="company_pricing",
                to="src.helmethousepart",
            ),
        ),
        migrations.AddField(
            model_name="brandhelmethousebrandmapping",
            name="brand",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="helmet_house_brand_mappings",
                to="src.brands",
            ),
        ),
        migrations.AddField(
            model_name="brandhelmethousebrandmapping",
            name="helmet_house_brand",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="brand_mappings",
                to="src.helmethousebrand",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="helmethousebrand",
            unique_together={("external_id",)},
        ),
        migrations.AddIndex(
            model_name="helmethousepart",
            index=models.Index(fields=["sku"], name="hh_parts_sku_idx"),
        ),
        migrations.AddIndex(
            model_name="helmethousepart",
            index=models.Index(fields=["status"], name="hh_parts_status_idx"),
        ),
        migrations.AlterUniqueTogether(
            name="helmethousepart",
            unique_together={("brand", "sku")},
        ),
        migrations.AlterUniqueTogether(
            name="helmethousecompanypricing",
            unique_together={("part", "company")},
        ),
        migrations.AlterUniqueTogether(
            name="brandhelmethousebrandmapping",
            unique_together={("brand", "helmet_house_brand")},
        ),
    ]
