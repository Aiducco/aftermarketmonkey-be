# New table for AAM/PACE's PACE_TSE_parts Meilisearch index -- see PaceTsePart's docstring in
# src/models.py and the import_pace_tse_parts command for the price-point slicing this needs.
#
# Hand-written for the same reason as 0222: makemigrations also wants ~25 unrelated AlterField
# operations from pre-existing model/migration drift elsewhere in src.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0222_pace_tse_taxonomy"),
    ]

    operations = [
        migrations.CreateModel(
            name="PaceTsePart",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("object_id", models.CharField(max_length=64, unique=True)),
                ("part", models.CharField(db_index=True, max_length=128)),
                ("aaia_brand_code", models.CharField(db_index=True, max_length=8)),
                ("catalog_object_id", models.CharField(blank=True, default="", max_length=255)),
                ("stock_number", models.CharField(blank=True, default="", max_length=128)),
                ("brand", models.CharField(blank=True, default="", max_length=255)),
                ("name", models.TextField(blank=True, default="")),
                ("ext", models.TextField(blank=True, default="")),
                ("slug", models.TextField(blank=True, default="")),
                ("image", models.TextField(blank=True, default="")),
                ("has_image", models.BooleanField(default=False)),
                ("category", models.CharField(blank=True, default="", max_length=255)),
                ("subcategory", models.CharField(blank=True, default="", max_length=255)),
                ("series", models.CharField(blank=True, default="", max_length=255)),
                ("price", models.DecimalField(blank=True, decimal_places=4, max_digits=12, null=True)),
                ("inventory", models.IntegerField(default=0)),
                ("sales", models.IntegerField(default=0)),
                ("viewed", models.IntegerField(default=0)),
                ("aces", models.IntegerField(db_index=True, default=0)),
                ("part_search", models.JSONField(blank=True, default=list)),
                ("makes", models.JSONField(blank=True, default=list)),
                ("models", models.JSONField(blank=True, default=list)),
                ("years", models.JSONField(blank=True, default=list)),
                ("fitment", models.JSONField(blank=True, default=dict)),
                ("site_id", models.CharField(blank=True, default="", max_length=16)),
                ("member_code", models.CharField(blank=True, default="", max_length=16)),
                ("group_name", models.CharField(blank=True, default="", max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "pace_tse_parts",
            },
        ),
        migrations.AddIndex(
            model_name="pacetsepart",
            index=models.Index(fields=["aaia_brand_code", "part"], name="ptp_brand_part_idx"),
        ),
        migrations.AddIndex(
            model_name="pacetsepart",
            index=models.Index(fields=["aaia_brand_code", "price"], name="ptp_brand_price_idx"),
        ),
    ]
