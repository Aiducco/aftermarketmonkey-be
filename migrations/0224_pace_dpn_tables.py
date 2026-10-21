# DIX Performance (DPN) counterparts of the four PaceTse* tables -- same AAM/PACE Meilisearch
# backend, indexes PACE_DPN_categories / PACE_DPN_brands / PACE_DPN_series / PACE_DPN_parts.
# See the models' docstrings in src/models.py.
#
# Hand-written for the same reason as 0222/0223: makemigrations also wants ~25 unrelated
# AlterField operations from pre-existing model/migration drift elsewhere in src.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0223_pace_tse_parts"),
    ]

    operations = [
        migrations.CreateModel(
            name="PaceDpnCategory",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("object_id", models.CharField(max_length=64, unique=True)),
                ("name", models.CharField(db_index=True, max_length=255)),
                ("slug", models.CharField(blank=True, default="", max_length=255)),
                ("parent_name", models.CharField(blank=True, default="", max_length=255)),
                ("parent_slug", models.CharField(blank=True, default="", max_length=255)),
                ("parts", models.JSONField(blank=True, default=list)),
                ("series", models.JSONField(blank=True, default=list)),
                ("brands", models.JSONField(blank=True, default=list)),
                ("site_id", models.CharField(blank=True, default="", max_length=16)),
                ("member_code", models.CharField(blank=True, default="", max_length=16)),
                ("group_name", models.CharField(blank=True, default="", max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "pace_dpn_categories"},
        ),
        migrations.CreateModel(
            name="PaceDpnBrand",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("object_id", models.CharField(max_length=64, unique=True)),
                ("aaia_brand_code", models.CharField(db_index=True, max_length=8)),
                ("name", models.CharField(db_index=True, max_length=255)),
                ("slug", models.CharField(blank=True, default="", max_length=255)),
                ("image", models.TextField(blank=True, default="")),
                ("parts", models.JSONField(blank=True, default=list)),
                ("categories", models.JSONField(blank=True, default=list)),
                ("subcategories", models.JSONField(blank=True, default=list)),
                ("series", models.JSONField(blank=True, default=list)),
                ("site_id", models.CharField(blank=True, default="", max_length=16)),
                ("member_code", models.CharField(blank=True, default="", max_length=16)),
                ("group_name", models.CharField(blank=True, default="", max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "pace_dpn_brands"},
        ),
        migrations.CreateModel(
            name="PaceDpnSeries",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("object_id", models.CharField(max_length=64, unique=True)),
                ("name", models.CharField(db_index=True, max_length=255)),
                ("slug", models.CharField(blank=True, default="", max_length=255)),
                ("brand", models.CharField(blank=True, default="", max_length=255)),
                ("aaia_brand_code", models.CharField(blank=True, db_index=True, default="", max_length=8)),
                ("brand_slug", models.CharField(blank=True, default="", max_length=255)),
                ("parts", models.JSONField(blank=True, default=list)),
                ("categories", models.JSONField(blank=True, default=list)),
                ("subcategories", models.JSONField(blank=True, default=list)),
                ("site_id", models.CharField(blank=True, default="", max_length=16)),
                ("member_code", models.CharField(blank=True, default="", max_length=16)),
                ("group_name", models.CharField(blank=True, default="", max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "pace_dpn_series"},
        ),
        migrations.CreateModel(
            name="PaceDpnPart",
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
            options={"db_table": "pace_dpn_parts"},
        ),
        migrations.AddIndex(
            model_name="pacedpncategory",
            index=models.Index(fields=["parent_slug"], name="pdc_parent_slug_idx"),
        ),
        migrations.AddIndex(
            model_name="pacedpnseries",
            index=models.Index(fields=["aaia_brand_code", "name"], name="pds_brand_name_idx"),
        ),
        migrations.AddIndex(
            model_name="pacedpnpart",
            index=models.Index(fields=["aaia_brand_code", "part"], name="pdp_brand_part_idx"),
        ),
        migrations.AddIndex(
            model_name="pacedpnpart",
            index=models.Index(fields=["aaia_brand_code", "price"], name="pdp_brand_price_idx"),
        ),
    ]
