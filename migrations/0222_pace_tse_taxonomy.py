# Three new tables for AAM/PACE's Meilisearch taxonomy for Tri-State's storefront
# (search.theaamgroup.com): PACE_TSE_categories / PACE_TSE_brands / PACE_TSE_series.
#
# Deliberately standalone -- see the models' own docstrings in src/models.py and the
# import_pace_tse_taxonomy command: these are a raw landing zone, not part of the Tri-State
# distributor integration, and nothing in that integration reads them.
#
# Hand-written rather than generated: `makemigrations` currently also wants to emit ~25
# unrelated AlterField operations from pre-existing model/migration drift elsewhere in src, and
# that churn does not belong in this migration.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0221_merge_sb_platform_and_status_backoff"),
    ]

    operations = [
        migrations.CreateModel(
            name="PaceTseCategory",
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
            options={
                "db_table": "pace_tse_categories",
            },
        ),
        migrations.CreateModel(
            name="PaceTseBrand",
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
            options={
                "db_table": "pace_tse_brands",
            },
        ),
        migrations.CreateModel(
            name="PaceTseSeries",
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
            options={
                "db_table": "pace_tse_series",
            },
        ),
        migrations.AddIndex(
            model_name="pacetsecategory",
            index=models.Index(fields=["parent_slug"], name="ptc_parent_slug_idx"),
        ),
        migrations.AddIndex(
            model_name="pacetseseries",
            index=models.Index(fields=["aaia_brand_code", "name"], name="pts_brand_name_idx"),
        ),
    ]
