# New table for AAM/PACE's shared Algolia catalog data -- see PaceCatalogPart's own docstring
# in src/models.py for what this is and why it deliberately excludes per-distributor inventory.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0209_brand_providers_part_count"),
    ]

    operations = [
        migrations.CreateModel(
            name="PaceCatalogPart",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("object_id", models.CharField(max_length=64, unique=True)),
                ("aaia_brand_code", models.CharField(db_index=True, max_length=8)),
                ("part_number", models.CharField(db_index=True, max_length=64)),
                ("name", models.TextField(blank=True, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("category", models.CharField(blank=True, max_length=255, null=True)),
                ("subcategory", models.CharField(blank=True, max_length=255, null=True)),
                ("series", models.CharField(blank=True, max_length=255, null=True)),
                ("part_search", models.JSONField(blank=True, default=list)),
                ("pace_brand", models.CharField(blank=True, max_length=255, null=True)),
                ("pace_name", models.TextField(blank=True, null=True)),
                ("pace_image", models.TextField(blank=True, null=True)),
                ("pace_slug", models.TextField(blank=True, null=True)),
                ("aw_brand", models.CharField(blank=True, max_length=255, null=True)),
                ("aw_name", models.TextField(blank=True, null=True)),
                ("aw_image", models.TextField(blank=True, null=True)),
                ("aw_slug", models.TextField(blank=True, null=True)),
                ("fitment", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "pace_catalog",
            },
        ),
    ]
