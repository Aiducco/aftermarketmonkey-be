from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0102_masterpart_gtin"),
    ]

    operations = [
        migrations.CreateModel(
            name="BrandFilterCache",
            fields=[
                (
                    "brand",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        primary_key=True,
                        related_name="filter_cache",
                        serialize=False,
                        to="src.brands",
                    ),
                ),
                ("name", models.CharField(max_length=255)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "brand_filter_cache",
                "ordering": ["name"],
            },
        ),
        migrations.AddIndex(
            model_name="brandfiltercache",
            index=models.Index(fields=["name"], name="brand_filter_cache_name_idx"),
        ),
    ]
