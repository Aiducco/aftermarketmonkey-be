from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0233_ntw_models"),
    ]

    operations = [
        migrations.CreateModel(
            name="NtwLocation",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("external_id", models.CharField(max_length=32)),
                ("name", models.CharField(blank=True, max_length=255)),
                ("street", models.CharField(blank=True, max_length=255)),
                ("city", models.CharField(blank=True, max_length=255)),
                ("state", models.CharField(blank=True, max_length=64)),
                ("country", models.CharField(blank=True, max_length=64)),
                ("zip_code", models.CharField(blank=True, max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "ntw_locations",
                "unique_together": {("external_id",)},
            },
        ),
    ]
