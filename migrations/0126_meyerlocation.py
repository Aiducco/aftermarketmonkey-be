from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0125_po_line_item_quantity_not_orderable"),
    ]

    operations = [
        migrations.CreateModel(
            name="MeyerLocation",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=32)),
                ("city", models.CharField(blank=True, max_length=255)),
                ("state", models.CharField(blank=True, max_length=64)),
                ("country", models.CharField(blank=True, max_length=64)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "meyer_locations",
            },
        ),
        migrations.AlterUniqueTogether(
            name="meyerlocation",
            unique_together={("external_id",)},
        ),
    ]
