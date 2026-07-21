from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0112_company_locations"),
    ]

    operations = [
        migrations.CreateModel(
            name="Turn14ItemFitment",
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
                ("item_external_id", models.CharField(max_length=255)),
                ("vehicle_id", models.PositiveIntegerField(db_index=True)),
                ("late_models_only", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "brand",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="item_fitments",
                        to="src.turn14brand",
                    ),
                ),
            ],
            options={
                "db_table": "turn14_item_fitments",
                "unique_together": {("item_external_id", "vehicle_id")},
            },
        ),
    ]
