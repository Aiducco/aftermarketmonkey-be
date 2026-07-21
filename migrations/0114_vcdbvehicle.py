from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0113_turn14_item_fitment"),
    ]

    operations = [
        migrations.CreateModel(
            name="VcdbVehicle",
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
                ("vehicle_id", models.PositiveIntegerField(unique=True)),
                ("base_vehicle_id", models.PositiveIntegerField(db_index=True)),
                ("year", models.PositiveSmallIntegerField(db_index=True)),
                ("make", models.CharField(db_index=True, max_length=128)),
                ("model", models.CharField(db_index=True, max_length=128)),
                ("submodel", models.CharField(blank=True, default="", max_length=255)),
                ("region_id", models.PositiveSmallIntegerField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "vcdb_vehicles",
            },
        ),
        migrations.AddIndex(
            model_name="vcdbvehicle",
            index=models.Index(fields=["year", "make", "model"], name="vcdb_veh_ymm_idx"),
        ),
    ]
