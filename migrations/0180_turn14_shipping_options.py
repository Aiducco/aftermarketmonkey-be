from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0179_tire_load_range"),
    ]

    operations = [
        migrations.CreateModel(
            name="Turn14ShippingOption",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=32)),
                ("transportation_name", models.CharField(max_length=255)),
                ("carrier_name", models.CharField(blank=True, max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "turn14_shipping_options",
                "unique_together": {("external_id",)},
            },
        ),
    ]
