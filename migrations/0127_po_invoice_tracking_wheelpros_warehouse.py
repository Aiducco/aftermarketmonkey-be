from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0126_meyerlocation"),
    ]

    operations = [
        migrations.CreateModel(
            name="WheelProsWarehouse",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("external_id", models.CharField(max_length=32)),
                ("name", models.CharField(blank=True, max_length=255)),
                ("city", models.CharField(blank=True, max_length=255)),
                ("state", models.CharField(blank=True, max_length=64)),
                ("country", models.CharField(blank=True, max_length=64)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "wheelpros_warehouses",
            },
        ),
        migrations.AlterUniqueTogether(
            name="wheelproswarehouse",
            unique_together={("external_id",)},
        ),
        migrations.AddField(
            model_name="purchaseorderdistributororder",
            name="ship_date",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="purchaseorderdistributororder",
            name="estimated_delivery_date",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="purchaseorderdistributororder",
            name="delivery_status",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AddField(
            model_name="purchaseorderinvoice",
            name="line_items",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
