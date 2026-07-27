from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0130_po_distributor_order_po_number"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorderdistributororder",
            name="distributor_internal_order_number",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AddField(
            model_name="purchaseorderdistributororder",
            name="distributor_order_status",
            field=models.PositiveSmallIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="purchaseorderdistributororder",
            name="distributor_order_status_name",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
    ]
