from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0120_po_shipments"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorder",
            name="po_name",
            field=models.TextField(null=True, blank=True),
        ),
    ]
