from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0123_po_invoices"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorder",
            name="ship_to_is_shop_address",
            field=models.BooleanField(default=False),
        ),
    ]
