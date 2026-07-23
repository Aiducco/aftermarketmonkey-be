from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0124_po_ship_to_is_shop_address"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorderlineitem",
            name="quantity_not_orderable",
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
    ]
