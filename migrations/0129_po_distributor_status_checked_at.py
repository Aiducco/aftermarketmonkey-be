from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0128_merge_shop_management_and_po_invoice_tracking"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorder",
            name="distributor_status_checked_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
