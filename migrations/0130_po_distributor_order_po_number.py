from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0129_po_distributor_status_checked_at"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseorderdistributororder",
            name="po_number",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
    ]
