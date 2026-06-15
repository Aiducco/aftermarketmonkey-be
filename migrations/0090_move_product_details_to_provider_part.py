from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0089_provider_part_inventory_product_details"),
    ]

    operations = [
        migrations.AddField(
            model_name="providerpart",
            name="product_details",
            field=models.JSONField(null=True),
        ),
        migrations.RemoveField(
            model_name="providerpartinventory",
            name="product_details",
        ),
    ]
