from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0088_integration_request"),
    ]

    operations = [
        migrations.AddField(
            model_name="providerpartinventory",
            name="product_details",
            field=models.JSONField(null=True),
        ),
    ]
