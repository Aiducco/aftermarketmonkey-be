from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0090_move_product_details_to_provider_part"),
    ]

    operations = [
        migrations.AddField(
            model_name="providerpart",
            name="is_discontinued",
            field=models.BooleanField(default=False),
        ),
    ]
