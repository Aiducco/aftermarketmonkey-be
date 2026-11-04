from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0244_external_order_estimated_delivery"),
    ]

    operations = [
        migrations.AddField(
            model_name="keystoneparts",
            name="is_discontinued",
            field=models.BooleanField(default=False),
        ),
    ]
