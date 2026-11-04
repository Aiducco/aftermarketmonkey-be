from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0245_keystone_parts_is_discontinued"),
    ]

    operations = [
        migrations.RenameField(
            model_name="keystoneparts",
            old_name="is_discontinued",
            new_name="is_special_order",
        ),
        migrations.AddField(
            model_name="providerpart",
            name="is_special_order",
            field=models.BooleanField(default=False),
        ),
    ]
