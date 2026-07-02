from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0097_premier_tables"),
    ]

    operations = [
        migrations.AlterField(
            model_name="premierparts",
            name="canada_jobber",
            field=models.DecimalField(decimal_places=2, max_digits=12, null=True),
        ),
    ]
