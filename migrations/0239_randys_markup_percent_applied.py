from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0238_randys_company_pricing"),
    ]

    operations = [
        migrations.AddField(
            model_name="randyscompanypricing",
            name="markup_percent_applied",
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True),
        ),
    ]
