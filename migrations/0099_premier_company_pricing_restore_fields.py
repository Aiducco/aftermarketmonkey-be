from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0098_premier_canada_jobber_precision"),
    ]

    operations = [
        migrations.AddField(
            model_name="premiercompanypricing",
            name="jobber_price",
            field=models.DecimalField(decimal_places=4, max_digits=10, null=True),
        ),
        migrations.AddField(
            model_name="premiercompanypricing",
            name="map_price",
            field=models.DecimalField(decimal_places=4, max_digits=10, null=True),
        ),
        migrations.AddField(
            model_name="premiercompanypricing",
            name="core_charge",
            field=models.DecimalField(decimal_places=4, max_digits=10, null=True),
        ),
        migrations.AlterField(
            model_name="premiercompanypricing",
            name="customer_price",
            field=models.DecimalField(decimal_places=4, max_digits=10, null=True),
        ),
        migrations.AlterField(
            model_name="premiercompanypricing",
            name="customer_cad_price",
            field=models.DecimalField(decimal_places=4, max_digits=10, null=True),
        ),
    ]
