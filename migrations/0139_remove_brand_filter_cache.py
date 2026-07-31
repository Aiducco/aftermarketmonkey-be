from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0138_tirerack_company_pricing"),
    ]

    operations = [
        migrations.DeleteModel(
            name="BrandFilterCache",
        ),
    ]
