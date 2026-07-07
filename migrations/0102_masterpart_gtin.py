from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0101_company_providers_initial_sync_completed"),
    ]

    operations = [
        migrations.AddField(
            model_name="masterpart",
            name="gtin",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
