from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("migrations", "0086_add_ai_valid_to_lead_email"),
    ]

    operations = [
        migrations.AddField(
            model_name="providers",
            name="coming_soon",
            field=models.BooleanField(default=False),
        ),
    ]
