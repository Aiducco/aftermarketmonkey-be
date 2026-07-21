from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0114_vcdbvehicle"),
    ]

    operations = [
        migrations.AddField(
            model_name="vcdbvehicle",
            name="engine",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="vcdbvehicle",
            name="drive_type",
            field=models.CharField(blank=True, default="", max_length=64),
        ),
    ]
