from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0091_provider_part_is_discontinued"),
    ]

    operations = [
        migrations.AddField(
            model_name="company",
            name="subscription_plan",
            field=models.CharField(max_length=32, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="company",
            name="subscription_id",
            field=models.CharField(max_length=255, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="company",
            name="subscription_status",
            field=models.CharField(max_length=32, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="company",
            name="subscription_period_end",
            field=models.DateTimeField(null=True, blank=True),
        ),
    ]
