from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0104_onboarding_multi_select_business_type"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyproviders",
            name="status",
            field=models.PositiveSmallIntegerField(
                null=True,
                blank=True,
                help_text=(
                    "Live connectivity/sync status (see CompanyProviderConnectionStatus): "
                    "1=connected, 2=ingesting, 3=waiting, 4=failing. Null until first checked."
                ),
            ),
        ),
        migrations.AddField(
            model_name="companyproviders",
            name="status_name",
            field=models.CharField(max_length=32, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="companyproviders",
            name="status_reason",
            field=models.TextField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="companyproviders",
            name="status_checked_at",
            field=models.DateTimeField(null=True, blank=True),
        ),
    ]
