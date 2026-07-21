from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0116_namespace_company_providers_credentials"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyproviders",
            name="order_status",
            field=models.PositiveSmallIntegerField(
                null=True,
                blank=True,
                help_text=(
                    "Order-placement connectivity status (see CompanyProviderOrderConnectionStatus): "
                    "1=connected, 2=waiting (on feed), 3=error. Null until order credentials are entered."
                ),
            ),
        ),
        migrations.AddField(
            model_name="companyproviders",
            name="order_status_name",
            field=models.CharField(max_length=32, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="companyproviders",
            name="order_status_reason",
            field=models.TextField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="companyproviders",
            name="order_status_checked_at",
            field=models.DateTimeField(null=True, blank=True),
        ),
    ]
