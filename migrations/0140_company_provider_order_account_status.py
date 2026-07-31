from django.db import migrations, models


def backfill_order_account_status(apps, schema_editor):
    CompanyProviderOrderAccount = apps.get_model("src", "CompanyProviderOrderAccount")
    for account in (
        CompanyProviderOrderAccount.objects.filter(is_default=True)
        .select_related("company_provider")
        .iterator()
    ):
        cp = account.company_provider
        account.order_status = cp.order_status
        account.order_status_name = cp.order_status_name
        account.order_status_reason = cp.order_status_reason
        account.order_status_checked_at = cp.order_status_checked_at
        account.save(
            update_fields=[
                "order_status", "order_status_name", "order_status_reason", "order_status_checked_at",
            ]
        )


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0139_remove_brand_filter_cache"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyproviderorderaccount",
            name="order_status",
            field=models.PositiveSmallIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="companyproviderorderaccount",
            name="order_status_name",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AddField(
            model_name="companyproviderorderaccount",
            name="order_status_reason",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="companyproviderorderaccount",
            name="order_status_checked_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.RunPython(backfill_order_account_status, noop_reverse),
    ]
