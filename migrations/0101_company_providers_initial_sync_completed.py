from django.db import migrations, models


def mark_existing_as_completed(apps, schema_editor):
    """
    All CompanyProviders rows that existed before this migration are assumed to have
    already had their initial pricing sync (they were live before this feature).
    Set initial_sync_completed=True so they don't show as "Ingesting..." in the UI.
    Only newly-created rows (after this migration) will start with the default False.
    """
    CompanyProviders = apps.get_model("src", "CompanyProviders")
    CompanyProviders.objects.all().update(initial_sync_completed=True)


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0100_integration_pricing_sync_job_skip_raw_fetch"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyproviders",
            name="initial_sync_completed",
            field=models.BooleanField(
                default=False,
                help_text=(
                    "True once the first successful pricing sync completes for this "
                    "connection. False means initial data ingest is pending or in progress "
                    "— frontend should show a setup/loading state."
                ),
            ),
        ),
        migrations.RunPython(
            mark_existing_as_completed,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
