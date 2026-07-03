from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0099_premier_company_pricing_restore_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="integrationpricingsyncjob",
            name="skip_raw_fetch",
            field=models.BooleanField(
                default=False,
                help_text=(
                    "When True, skip the raw distributor data fetch (API/SFTP/CSV) and only run "
                    "the master-parts pricing sync. Set by the nightly pipeline (Phase 1 already "
                    "fetched all raw data). Leave False for on-demand new-company onboarding jobs."
                ),
            ),
        ),
    ]
