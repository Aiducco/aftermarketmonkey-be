from django.db import migrations, models


def backfill_default_order_accounts(apps, schema_editor):
    """
    One-time cutover: every CompanyProviders row that has order credentials embedded in its
    credentials["order"] JSON gets a real CompanyProviderOrderAccount(is_default=True) row
    created from them, and its credentials["order"] is cleared to {} — after this migration,
    CompanyProviderOrderAccount is the only place order credentials live; the JSON field only
    ever carries "feed" going forward (see src.integrations.credentials.get_order_credentials
    and the connect_provider/update_connection/disconnect_provider changes landing alongside
    this migration). Idempotent: skips any connection that already has an is_default row (so
    re-running this migration, or running it after a partial prior attempt, is safe).
    """
    CompanyProviders = apps.get_model("src", "CompanyProviders")
    CompanyProviderOrderAccount = apps.get_model("src", "CompanyProviderOrderAccount")

    for cp in CompanyProviders.objects.all().iterator():
        credentials = cp.credentials or {}
        order_creds = credentials.get("order") or {}
        if not order_creds:
            continue
        if CompanyProviderOrderAccount.objects.filter(company_provider_id=cp.id, is_default=True).exists():
            continue
        CompanyProviderOrderAccount.objects.create(
            company_provider_id=cp.id,
            label="Default",
            credentials=order_creds,
            is_default=True,
            active=True,
        )
        credentials["order"] = {}
        cp.credentials = credentials
        cp.save(update_fields=["credentials"])


def noop_reverse(apps, schema_editor):
    # Not reversible without risking data loss (would need to pick which is_default row, if any,
    # to fold back into the JSON) — a rollback should restore from a DB snapshot instead.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0135_vossen_provider_and_catalog"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyproviderorderaccount",
            name="is_default",
            field=models.BooleanField(default=False),
        ),
        migrations.RunPython(backfill_default_order_accounts, noop_reverse),
    ]
