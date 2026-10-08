# Mirror platform signups into FreshSales: a Company becomes a sales account, each of its users a
# contact. See src/integrations/services/platform_crm_sync.py and
# docs/INSTANTLY_FRESHSALES_SYNC_PLAN.md.
#
# Hand-written rather than from makemigrations, which also wanted to apply unrelated pre-existing
# drift on this project (a constraint on customintegrationrequest, AutoField -> BigAutoField on a
# dozen models, an initial_sync_completed alteration). None of that is part of this change and none
# of it should ride along on a production deploy, so only the seven fields below are here.
from django.db import migrations, models

# Domains that only ever belong to us: staff, demo, support, smoke tests and pentest accounts.
# Mirrors FRESHSALES_INTERNAL_EMAIL_DOMAINS in conf/settings_base.py -- duplicated rather than
# imported because a migration must keep describing the world as it was when it ran, even after
# someone edits that setting.
_INTERNAL_EMAIL_DOMAINS = frozenset(
    {
        "aftermarketscout.com",
        "test.com",
        "example.com",
        "pentest.local",
        "m.com",
        "dmzapps.com",
        "tridentatx.com",
    }
)

# Companies that are ours but cannot be recognised from their users' email domains. "Trident
# Motorsports" is the clearest case: it is our own entity (see docs/ROUGH_COUNTRY_EDI_PLAN.md,
# where our EDI trading-partner id is TRIDENT), but one of its three users signed up with a gmail
# address, so the domain rule alone leaves it looking like a customer. TICK_PERFORMANCE is the
# seeded admin company; the two "* Corp" rows are security-test signups.
_INTERNAL_COMPANY_NAMES = frozenset(
    {
        "TICK_PERFORMANCE",
        "Trident Motorsports",
        "Trident",
        "DMZ Apps",
        "After",
        "Turn 14 Distribution (Support Account)",
        "AftermarketScout",
        "Pentest Corp",
        "Chain Corp",
    }
)


def mark_internal_companies(apps, schema_editor):
    """
    Flag the companies that existed when this ran and must never reach the CRM.

    A one-time classification of real rows, so the names are spelled out rather than inferred:
    10 of the 27 companies on production at the time are ours, and the remaining 17 are genuine
    customers. Re-runnable, and it only ever sets the flag -- it will not clear one somebody has
    set by hand since.
    """
    Company = apps.get_model("src", "Company")
    UserProfile = apps.get_model("src", "UserProfile")

    emails_by_company = {}
    for company_id, email in UserProfile.objects.exclude(company=None).values_list("company_id", "user__email"):
        if (email or "").strip():
            emails_by_company.setdefault(company_id, []).append(email.strip())

    internal_ids = []
    for company_id, name in Company.objects.values_list("id", "name"):
        domains = {e.rsplit("@", 1)[-1].lower() for e in emails_by_company.get(company_id, [])}
        all_internal_domains = bool(domains) and domains <= _INTERNAL_EMAIL_DOMAINS
        if name in _INTERNAL_COMPANY_NAMES or all_internal_domains:
            internal_ids.append(company_id)

    if internal_ids:
        Company.objects.filter(id__in=internal_ids).update(is_internal=True)


def unmark(apps, schema_editor):
    """Reverse is a no-op: which companies a human has since flagged is not recoverable from here,
    and clearing the lot would quietly push our own accounts into the CRM on the next run."""
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0205_instantly_reply"),
    ]

    operations = [
        migrations.AddField(
            model_name="company",
            name="is_internal",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="company",
            name="freshsales_account_id",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="freshsales_synced_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="company",
            name="freshsales_last_error",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="userprofile",
            name="freshsales_contact_id",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="userprofile",
            name="freshsales_synced_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="userprofile",
            name="freshsales_last_error",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.RunPython(mark_internal_companies, unmark),
    ]
