# Tri-State moved from "API key in the feed namespace" to Meyer's two-namespace shape: the FEED
# is now an SFTP relay delivery (relay sftp_user/sftp_password, issued by us) and the PACE
# App-Id/Api-Key pair is ORDER credentials, which live on a CompanyProviderOrderAccount rather
# than in CompanyProviders.credentials (see src/integrations/credentials.py).
#
# Without this, the one existing connection keeps a PACE pair sitting in credentials.feed, where
# every reader now expects relay credentials: the integrations catalog would call it feed-connected
# off those keys, and the live-inventory/pricing paths would look for the API pair on an order
# account that does not exist, so the credentials would simply be orphaned.
#
# Idempotent and non-destructive: only rows whose feed still carries app_id/api_key are touched,
# an existing order account is never overwritten, and the feed is only replaced once the company
# actually has a provisioned relay account (otherwise it is left alone for connect_provider to
# fill in, which is the one path that can provision).
from django.db import migrations

_TRI_STATE_KIND = 38

# src.enums.CompanyProviderConnectionStatus.WAITING -- frozen here rather than imported, like
# the kind above: a migration must keep meaning the same thing if the enum is ever renumbered.
_STATUS_WAITING = 3


def forwards(apps, schema_editor):
    CompanyProviders = apps.get_model("src", "CompanyProviders")
    CompanyProviderOrderAccount = apps.get_model("src", "CompanyProviderOrderAccount")

    rows = CompanyProviders.objects.filter(provider__kind=_TRI_STATE_KIND).select_related("company")
    for cp in rows:
        credentials = dict(cp.credentials or {})
        feed = dict(credentials.get("feed") or {})
        app_id = (feed.get("app_id") or "").strip()
        api_key = (feed.get("api_key") or "").strip()
        if not app_id and not api_key:
            continue

        if not CompanyProviderOrderAccount.objects.filter(company_provider_id=cp.id).exists():
            CompanyProviderOrderAccount.objects.create(
                company_provider_id=cp.id,
                label="Default",
                credentials={"app_id": app_id, "api_key": api_key},
                order_method=1,
                order_method_name="API",
                is_default=True,
                active=True,
            )

        company = cp.company
        if company and company.relay_sftp_username and company.relay_sftp_password:
            credentials["feed"] = {
                "sftp_user": company.relay_sftp_username,
                "sftp_password": company.relay_sftp_password,
            }
        else:
            # No relay account yet, so there is no correct value to write. Clearing it is the
            # honest state -- nothing has been delivered -- and connect_provider provisions and
            # fills it on the next connect.
            credentials["feed"] = {}
        credentials.pop("order", None)  # never persisted here; see credentials.get_order_credentials
        cp.credentials = credentials

        # The stored status has to go back to WAITING too. It says CONNECTED, which under the
        # old shape meant "the PACE key authenticated" -- under the new one the feed status
        # answers "has Tri-State delivered a file", and none ever has. Leaving it would show a
        # dealer a connected feed that has received nothing. initial_sync_completed goes back to
        # False for the same reason, and it is also what puts the row back in
        # check_company_provider_connections' scope so the cron maintains it from here.
        cp.initial_sync_completed = False
        cp.status = _STATUS_WAITING
        cp.status_name = "WAITING"
        cp.status_reason = (
            "Please read the setup instructions and ask your rep to push your feed file to our relay."
        )
        cp.status_checked_at = None
        cp.status_consecutive_failures = 0
        cp.status_next_check_at = None
        cp.save(update_fields=[
            "credentials",
            "initial_sync_completed",
            "status",
            "status_name",
            "status_reason",
            "status_checked_at",
            "status_consecutive_failures",
            "status_next_check_at",
        ])


def backwards(apps, schema_editor):
    """
    Put the PACE pair back in credentials.feed and drop the order account this created. The
    relay credentials written into feed are not recoverable (they were never there before), so
    going back restores the API pair, which is what the old code read from feed.

    The status fields are deliberately NOT restored: the old CONNECTED was a statement about a
    key that PACE now rejects, so re-asserting it would be worse than leaving the row to be
    re-checked.
    """
    CompanyProviders = apps.get_model("src", "CompanyProviders")
    CompanyProviderOrderAccount = apps.get_model("src", "CompanyProviderOrderAccount")

    for cp in CompanyProviders.objects.filter(provider__kind=_TRI_STATE_KIND):
        account = CompanyProviderOrderAccount.objects.filter(
            company_provider_id=cp.id, is_default=True
        ).first()
        if not account:
            continue
        account_credentials = account.credentials or {}
        if not (account_credentials.get("app_id") or account_credentials.get("api_key")):
            continue
        credentials = dict(cp.credentials or {})
        credentials["feed"] = {
            "app_id": account_credentials.get("app_id", ""),
            "api_key": account_credentials.get("api_key", ""),
        }
        cp.credentials = credentials
        cp.save(update_fields=["credentials"])
        account.delete()


class Migration(migrations.Migration):
    dependencies = [("src", "0227_brand_pace_brand_mapping")]
    operations = [migrations.RunPython(forwards, backwards)]
