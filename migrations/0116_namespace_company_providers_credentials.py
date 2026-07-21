"""
Reshapes CompanyProviders.credentials from a flat dict into {"feed": {...}, "order": {...}}.
"feed" holds whatever the vendor's catalog/pricing sync already used (FTP/SFTP/OAuth/etc,
unchanged in value). "order" is only populated here for Turn14 (kind=1), the one vendor whose
existing feed credentials already double as order-placement credentials — every other vendor
gets order credentials added later, explicitly, once/if an order adapter is connected for it.

Idempotent: skips rows whose credentials dict already has a "feed" key (safe to re-run).
Kind value is hardcoded (not imported from src.enums) per Django migration convention — app
code may change independently of historical migrations.
"""
from django.db import migrations

_TURN_14_KIND = 1


def _namespace_credentials(apps, schema_editor):
    CompanyProviders = apps.get_model("src", "CompanyProviders")
    for cp in CompanyProviders.objects.select_related("provider").iterator():
        creds = cp.credentials
        if not isinstance(creds, dict) or "feed" in creds:
            continue
        new_creds = {"feed": creds}
        if cp.provider_id and cp.provider.kind == _TURN_14_KIND:
            new_creds["order"] = dict(creds)
        cp.credentials = new_creds
        cp.save(update_fields=["credentials"])


def _unnamespace_credentials(apps, schema_editor):
    CompanyProviders = apps.get_model("src", "CompanyProviders")
    for cp in CompanyProviders.objects.iterator():
        creds = cp.credentials
        if not isinstance(creds, dict) or "feed" not in creds:
            continue
        feed = creds.get("feed") or {}
        order = creds.get("order") or {}
        flat = dict(feed)
        if order and order != feed:
            # Distinct order credentials (e.g. Keystone's separate order-API account/security
            # key, entered after this migration first ran) have nowhere to live in the old flat
            # shape. Stash them under a backup key instead of silently discarding them on
            # rollback -- nothing reads this key, it's a recovery breadcrumb, not live data.
            flat["_unmigrated_order_credentials_backup"] = order
        cp.credentials = flat
        cp.save(update_fields=["credentials"])


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0115_vcdbvehicle_engine_drive_type"),
    ]
    operations = [
        migrations.RunPython(_namespace_credentials, reverse_code=_unnamespace_credentials),
    ]
