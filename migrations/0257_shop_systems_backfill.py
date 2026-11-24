# Shop systems per space, step 2 of 3 (data only): every existing shop-system connection and every ShopMonkey
# sync vendor, brand and execution row is put in its company's main space (AC-16: the main space keeps today's
# connection, brands and history), and each connection's account fingerprint is computed from the identity
# fields its system's catalog entry names. Idempotent: only rows whose `space` is still NULL are touched, and a
# fingerprint is only written where it is still NULL. The main space is the one 0252 made (a company without one
# gets it created the same way). The sync tables are small: one UPDATE per company and table.
# Reverse: NULL the four `space` columns and the fingerprints (the columns themselves go with 0256).
from django.db import migrations

from src import constants as src_constants
from src.domain import account_fingerprint

KIND_LOCATION = 1

SYNC_MODELS = ("ShopMonkeySyncVendor", "ShopMonkeySyncBrand", "ShopMonkeySyncExecution")


def _main_space_id(apps, company_id):
    Space = apps.get_model("src", "Space")
    CompanyLocation = apps.get_model("src", "CompanyLocation")
    Company = apps.get_model("src", "Company")
    main = Space.objects.filter(company_id=company_id, is_main=True).first()
    if main is None:
        primary = CompanyLocation.objects.filter(company_id=company_id, is_primary=True).order_by("id").first()
        main = Space.objects.create(
            company_id=company_id,
            kind=KIND_LOCATION,
            kind_name="LOCATION",
            name=primary.label if primary else Company.objects.values_list("name", flat=True).get(id=company_id),
            is_main=True,
            location=primary if primary and not Space.objects.filter(location=primary).exists() else None,
        )
    return main.id


def _identity_fields_by_kind():
    out = {}
    for entry in src_constants.SHOP_MANAGEMENT_PROVIDER_CATALOG:
        fields = entry.get("account_identity_fields")
        if fields is None:
            fields = entry.get("connection_required_fields") or []
        out[entry["kind"].value] = list(fields)
    return out


def backfill_shop_spaces(apps, schema_editor):
    Connection = apps.get_model("src", "CompanyShopManagementProviders")
    Provider = apps.get_model("src", "ShopManagementProviders")

    kinds = dict(Provider.objects.values_list("id", "kind"))
    identity_fields = _identity_fields_by_kind()

    company_ids = set(Connection.objects.values_list("company_id", flat=True))
    for model_name in SYNC_MODELS:
        company_ids.update(apps.get_model("src", model_name).objects.values_list("company_id", flat=True))

    for company_id in sorted(company_ids):
        main_id = _main_space_id(apps, company_id)
        for conn in Connection.objects.filter(company_id=company_id).order_by("id"):
            changes = {}
            if conn.space_id is None and not Connection.objects.filter(space_id=main_id).exists():
                changes["space_id"] = main_id
            if conn.credentials_fingerprint is None:
                fingerprint = account_fingerprint.account_fingerprint(
                    identity_fields.get(kinds.get(conn.provider_id), []), conn.credentials
                )
                if fingerprint:
                    changes["credentials_fingerprint"] = fingerprint
            if changes:
                Connection.objects.filter(id=conn.id).update(**changes)
        for model_name in SYNC_MODELS:
            apps.get_model("src", model_name).objects.filter(company_id=company_id, space_id__isnull=True).update(
                space_id=main_id
            )


def clear_shop_spaces(apps, schema_editor):
    apps.get_model("src", "CompanyShopManagementProviders").objects.update(space=None, credentials_fingerprint=None)
    for model_name in SYNC_MODELS:
        apps.get_model("src", model_name).objects.update(space=None)


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0256_shop_systems_schema"),
    ]

    operations = [
        migrations.RunPython(backfill_shop_spaces, clear_shop_spaces),
    ]
