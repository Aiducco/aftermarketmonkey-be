# Spaces, step 2 of 3 (data only): every company gets its main space, named after its primary location's
# label (else the company's name) and linked to that location, and every existing location, purchase order
# and external order is put in it. Idempotent: it reuses a main space the application already created
# (Space.objects.main_for between 0251 and now) and only touches rows whose `space` is still NULL.
# External orders get space_reason=ACCOUNT so pre-migration orders never count in the "went to Main by
# default" notice. No rows are written for members or order accounts: no rows means the owner (ADR 0001).
# Reverse: NULL the three `space` columns and delete the Space rows (the columns themselves go with 0251).
from django.db import migrations

KIND_LOCATION = 1
REASON_ACCOUNT = 2


def backfill_main_spaces(apps, schema_editor):
    Company = apps.get_model("src", "Company")
    CompanyLocation = apps.get_model("src", "CompanyLocation")
    PurchaseOrder = apps.get_model("src", "PurchaseOrder")
    ExternalPurchaseOrder = apps.get_model("src", "ExternalPurchaseOrder")
    Space = apps.get_model("src", "Space")

    for company_id, company_name in Company.objects.order_by("id").values_list("id", "name").iterator():
        primary = CompanyLocation.objects.filter(company_id=company_id, is_primary=True).order_by("id").first()
        main = Space.objects.filter(company_id=company_id, is_main=True).first()
        if main is None:
            main = Space.objects.create(
                company_id=company_id,
                kind=KIND_LOCATION,
                kind_name="LOCATION",
                name=primary.label if primary else company_name,
                is_main=True,
                location=primary,
            )
        elif main.location_id is None and primary is not None and not Space.objects.filter(location=primary).exists():
            main.location = primary
            main.save(update_fields=["location"])

        CompanyLocation.objects.filter(company_id=company_id, space_id__isnull=True).update(space_id=main.id)
        PurchaseOrder.objects.filter(company_id=company_id, space_id__isnull=True).update(space_id=main.id)
        ExternalPurchaseOrder.objects.filter(company_id=company_id, space_id__isnull=True).update(
            space_id=main.id, space_reason=REASON_ACCOUNT, space_reason_name="ACCOUNT"
        )


def remove_spaces(apps, schema_editor):
    CompanyLocation = apps.get_model("src", "CompanyLocation")
    PurchaseOrder = apps.get_model("src", "PurchaseOrder")
    ExternalPurchaseOrder = apps.get_model("src", "ExternalPurchaseOrder")
    Space = apps.get_model("src", "Space")

    CompanyLocation.objects.update(space=None)
    PurchaseOrder.objects.update(space=None)
    ExternalPurchaseOrder.objects.update(space=None)
    Space.objects.all().delete()


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0251_spaces_schema"),
    ]

    operations = [
        migrations.RunPython(backfill_main_spaces, remove_spaces),
    ]
