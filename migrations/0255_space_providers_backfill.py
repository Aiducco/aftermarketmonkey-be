# Feed tenants, step 2 of 2 (data only): one `space_providers` row per existing (space, connection of that
# space's company) pair, so no space that exists before this release loses a distributor (before it, every
# space of a company used every connection of that company). Spaces created later start with none (spec
# OQ-13). Idempotent: existing rows are kept, only missing pairs are inserted. Rows go in with bulk_create in
# batches; `company_providers` and `spaces` are small tables. Reverse: delete every row.
from django.db import migrations

BATCH = 1000


def backfill_space_providers(apps, schema_editor):
    Space = apps.get_model("src", "Space")
    CompanyProviders = apps.get_model("src", "CompanyProviders")
    SpaceProvider = apps.get_model("src", "SpaceProvider")

    existing = set(SpaceProvider.objects.values_list("space_id", "provider_id"))
    connections_by_company = {}
    for cp_id, company_id, provider_id in CompanyProviders.objects.order_by("id").values_list(
        "id", "company_id", "provider_id"
    ):
        connections_by_company.setdefault(company_id, []).append((cp_id, provider_id))

    batch = []
    for space_id, company_id in Space.objects.order_by("id").values_list("id", "company_id").iterator():
        for cp_id, provider_id in connections_by_company.get(company_id, []):
            if (space_id, provider_id) in existing:
                continue
            batch.append(SpaceProvider(space_id=space_id, provider_id=provider_id, company_provider_id=cp_id))
            if len(batch) >= BATCH:
                SpaceProvider.objects.bulk_create(batch)
                batch = []
    if batch:
        SpaceProvider.objects.bulk_create(batch)


def remove_space_providers(apps, schema_editor):
    SpaceProvider = apps.get_model("src", "SpaceProvider")
    SpaceProvider.objects.all().delete()


class Migration(migrations.Migration):
    dependencies = [
        ("src", "0254_feed_tenants_schema"),
    ]

    operations = [
        migrations.RunPython(backfill_space_providers, remove_space_providers),
    ]
