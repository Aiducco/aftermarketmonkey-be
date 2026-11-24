"""
Remove a company's feed tenants (the hidden companies that hold a space's own distributor feeds, ADR 0002) and
every row they own, including their copies of the per-company pricing tables. This is the only way tenants are
deleted: never inside a request (a tenant can own millions of pricing rows) and not by delete_all_company_data,
which refuses while a company still has tenants.

The human runs it, after a disk check (`df -h` on the DB host) and a dry run:

    manage.py purge_feed_tenants --company-id 16                  # dry run: lists, deletes nothing
    manage.py purge_feed_tenants --company-id 16 --tenant-id 41   # dry run for one tenant
    manage.py purge_feed_tenants --company-id 16 --execute        # deletes

Rows are deleted in batches of --batch-size primary keys per table, each batch its own transaction, so an
interrupted run can simply be started again. A tenant whose connections still have purchase orders or orders from
distributors is skipped and reported (deactivate it instead); so is a tenant with any other protected reference.
A space that pointed at a purged tenant has its feed_company cleared. It is a dry run unless --execute is given.
A tenant that is live (an active connection, or a space that uses one of its connections) is skipped and reported
unless --include-live is passed. Before any pricing row goes, the tenant's connections are deactivated, so an
interrupted run never leaves an active tenant with partial prices.
"""
import typing

from django.core.management.base import BaseCommand, CommandError
from django.db import models as django_models
from django.db import transaction

from src import models as src_models

DEFAULT_BATCH_SIZE = 5000


def _related_fields() -> typing.List[django_models.ForeignObjectRel]:
    """Every one-to-many / one-to-one relation that points at Company, discovered from the model so a pricing
    table added later is covered without touching this command."""
    return [rel for rel in src_models.Company._meta.related_objects if not rel.many_to_many]


def _is_pricing(model: typing.Type[django_models.Model]) -> bool:
    return model.__name__.endswith("Pricing")


class Command(BaseCommand):
    help = (
        "Delete a company's feed tenants and all their rows in batches (disk check first). Dry run unless --execute "
        "is given. Tenants with orders, or live ones (unless --include-live), are skipped."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument("--company-id", type=int, required=True, help="The real (parent) company's id.")
        parser.add_argument("--tenant-id", type=int, default=None, help="Purge only this tenant of the company.")
        parser.add_argument("--execute", action="store_true", help="Delete. Without it nothing is written.")
        parser.add_argument(
            "--include-live",
            action="store_true",
            help="Also purge a tenant with an active connection or a space using one of its connections.",
        )
        parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)

    def handle(self, *args: typing.Any, **options: typing.Any) -> None:
        company_id = options["company_id"]
        dry_run = not options["execute"]
        tenant_id = options["tenant_id"]
        include_live = options["include_live"]
        batch_size = max(1, options["batch_size"])

        parent = src_models.Company.objects.filter(id=company_id).first()
        if parent is None:
            raise CommandError("No company with id={}.".format(company_id))
        if parent.parent_company_id is not None:
            raise CommandError("Company {} is itself a feed tenant; pass its parent's id.".format(company_id))

        self.stdout.write("Company {} ({}){}".format(parent.id, parent.name, " [dry run]" if dry_run else ""))
        tenants = src_models.Company.objects.filter(parent_company_id=company_id)
        if tenant_id is not None:
            tenants = tenants.filter(id=tenant_id)
        tenants = list(tenants.order_by("id"))
        if not tenants:
            self.stdout.write("No feed tenants to purge.")
            return

        for tenant in tenants:
            self._purge_tenant(tenant, dry_run, batch_size, include_live)

    def _purge_tenant(
        self, tenant: src_models.Company, dry_run: bool, batch_size: int, include_live: bool = False
    ) -> None:
        self.stdout.write("Tenant {} ({}), is_internal={}:".format(tenant.id, tenant.name, tenant.is_internal))

        plan: typing.List[typing.Tuple[typing.Type[django_models.Model], str, int]] = []
        pricing_total = 0
        blocked = []
        orders = src_models.PurchaseOrder.objects.filter(company_provider__company_id=tenant.id).count()
        orders += src_models.ExternalPurchaseOrder.objects.filter(company_provider__company_id=tenant.id).count()
        if orders:
            blocked.append("{} order(s) reference its connections (deactivate them instead)".format(orders))
        for rel in _related_fields():
            model, field = rel.related_model, rel.field
            if model is src_models.Space or model is src_models.Company:
                continue  # the space link is cleared below; a tenant of a tenant does not exist (checked below)
            count = model.objects.filter(**{field.name: tenant.id}).count()
            if not count:
                continue
            if rel.on_delete is not django_models.CASCADE:
                blocked.append("{}.{} ({} rows, not CASCADE)".format(model.__name__, field.name, count))
            plan.append((model, field.name, count))
            if _is_pricing(model):
                pricing_total += count
        if src_models.Company.objects.filter(parent_company_id=tenant.id).exists():
            blocked.append("a tenant of this tenant exists")
        if not include_live:
            if src_models.CompanyProviders.objects.filter(company_id=tenant.id, active=True).exists():
                blocked.append("it has an active connection (--include-live to purge anyway)")
            if src_models.SpaceProvider.objects.filter(company_provider__company_id=tenant.id).exists():
                blocked.append("a space uses one of its connections (--include-live to purge anyway)")

        for model, field_name, count in sorted(plan, key=lambda item: -item[2]):
            self.stdout.write("  {}: {} row(s)".format(model.__name__, count))
        self.stdout.write("  pricing rows: {}".format(pricing_total))
        if blocked:
            self.stdout.write(self.style.WARNING("  SKIPPED: {}".format("; ".join(blocked))))
            return
        if dry_run:
            self.stdout.write("  dry run: nothing deleted.")
            return

        # Nothing may read these prices while they go: deactivate the connections before the first batch.
        deactivated = src_models.CompanyProviders.objects.filter(company_id=tenant.id, active=True).update(active=False)
        self.stdout.write("  deactivated {} connection(s).".format(deactivated))

        # Biggest tables first: the long part, and the tables nothing else points at.
        for model, field_name, count in sorted(plan, key=lambda item: -item[2]):
            deleted = 0
            while True:
                ids = list(model.objects.filter(**{field_name: tenant.id}).values_list("pk", flat=True)[:batch_size])
                if not ids:
                    break
                with transaction.atomic():
                    model.objects.filter(pk__in=ids).delete()
                deleted += len(ids)
            self.stdout.write("  deleted {} from {}".format(deleted, model.__name__))

        tenant_id = tenant.id
        with transaction.atomic():
            src_models.Space.objects.filter(feed_company_id=tenant_id).update(feed_company=None)
            tenant.delete()
        self.stdout.write(self.style.SUCCESS("  tenant {} removed.".format(tenant_id)))
