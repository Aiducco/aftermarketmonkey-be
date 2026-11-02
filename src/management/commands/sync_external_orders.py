"""
Discover distributor-placed ("external") orders we did not place through this app, for every
active order account whose distributor has a registered order-history reader (Meyer, Premier,
Keystone, Turn 14 today -- see src.integrations.orders.history_registry). Thin wrapper around
src.integrations.services.external_order_sync.sync_external_orders -- see that module's
docstring for the actual polling/dedup/upsert logic.

Safe to run repeatedly and intended to run a few times a day (cron cadence is not pinned by
this command itself -- see research-be.md's Open Questions).

    manage.py sync_external_orders --dry-run
    manage.py sync_external_orders --company 16
    manage.py sync_external_orders --company trident-motorsports-9992401f --provider MEYER
    manage.py sync_external_orders --company 16 --since 2026-10-01 --limit 5
"""
import datetime
import typing

from django.core.management.base import BaseCommand, CommandError

from src import enums as src_enums
from src import models as src_models
from src.integrations.services import external_order_sync


def _resolve_company_id(value: str) -> int:
    """Accepts either a numeric id or a slug -- never a bare name (duplicate Company rows with
    the same name exist; see the repo's own "resolve by slug" gotcha)."""
    if value.isdigit():
        company = src_models.Company.objects.filter(id=int(value)).first()
    else:
        company = src_models.Company.objects.filter(slug=value).first()
    if company is None:
        raise CommandError("No company found for --company={!r} (expected an id or a slug).".format(value))
    return company.id


def _resolve_provider_kind(value: str) -> int:
    try:
        return src_enums.BrandProviderKind[value.strip().upper()].value
    except KeyError:
        valid = ", ".join(m.name for m in src_enums.BrandProviderKind)
        raise CommandError("Unknown --provider={!r}. Valid values: {}".format(value, valid))


class Command(BaseCommand):
    help = (
        "Poll Meyer/Premier/Keystone/Turn 14 order-history APIs (read-only) to discover orders "
        "placed outside this app, and upsert them into ExternalPurchaseOrder."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument("--company", type=str, default=None, help="Company id or slug (default: every company).")
        parser.add_argument(
            "--provider",
            type=str,
            default=None,
            help="BrandProviderKind name, e.g. MEYER (default: every supported kind).",
        )
        parser.add_argument(
            "--since",
            type=str,
            default=None,
            help="YYYY-MM-DD -- only used for an account's first-ever run (no watermark yet).",
        )
        parser.add_argument("--limit", type=int, default=None, help="Cap how many order accounts are polled this run.")
        parser.add_argument(
            "--dry-run", action="store_true", help="Log what would be upserted; write nothing (no DB writes at all)."
        )

    def handle(self, *args: typing.Any, **options: typing.Any) -> None:
        company_id = _resolve_company_id(options["company"]) if options["company"] else None
        provider_kind = _resolve_provider_kind(options["provider"]) if options["provider"] else None
        since = datetime.date.fromisoformat(options["since"]) if options["since"] else None

        result = external_order_sync.sync_external_orders(
            company_id=company_id,
            provider_kind=provider_kind,
            since=since,
            limit=options["limit"],
            dry_run=options["dry_run"],
        )

        self.stdout.write(
            self.style.SUCCESS(
                "{}Polled {} account(s) ({} skipped, {} failed) -- {} order(s) discovered, "
                "{} updated, {} self-healed.".format(
                    "[DRY RUN] " if options["dry_run"] else "",
                    result["accounts_polled"],
                    result["accounts_skipped"],
                    result["accounts_failed"],
                    result["orders_discovered"],
                    result["orders_updated"],
                    result["orders_healed"],
                )
            )
        )
