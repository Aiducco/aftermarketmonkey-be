"""
Link, instead of duplicate, the parts ShopMonkey already holds. Run this right before enabling a brand.

    manage.py adopt_shopmonkey_live_parts --company-id 16 --all              # dry run (the default)
    manage.py adopt_shopmonkey_live_parts --company-id 16 --brand "BDS" --apply

The standalone tool this feature replaces does not track every part in the account, and both it and the shop keep
creating parts, so a master part the sync is about to create may already exist upstream with the same SKU. Creating
it would give the shop a second copy. This reads the account (a full listing, ~8 minutes -- ShopMonkey cannot filter by
SKU) and, for each target brand, links any part that already exists under that brand's ShopMonkey brand with an
identical SKU and that nothing else points at. From then on the sync updates it like any other part.

Writes data only; its calls to ShopMonkey are GETs. Dry run is the default, and it reports what it would adopt.
Only brands that are DISABLED are targeted by --all: an enabled brand is already syncing.
"""
import typing

from django.core.management.base import BaseCommand, CommandError

from common import enums as common_enums
from src import models as src_models
from src.integrations.services import shopmonkey_import as import_services
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services

_PAGE = 100  # ShopMonkey caps the listing at 100 per page


class Command(BaseCommand):
    help = "Link parts ShopMonkey already holds to master parts before a brand is enabled (dry run by default)."

    def add_arguments(self, parser):
        parser.add_argument("--company-id", type=int, required=True)
        parser.add_argument("--brand", action="append", default=None, help="Our brand name; repeat for several.")
        parser.add_argument("--all", action="store_true", help="Every disabled sync brand for the company.")
        parser.add_argument("--apply", action="store_true", help="Write. Without it nothing is written.")

    def handle(self, *args, **options):
        company_id = options["company_id"]
        brands = src_models.ShopMonkeySyncBrand.objects.filter(company_id=company_id).select_related(
            "brand", "vendor__provider"
        )
        if options["brand"]:
            brands = brands.filter(brand__name__in=options["brand"])
        elif options["all"]:
            brands = brands.filter(enabled=False)
        else:
            raise CommandError("Pass --brand NAME (repeatable) or --all.")
        targets = list(brands.order_by("id"))
        if not targets:
            raise CommandError("No matching sync brands.")

        self.stdout.write("Reading the live ShopMonkey account (about 8 minutes)...")
        records = self._scan(company_id)
        self.stdout.write("Account holds {} parts.\n".format(len(records)))

        total_adopted = 0
        for sync_brand in targets:
            if options["apply"]:
                result = import_services.adopt_untracked_live_parts(sync_brand, records)
            else:
                result = import_services.adopt_untracked_live_parts(sync_brand, records, dry_run=True)
            total_adopted += result["adopted"]
            if result["adopted"] or result["ambiguous"]:
                self.stdout.write(
                    "  {:<28} adopt={:<5} ambiguous={:<3} already linked elsewhere={}".format(
                        sync_brand.brand.name[:28], result["adopted"], result["ambiguous"], result["already_tracked"]
                    )
                )
        verb = "Adopted" if options["apply"] else "Would adopt"
        self.stdout.write(
            self.style.SUCCESS("\n{} {} part(s) across {} brand(s).".format(verb, total_adopted, len(targets)))
        )
        if not options["apply"]:
            self.stdout.write(self.style.WARNING("DRY RUN -- nothing written. Re-run with --apply."))

    def _scan(self, company_id: int) -> typing.List[typing.Dict[str, typing.Any]]:
        client = shopmonkey_sync_services._client_for_company(company_id)
        records: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        skip = 0
        while True:
            response = client._request(
                endpoint="inventory_part",
                method=common_enums.HttpMethod.GET,
                params={"limit": _PAGE, "skip": skip},
            )
            rows = response.get("data") or []
            for row in rows:
                records[row["id"]] = row
            if not (response.get("meta") or {}).get("hasMore") or not rows:
                break
            skip += _PAGE
            if skip % 10000 == 0:
                self.stdout.write("  ...{} read".format(len(records)))
        return list(records.values())
