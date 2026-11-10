"""
Port ONE brand of the old standalone ShopMonkey tool onto a company's sync tables.

    manage.py import_shopmonkey_brand --company-id 16 --old-brand "Titan Fuel Tanks" --our-brand-id 3082   # dry run
    manage.py import_shopmonkey_brand --company-id 16 --old-brand "Titan Fuel Tanks" --our-brand-id 3082 --apply

Dry run is the default and the only way to look. It reports, before anything is written, how many of the old
brand's parts link to a master part and how many do not, because every part that fails to link is a part whose
ShopMonkey id we cannot carry -- and a part with no id is one a later sync would create a second time.

The brand is written DISABLED. Nothing syncs until someone enables it, and the intended order is: import,
rehearse the real sync against it (what would be sent, with nothing sent), read that, then enable.

Writes data only; its calls to ShopMonkey are GETs. See src.integrations.services.shopmonkey_import.
"""
import collections
import typing

from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import shopmonkey_import as import_services
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services


class Command(BaseCommand):
    help = "Port one brand of the old ShopMonkey tool onto a company (dry run by default)."

    def add_arguments(self, parser):
        parser.add_argument("--company-id", type=int, required=True)
        parser.add_argument("--old-brand", required=True, help="Exact name in the old tool's inventory_brand.")
        parser.add_argument("--our-brand-id", type=int, required=True, help="Our Brands.id. Explicit, never fuzzy.")
        parser.add_argument("--old-vendor", default=None, help="Old vendor name, if the brand exists under several.")
        parser.add_argument("--old-repo", default=import_services.DEFAULT_OLD_REPO)
        parser.add_argument("--skip-live-check", action="store_true", help="Do not GET each old part from ShopMonkey.")
        parser.add_argument("--apply", action="store_true", help="Write. Without it nothing is written.")

    def handle(self, *args, **options):
        plan = import_services.plan_brand_import(
            company_id=options["company_id"],
            old_brand_name=options["old_brand"],
            our_brand_id=options["our_brand_id"],
            old_repo=options["old_repo"],
            old_vendor_name=options["old_vendor"],
        )
        old = plan.old_brand
        parts = old["parts"]
        self.stdout.write(
            "Old brand : {} (vendor {}, bin {}, ShopMonkey brand id {})".format(
                old["name"], old["provider_name"], old["vendor_bin_name"], old["sm_brand_id"]
            )
        )
        self.stdout.write("Our brand : {} (id {})".format(plan.our_brand.name, plan.our_brand.id))
        self.stdout.write("Vendor    : {} -> {}".format(plan.provider.kind_name, plan.vendor.shopmonkey_external_id))
        self.stdout.write(
            "Old parts : {}  {}".format(len(parts), dict(collections.Counter(p["status_name"] for p in parts)))
        )
        self.stdout.write("")
        self.stdout.write("  will link              {:>5}".format(len(plan.linkable)))
        self.stdout.write("  already imported       {:>5}".format(plan.already_imported))
        self.stdout.write(
            "  matched, not carried   {:>5}   (vendor no longer carries it; left alone in ShopMonkey)".format(
                len(plan.not_carried)
            )
        )
        self.stdout.write("  no master part         {:>5}   (left alone in ShopMonkey)".format(len(plan.unresolved)))
        self.stdout.write("  ambiguous number       {:>5}   (never guessed)".format(len(plan.ambiguous)))
        self.stdout.write("  two old -> one master  {:>5}   (second one left alone)".format(len(plan.collisions)))

        for label, group in (
            ("matched, not carried", [p for p, _ in plan.not_carried]),
            ("no master part", plan.unresolved),
            ("ambiguous", plan.ambiguous),
            ("collision", plan.collisions),
        ):
            for p in group[:5]:
                self.stdout.write(
                    "    [{}] {} / {} {} {!r}".format(label, p["sku"], p["number"], p["status_name"], p["name"][:40])
                )

        live_problems = self._live_check(plan, options)
        problems = plan.problems + live_problems
        for problem in problems:
            self.stdout.write(self.style.ERROR("PROBLEM: " + problem))

        if not options["apply"]:
            self.stdout.write(self.style.WARNING("\nDRY RUN -- nothing written. Re-run with --apply."))
            return
        if problems:
            raise CommandError("Not applying: {} open problem(s).".format(len(problems)))
        result = import_services.apply_brand_import(plan)
        self.stdout.write(self.style.SUCCESS("\nApplied (brand written DISABLED): {}".format(result)))

    def _live_check(self, plan, options) -> typing.List[str]:
        """Every id we are about to carry must exist in the live account, under the brand we think it does."""
        if options["skip_live_check"]:
            return []
        client = shopmonkey_sync_services._client_for_company(plan.company_id)
        expected_brand = plan.old_brand["sm_brand_id"]
        missing, wrong_brand = [], []
        for part, _ in plan.linkable:
            try:
                live = client.get_inventory_part(part["external_id"])
            except Exception:
                missing.append(part["sku"])
                continue
            live = live.get("data", live) if isinstance(live, dict) and "data" in live else live
            if live.get("deleted"):
                missing.append(part["sku"])
            elif live.get("brandId") != expected_brand:
                wrong_brand.append(part["sku"])
        self.stdout.write(
            "\nLive check: {} linkable parts, {} missing from the account, {} under a different brand".format(
                len(plan.linkable), len(missing), len(wrong_brand)
            )
        )
        out = []
        if missing:
            out.append("{} linkable part id(s) not found live: {}".format(len(missing), ", ".join(missing[:5])))
        if wrong_brand:
            out.append("{} part(s) live under another brand: {}".format(len(wrong_brand), ", ".join(wrong_brand[:5])))
        return out
