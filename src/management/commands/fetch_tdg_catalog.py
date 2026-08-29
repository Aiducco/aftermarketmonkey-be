"""
Pull the TDG Access catalog into ``tdg_products`` -- one row per SKU, every product type.

The machinery lives in ``src/integrations/services/tdg.py``. Read that before changing anything
here, in particular the note on why this is a single request rather than a crawl.

Scale: one POST, ~32 MB, ~45,000 products (roughly 35k tires, 9.6k wheels, and a few hundred lug
kits, hub rings, generic products and services), under a minute end to end. Nothing to resume --
if it fails, run it again.

Typical runs:

    # what is in the catalog, without writing anything
    manage.py fetch_tdg_catalog --dry-run

    # the real thing
    manage.py fetch_tdg_catalog

    # keep the response, then iterate on the mapper against it -- no re-download
    manage.py fetch_tdg_catalog --keep-file
    manage.py fetch_tdg_catalog --from-file /tmp/tdg_products_ab12cd.json

    # wheels only
    manage.py fetch_tdg_catalog --type Wheel

Requires TDG_API_KEY. TDG_ENVIRONMENT defaults to production ('rst' key prefix); the sandbox
takes its own key, prefixed 'rstsb'.
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.clients.tdg import exceptions as tdg_exceptions
from src.integrations.services import tdg


class Command(BaseCommand):
    help = (
        "Fetch every product TDG Access lists -- tires, wheels, lug kits, hub rings and the rest -- "
        "and upsert them into tdg_products. Idempotent; safe to re-run."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--api-key",
            default="",
            help="Override settings.TDG_API_KEY for this run.",
        )
        parser.add_argument(
            "--environment",
            choices=["production", "sandbox"],
            default="",
            help="Override settings.TDG_ENVIRONMENT. The two take different keys.",
        )
        parser.add_argument(
            "--type",
            action="append",
            dest="product_types",
            default=None,
            metavar="TYPE",
            help=(
                "Only write this product type, spelled as TDG spells it: Tire, Wheel, 'Lug Kit', "
                "'Hub Ring', 'Generic Product', Service. Repeatable. The whole catalog is "
                "downloaded either way -- this filters the write, not the request."
            ),
        )
        parser.add_argument(
            "--from-file",
            default="",
            help="Parse a previously saved response instead of calling TDG. Pairs with --keep-file.",
        )
        parser.add_argument(
            "--keep-file",
            action="store_true",
            help="Leave the downloaded response on disk and print its path.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=tdg.DEFAULT_BATCH_SIZE,
            help=f"Rows per upsert batch (default: {tdg.DEFAULT_BATCH_SIZE}).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and map, write nothing. Still reports the per-type breakdown.",
        )

    def handle(self, *args, **options):
        if options["batch_size"] < 1:
            raise CommandError("--batch-size must be at least 1")

        try:
            stats = tdg.run_fetch(
                api_key=options["api_key"],
                environment=options["environment"],
                from_file=options["from_file"],
                keep_file=options["keep_file"],
                batch_size=options["batch_size"],
                product_types=options["product_types"],
                dry_run=options["dry_run"],
                progress=self.stdout.write,
            )
        except (tdg_exceptions.TdgAuthError, tdg_exceptions.TdgPermissionError) as exc:
            raise CommandError(str(exc)) from exc
        except tdg_exceptions.TdgException as exc:
            raise CommandError(f"TDG fetch failed: {exc}") from exc
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self._report(stats, dry_run=options["dry_run"])

    def _report(self, stats: tdg.FetchStats, *, dry_run: bool) -> None:
        verb = "would write" if dry_run else "wrote"
        self.stdout.write("")
        self.stdout.write(f"  downloaded            : {stats.bytes_downloaded / (1 << 20):.1f} MB")
        self.stdout.write(f"  products in response  : {stats.products_seen}")
        self.stdout.write(f"  products {verb:<12} : {stats.products_written}")
        if stats.products_skipped:
            self.stdout.write(f"  skipped (filter/no id): {stats.products_skipped}")

        if stats.by_type:
            self.stdout.write("")
            self.stdout.write("  by type:")
            for product_type, count in sorted(stats.by_type.items(), key=lambda item: -item[1]):
                self.stdout.write(f"    {product_type or '(untyped)':<20} {count:>7}")

        if stats.products_rejected:
            self.stdout.write(
                self.style.WARNING(
                    f"  rejected by the database: {stats.products_rejected} "
                    "(a column is too narrow -- see the log lines for the offending values)"
                )
            )
        self.stdout.write(self.style.SUCCESS("Done."))
