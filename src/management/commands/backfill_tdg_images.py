"""
One-time backfill: copy product images from ``tdg_products`` onto the parts that have none.

The matching rules, and what this deliberately will not touch, are in
``src/integrations/services/tdg_images.py``. Read that first -- in particular the note on why
parts with no ``MasterPartData`` row are reported rather than created.

Run ``fetch_tdg_catalog`` first; this reads whatever that last left in the table.

Typical runs:

    # what would change, writing nothing
    manage.py backfill_tdg_images --dry-run

    # rehearse on a small slice
    manage.py backfill_tdg_images --limit 100 --dry-run

    # the real thing
    manage.py backfill_tdg_images

Only empty fields are written, so re-running is a no-op rather than a second opinion.
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tdg_images


class Command(BaseCommand):
    help = (
        "Fill MasterPart.image_url and MasterPartData.images from tdg_products, for parts that "
        "have no image. Matches on GTIN, then brand + part number. Idempotent."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=tdg_images.DEFAULT_BATCH_SIZE,
            help=f"Rows per bulk_update (default: {tdg_images.DEFAULT_BATCH_SIZE}).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Stop after this many matched parts. For rehearsals.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Match and report, write nothing.",
        )

    def handle(self, *args, **options):
        if options["batch_size"] < 1:
            raise CommandError("--batch-size must be at least 1")
        if options["limit"] is not None and options["limit"] < 1:
            raise CommandError("--limit must be at least 1")

        if not tdg_images.TdgProduct.objects.exists():
            raise CommandError("tdg_products is empty -- run `manage.py fetch_tdg_catalog` first.")

        stats = tdg_images.run_backfill(
            batch_size=options["batch_size"],
            limit=options["limit"],
            dry_run=options["dry_run"],
            progress=self.stdout.write,
        )
        self._report(stats, dry_run=options["dry_run"])

    def _report(self, stats: tdg_images.BackfillStats, *, dry_run: bool) -> None:
        verb = "would fill" if dry_run else "filled"
        self.stdout.write("")
        self.stdout.write(f"  candidates scanned      : {stats.candidates_scanned}")
        self.stdout.write(f"  matched a TDG image     : {stats.matched}")
        for how, count in sorted(stats.by_match.items(), key=lambda item: -item[1]):
            self.stdout.write(f"    by {how:<20} {count:>7}")
        self.stdout.write("")
        self.stdout.write(f"  image_url {verb:<12}  : {stats.image_url_filled}")
        self.stdout.write(f"  data.images {verb:<12}: {stats.data_images_filled}")
        self.stdout.write(f"  left alone (had images) : {stats.skipped_already_set}")

        if stats.skipped_no_data_row:
            self.stdout.write(
                self.style.WARNING(
                    f"  no MasterPartData row   : {stats.skipped_no_data_row} "
                    "(skipped on purpose -- creating one needs a TDG provider; see the service docstring)"
                )
            )
        self.stdout.write(self.style.SUCCESS("Done."))
