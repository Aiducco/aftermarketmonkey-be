"""
Pull the Wheel Pros Product API catalogue into ``wheelpros_parts`` — images, UPC, structured
properties, real ``nip`` cost and live inventory — enriching the rows the SFTP CSV sync built and
inserting the SKUs that feed never carried.

The crawl machinery, the 10,000-row window it works around, and the reasoning behind writing to
``api_data`` rather than ``raw_data`` are in ``src/integrations/services/wheelpros_products.py``.
Read that before changing anything here.

Credentials: an active Wheel Pros connection with API username/password. Product *pricing* is
account-scoped (``nip`` is that customer's negotiated cost), so which connection is used matters
— pin it with ``--company-provider-id``. Company 16's connection is id 17.

Scale: ~115,900 SKUs (85.9k wheels, 24.1k accessories, 5.9k tires) against ~69.1k existing rows.
A full pass is a few hundred requests at ``--page-size 1000``.

Typical runs:

    # what would be pulled, and how it partitions. No writes.
    manage.py fetch_wheelpros_products --company-provider-id 17 --plan-only

    # rehearse one kind end to end, writing nothing
    manage.py fetch_wheelpros_products --company-provider-id 17 --kind tire --dry-run

    # tires for real (smallest kind, good first write)
    manage.py fetch_wheelpros_products --company-provider-id 17 --kind tire

    # the whole catalogue, enriching and inserting
    nohup manage.py fetch_wheelpros_products --company-provider-id 17 > /tmp/wp_products.log 2>&1 &

    # enrich existing rows only, never insert
    manage.py fetch_wheelpros_products --company-provider-id 17 --no-insert
"""
import pathlib

from django.core.management.base import BaseCommand, CommandError

from src.integrations.clients.wheelpros import exceptions as wheelpros_exceptions
from src.integrations.services import wheelpros_products

DEFAULT_UNMATCHED_PATH = pathlib.Path("logs/wheelpros_products_unmatched.jsonl")


class Command(BaseCommand):
    help = (
        "Fetch the Wheel Pros Product API catalogue and upsert it into wheelpros_parts: enriches "
        "existing rows via api_data and inserts API-only SKUs. Safe to re-run."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--kind",
            action="append",
            choices=list(wheelpros_products._PARTITION_AXES),
            default=None,
            help="Limit to wheel, tire or accessory. Repeatable. Omit for all three.",
        )
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="Authenticate with this CompanyProviders row. Determines whose nip cost is "
            "returned. Company 16's Wheel Pros connection is id 17.",
        )
        parser.add_argument(
            "--company",
            default="1000",
            help="Wheel Pros sales org for pricing: 1000 (CAD|USD), 1500 (USD), 4000 (CAD), "
            "5000 (AUD), 6000 (GBP), 7000 (EUR). Default 1000.",
        )
        parser.add_argument(
            "--page-size",
            type=int,
            default=wheelpros_products.DEFAULT_PAGE_SIZE,
            help=f"Rows per request, max 1000 (default: {wheelpros_products.DEFAULT_PAGE_SIZE}). "
            "The documented max of 100 is wrong; 1000 works and is 10x cheaper.",
        )
        parser.add_argument(
            "--no-insert",
            action="store_true",
            help="Enrich existing rows only. API-only SKUs are recorded to the unmatched file "
            "and skipped instead of being inserted.",
        )
        parser.add_argument(
            "--unmatched",
            default=str(DEFAULT_UNMATCHED_PATH),
            help=f"Where to record API SKUs with no existing part (default: {DEFAULT_UNMATCHED_PATH}).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and match, write nothing to the database.",
        )
        parser.add_argument(
            "--plan-only",
            action="store_true",
            help="Print the slice plan and true catalogue sizes, then exit. No detail fetches.",
        )

    def handle(self, *args, **options):
        if options["page_size"] < 1 or options["page_size"] > 1000:
            raise CommandError("--page-size must be between 1 and 1000")

        try:
            client = wheelpros_products.get_product_api_client(
                company_provider_id=options["company_provider_id"]
            )
        except (wheelpros_products.WheelProsProductError, ValueError) as exc:
            raise CommandError(str(exc))
        client.company = options["company"]

        kinds = options["kind"] or list(wheelpros_products._PARTITION_AXES)
        stats = wheelpros_products.ProductStats()

        try:
            if options["plan_only"]:
                for kind in kinds:
                    self.stdout.write("Planning {} ...".format(kind))
                    slices = wheelpros_products.plan_slices(
                        client, kind, stats=stats, progress=self.stdout.write
                    )
                    for s in slices[:15]:
                        self.stdout.write("    {:<52} {:>7}".format(str(s), s.expected))
                    if len(slices) > 15:
                        self.stdout.write("    ... and {} more slices".format(len(slices) - 15))
                self.stdout.write(self.style.SUCCESS(
                    "{} slices planned, {} requests used".format(stats.slices_planned, client.requests_made)))
                return

            stats = wheelpros_products.run(
                client,
                kinds=kinds,
                insert_new=not options["no_insert"],
                dry_run=options["dry_run"],
                page_size=options["page_size"],
                unmatched_path=pathlib.Path(options["unmatched"]),
                progress=self.stdout.write,
                stats=stats,
            )
        except wheelpros_exceptions.WheelProsProductPermissionError as exc:
            raise CommandError(str(exc))

        self._report(stats, dry_run=options["dry_run"], insert=not options["no_insert"],
                     unmatched=options["unmatched"])

    def _report(self, stats, *, dry_run, insert, unmatched):
        verb = "would be" if dry_run else ""
        self.stdout.write("")
        self.stdout.write("  slices planned        : {}".format(stats.slices_planned))
        self.stdout.write("  slices completed      : {}".format(stats.slices_done))
        self.stdout.write("  API rows fetched      : {}".format(stats.rows_fetched))
        self.stdout.write("  parts enriched  {:6}: {}".format(verb, stats.parts_updated))
        self.stdout.write("  feed_type backfilled  : {}".format(stats.feed_types_backfilled))
        if insert:
            self.stdout.write("  parts inserted  {:6}: {}".format(verb, stats.parts_inserted))
            self.stdout.write("  brands created        : {}".format(stats.brands_created))
        else:
            self.stdout.write("  API-only SKUs skipped : {}".format(stats.unmatched))
        self.stdout.write("  unmatched recorded to : {}".format(unmatched))
        self.stdout.write("  requests made         : {}".format(stats.requests_made))

        if stats.slices_skipped:
            self.stdout.write(self.style.WARNING(
                "  slices FAILED         : {} -- incomplete; re-run to retry them".format(stats.slices_skipped)))
        self.stdout.write(self.style.SUCCESS("Done."))
