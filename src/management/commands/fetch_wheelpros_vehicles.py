"""
Scrape Wheel Pros' Vehicle API into ``wheelpros_vehicles`` / ``wheelpros_vehicle_axles`` — every
year/make/model/submodel they publish, with the wheel-and-tire fitment envelope for each.

The crawl machinery, the cost model, and the reasoning behind the table layout are in
``src/integrations/services/wheelpros_vehicles.py``. Read that before changing anything here.

Credentials are not passed in: the command picks an active Wheel Pros connection that has API
username/password configured (``--company-provider-id`` pins one). The Vehicle API reuses the
Product Data Portal credentials the Orders API already uses.

ACCESS: /vehicles is entitled separately from Orders/Pricing/Product. An account that places
orders fine can still be refused by every Vehicle API route. ``--check-access`` answers that in
one request before you commit to a long run, and a real run aborts on the first 403 with the same
message rather than grinding through the work list.

Scale: a full pass is one request per year, one per year/make, and two-plus per model — hundreds
of thousands of calls for the whole catalogue. It is resumable: every finished model is appended
to a checkpoint file and skipped on restart. Run it under ``nohup``/``screen``, or slice it by
year and let the slices compose.

Typical runs:

    # is this account even allowed in? one request, writes nothing
    manage.py fetch_wheelpros_vehicles --check-access

    # see the size of the job without crawling details
    manage.py fetch_wheelpros_vehicles --list-only --year 2024

    # rehearse: fetch and parse a few models, write nothing
    manage.py fetch_wheelpros_vehicles --year 2024 --make Ford --limit-models 5 --dry-run

    # one year for real
    manage.py fetch_wheelpros_vehicles --year 2024

    # the whole catalogue
    nohup manage.py fetch_wheelpros_vehicles --concurrency 6 --rate 8 > /tmp/wheelpros_vehicles.log 2>&1 &

    # after a crash: the identical command. Finished models are skipped.
    # to start over instead, pass --no-resume (or delete the checkpoint file)
"""
import pathlib

from django.core.management.base import BaseCommand, CommandError

from src.integrations.clients.wheelpros import exceptions as wheelpros_exceptions
from src.integrations.services import wheelpros_vehicles

DEFAULT_CHECKPOINT_PATH = pathlib.Path("logs/wheelpros_vehicles_checkpoint.jsonl")


class Command(BaseCommand):
    help = (
        "Crawl the Wheel Pros Vehicle API and upsert every year/make/model/submodel, with its "
        "front and rear axle fitment specs, into wheelpros_vehicles. Resumable; safe to re-run."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--year",
            action="append",
            type=int,
            default=None,
            help="Limit to this model year. Repeatable. Omit for every year Wheel Pros lists.",
        )
        parser.add_argument(
            "--make",
            action="append",
            default=None,
            metavar="MAKE",
            help='Limit to this make, e.g. "Ford". Case-insensitive, repeatable.',
        )
        parser.add_argument(
            "--type",
            dest="vehicle_type",
            choices=["wheel", "tire", "both"],
            default=None,
            help=(
                "Restrict the listings to Wheel Pros' wheel or tire catalogue. Omit for the "
                "unfiltered union in one pass (the cheapest full crawl). 'both' walks the two "
                "catalogues separately, doubling the listing calls, and records which one each "
                "vehicle came from on vehicle_types."
            ),
        )
        parser.add_argument(
            "--limit-models",
            type=int,
            default=None,
            help="Crawl at most this many models. For rehearsals, not for production runs.",
        )
        parser.add_argument(
            "--max-submodels",
            type=int,
            default=None,
            help=(
                "Crawl at most this many submodels per model. Rehearsal only -- it writes an "
                "incomplete catalogue, and resume will not come back for the rest."
            ),
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=wheelpros_vehicles.DEFAULT_CONCURRENCY,
            help=f"Models fetched in parallel (default: {wheelpros_vehicles.DEFAULT_CONCURRENCY}).",
        )
        parser.add_argument(
            "--rate",
            type=float,
            default=wheelpros_vehicles.DEFAULT_RATE_PER_SECOND,
            help=(
                f"Requests per second across all workers (default: "
                f"{wheelpros_vehicles.DEFAULT_RATE_PER_SECOND}). Wheel Pros publishes no rate "
                "limit; keep this neighbourly, and back off if you start seeing 429s."
            ),
        )
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="Authenticate with this CompanyProviders row's API credentials instead of the "
            "first active Wheel Pros connection that has them.",
        )
        parser.add_argument(
            "--environment",
            choices=["production", "staging"],
            default=None,
            help="API environment (default: production). Staging needs a separate grant.",
        )
        parser.add_argument(
            "--checkpoint",
            default=str(DEFAULT_CHECKPOINT_PATH),
            help=f"Resume file, one JSON record per finished model (default: {DEFAULT_CHECKPOINT_PATH}).",
        )
        parser.add_argument(
            "--no-resume",
            action="store_true",
            help="Ignore the checkpoint and re-crawl everything. The upsert makes this safe, just slow.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and parse, write nothing -- to the tables or the checkpoint.",
        )
        parser.add_argument(
            "--list-only",
            action="store_true",
            help="Walk years/makes/models, print the size of the job, and stop before any detail call.",
        )
        parser.add_argument(
            "--check-access",
            action="store_true",
            help="Authenticate and make one Vehicle API call to prove entitlement, then exit.",
        )

    def handle(self, *args, **options):
        if options["concurrency"] < 1:
            raise CommandError("--concurrency must be at least 1")
        if options["rate"] <= 0:
            raise CommandError("--rate must be greater than 0")

        try:
            client = wheelpros_vehicles.get_vehicle_api_client(
                company_provider_id=options["company_provider_id"],
                environment=options["environment"],
            )
        except (wheelpros_vehicles.WheelProsVehicleCrawlError, ValueError) as exc:
            raise CommandError(str(exc))

        if options["check_access"]:
            self._check_access(client)
            return

        stats = wheelpros_vehicles.CrawlStats()
        vehicle_types = self._vehicle_types(options["vehicle_type"])

        self.stdout.write("Listing years / makes / models ...")
        try:
            refs = wheelpros_vehicles.discover_models(
                client,
                years=options["year"],
                makes=options["make"],
                vehicle_types=vehicle_types,
                stats=stats,
                progress=self.stdout.write,
            )
        except wheelpros_exceptions.WheelProsVehiclePermissionError as exc:
            raise CommandError(str(exc))

        if not refs:
            raise CommandError("No models matched. Check --year/--make against --list-only.")

        if options["limit_models"] is not None:
            refs = refs[: options["limit_models"]]

        self.stdout.write(
            self.style.SUCCESS(
                "  {} models across {} year/make combinations ({} requests so far)".format(
                    len(refs), stats.makes_seen, client.requests_made
                )
            )
        )

        if options["list_only"]:
            for ref in refs[:50]:
                self.stdout.write("  {}".format(ref))
            if len(refs) > 50:
                self.stdout.write("  ... and {} more".format(len(refs) - 50))
            return

        dry_run = options["dry_run"]
        checkpoint_path = None if dry_run else pathlib.Path(options["checkpoint"])
        self.stdout.write(
            "Crawling {} models (concurrency={}, rate={}/s{})".format(
                len(refs),
                options["concurrency"],
                options["rate"],
                ", DRY RUN" if dry_run else ", checkpoint={}".format(checkpoint_path),
            )
        )

        try:
            stats = wheelpros_vehicles.run_crawl(
                client,
                refs=refs,
                concurrency=options["concurrency"],
                rate_per_second=options["rate"],
                checkpoint_path=checkpoint_path,
                resume=not options["no_resume"],
                dry_run=dry_run,
                max_submodels=options["max_submodels"],
                progress=self.stdout.write,
                stats=stats,
            )
        except wheelpros_exceptions.WheelProsVehiclePermissionError as exc:
            raise CommandError(str(exc))

        self._report(stats, dry_run=dry_run)

    def _check_access(self, client) -> None:
        self.stdout.write("Authenticating as {} ...".format(client.username))
        try:
            client.test_connection()
        except wheelpros_exceptions.WheelProsVehiclePermissionError as exc:
            raise CommandError(str(exc))
        except wheelpros_exceptions.WheelProsException as exc:
            raise CommandError("Vehicle API check failed: {}".format(exc))
        self.stdout.write(self.style.SUCCESS("Vehicle API access confirmed."))

    @staticmethod
    def _vehicle_types(choice):
        if choice == "both":
            return ("wheel", "tire")
        if choice:
            return (choice,)
        return (None,)

    def _report(self, stats: wheelpros_vehicles.CrawlStats, *, dry_run: bool) -> None:
        verb = "would write" if dry_run else "wrote"
        self.stdout.write("")
        self.stdout.write("  models crawled        : {}".format(stats.models_done))
        self.stdout.write("  skipped (checkpoint)  : {}".format(stats.models_skipped))
        self.stdout.write("  no detail published   : {}".format(stats.models_missing))
        self.stdout.write("  requests made         : {}".format(stats.requests_made))
        self.stdout.write("  vehicles {:<13}: {}".format(verb, stats.vehicles_written))
        self.stdout.write("  axles {:<16}: {}".format(verb, stats.axles_written))

        if stats.models_failed:
            self.stdout.write(
                self.style.WARNING(
                    "  failed                : {} (not checkpointed -- re-run the same "
                    "command to retry them)".format(stats.models_failed)
                )
            )
        self.stdout.write(self.style.SUCCESS("Done."))
