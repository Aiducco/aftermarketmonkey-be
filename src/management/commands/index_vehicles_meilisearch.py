from django.core.management.base import BaseCommand

from src.search.meilisearch_client import is_configured, reindex_vehicles_index


class Command(BaseCommand):
    help = (
        "Rebuild the Meilisearch 'vehicles' reference index from MasterPartFitment "
        "(powers the FE's Year/Make/Model cascading selector via facet search). "
        "Not on any cron - run manually after fitment syncs (e.g. sync_asap_network, sync_rough_country)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=5000,
            help="Documents per upload batch (default: 5000).",
        )

    def handle(self, *args, **options):
        if not is_configured():
            self.stdout.write(
                self.style.ERROR(
                    "Meilisearch not configured. Set MEILISEARCH_HOST (and MEILISEARCH_MASTER_KEY if required)."
                )
            )
            return

        self.stdout.write("Rebuilding Meilisearch vehicles index...")
        ok, fail = reindex_vehicles_index(batch_size=options["batch_size"])
        self.stdout.write(
            self.style.SUCCESS("Indexed {} vehicle documents. Failed: {}.".format(ok, fail))
        )
