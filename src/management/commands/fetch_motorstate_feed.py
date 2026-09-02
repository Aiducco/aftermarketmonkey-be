from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import motorstate_feed


class Command(BaseCommand):
    help = (
        "Download the Motor State FTP feed and upsert the catalog (MotorStateProduct) from the "
        "primary connection, then each company's own prices (MotorStateCompanyPricing) from its "
        "own feed file. Requires CompanyProviders for Motor State with ftp_user / ftp_password "
        "in credentials['feed']."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-force-download",
            action="store_true",
            help="Reuse the local cached feed file when it is still fresh.",
        )
        parser.add_argument(
            "--skip-catalog",
            action="store_true",
            help="Skip the catalog pass; only refresh per-company pricing.",
        )
        parser.add_argument(
            "--skip-content",
            action="store_true",
            help="Skip the content overlay (image / categories / long description from the "
                 "content feed, when the connection has content_ftp_user credentials).",
        )
        parser.add_argument(
            "--skip-pricing",
            action="store_true",
            help="Skip per-company pricing; only refresh the shared catalog.",
        )
        parser.add_argument(
            "--no-delist",
            action="store_true",
            help="Leave catalog rows absent from the feed marked found=True (default is to "
                 "delist them, since stock is feed-only).",
        )
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="Price only this connection (and, with --skip-pricing, read the catalog from it).",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_motorstate_feed")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_motorstate_feed")
        force_download = not options["no_force_download"]
        try:
            if not options["skip_catalog"]:
                self.stdout.write("Fetching Motor State catalog from FTP feed...")
                upserted = motorstate_feed.fetch_and_save_motorstate_catalog_from_feed(
                    force_download=force_download,
                    company_provider_id=options["company_provider_id"],
                    delist_missing=not options["no_delist"],
                )
                self.stdout.write("Catalog rows upserted: {}".format(upserted))

            if not options["skip_content"]:
                self.stdout.write("Overlaying content (image / categories / long description)...")
                enriched = motorstate_feed.sync_motorstate_content_from_feed(
                    company_provider_id=options["company_provider_id"],
                    force_download=force_download,
                )
                self.stdout.write("Catalog rows enriched: {}".format(enriched))

            if not options["skip_pricing"]:
                if options["company_provider_id"]:
                    self.stdout.write(
                        "Fetching Motor State pricing for company_provider_id={}...".format(
                            options["company_provider_id"]
                        )
                    )
                    priced = motorstate_feed.sync_motorstate_company_pricing_from_feed(
                        options["company_provider_id"], force_download=force_download
                    )
                    self.stdout.write("Pricing rows upserted: {}".format(priced))
                else:
                    self.stdout.write("Fetching Motor State pricing for every active connection...")
                    results = motorstate_feed.sync_motorstate_company_pricing_from_feed_for_all_companies(
                        force_download=force_download
                    )
                    for cp_id, count in sorted(results.items()):
                        self.stdout.write("  company_provider_id={}: {} rows".format(cp_id, count))

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed Motor State feed ingest."
            )
            self.stdout.write(self.style.SUCCESS("Done."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
