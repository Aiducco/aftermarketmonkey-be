from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import motorstate


class Command(BaseCommand):
    help = (
        "First-time Motor State raw ingest: brands -> availability spine -> product "
        "detail/pricing. Raw tables only; nothing is propagated into master parts."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-provider-id",
            type=int,
            default=None,
            help="Specific CompanyProviders id; defaults to the primary active Motor State connection.",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=motorstate._DEFAULT_PRODUCT_WORKERS,
            help="Parallel /api/Product workers for the hydrate step (default 5).",
        )
        parser.add_argument(
            "--skip-products",
            action="store_true",
            help="Only sync brands + availability spine; skip the (slow) product hydrate.",
        )

    def handle(self, *args, **options):
        company_provider_id = options["company_provider_id"]
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_motorstate_full_sync")
        self.stdout.write("Starting full Motor State raw sync...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_motorstate_full_sync")
        try:
            self.stdout.write("1/3 Fetching brands...")
            motorstate.fetch_and_save_motorstate_brands(company_provider_id=company_provider_id)

            self.stdout.write("2/3 Building availability spine (per-brand)...")
            motorstate.fetch_and_save_motorstate_availability_full(company_provider_id=company_provider_id)

            if options["skip_products"]:
                self.stdout.write("3/3 Skipping product hydrate (--skip-products).")
            else:
                self.stdout.write("3/3 Hydrating product detail + pricing (parallel)...")
                motorstate.fetch_and_save_motorstate_products(
                    company_provider_id=company_provider_id,
                    workers=options["workers"],
                )

            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed full Motor State raw sync."
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed full Motor State raw sync."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
