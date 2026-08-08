from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import motorstate


class Command(BaseCommand):
    help = (
        "Hydrate Motor State product detail + account pricing via parallel "
        "/api/Product calls (<=15 part numbers each) over the stored spine."
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
            help="Parallel /api/Product workers (default 5).",
        )
        parser.add_argument(
            "--include-discontinued",
            action="store_true",
            help="Also hydrate discontinued (StatusType X) parts, which return no detail.",
        )

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions("fetch_motorstate_products")
        self.stdout.write("Starting Motor State product hydrate...")
        execution = audit_scheduled_tasks.start_scheduled_task_execution("fetch_motorstate_products")
        try:
            motorstate.fetch_and_save_motorstate_products(
                company_provider_id=options["company_provider_id"],
                workers=options["workers"],
                include_discontinued=options["include_discontinued"],
            )
            audit_scheduled_tasks.mark_scheduled_task_completed(
                execution, message="Successfully completed Motor State product hydrate."
            )
            self.stdout.write(self.style.SUCCESS("Successfully completed Motor State product hydrate."))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise
