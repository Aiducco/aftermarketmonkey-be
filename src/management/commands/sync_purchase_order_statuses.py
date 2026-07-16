from django.core.management.base import BaseCommand

from src import enums as src_enums
from src import models as src_models
from src.integrations.services import purchase_order_jobs

# In-flight statuses worth polling for fulfillment/tracking updates. DRAFT/QUOTED aren't
# submitted anywhere yet; CANCELLED/FAILED/FULFILLED are terminal.
_IN_FLIGHT_STATUSES = [
    src_enums.PurchaseOrderStatus.SUBMITTED.value,
    src_enums.PurchaseOrderStatus.CONFIRMED.value,
    src_enums.PurchaseOrderStatus.PARTIALLY_FULFILLED.value,
]


class Command(BaseCommand):
    help = (
        "Enqueue a STATUS_CHECK PurchaseOrderJob for every in-flight PurchaseOrder "
        "(SUBMITTED/CONFIRMED/PARTIALLY_FULFILLED), so tracking/fulfillment status stays "
        "current without a staff user manually refreshing. Intended to run from cron every "
        "15-30 minutes. Skips POs that already have an OPEN STATUS_CHECK job pending."
    )

    def handle(self, *args, **options):
        already_pending_po_ids = set(
            src_models.PurchaseOrderJob.objects.filter(
                operation=src_enums.PurchaseOrderOperation.STATUS_CHECK.value,
                status=src_enums.PurchaseOrderJobStatus.OPEN.value,
            ).values_list("purchase_order_id", flat=True)
        )

        po_ids = list(
            src_models.PurchaseOrder.objects.filter(status__in=_IN_FLIGHT_STATUSES)
            .exclude(id__in=already_pending_po_ids)
            .values_list("id", flat=True)
        )

        for po_id in po_ids:
            purchase_order_jobs.enqueue_status_check_job(po_id)

        self.stdout.write(
            self.style.SUCCESS(
                "Enqueued {} status-check job(s) for in-flight purchase order(s).".format(len(po_ids))
            )
        )
