"""
Renders a purchase order to PDF for EmailOrderAdapter.submit_order() — see
src/integrations/orders/email_order.py. Pure rendering: takes the same context dict that gets
persisted as PurchaseOrderSubmissionAttempt.request_payload (see purchase_order_jobs._record_attempt),
so regenerating the PDF later (GET /purchase-orders/<id>/pdf/) is just calling this again with
that stored dict — no separate PDF storage needed.
"""
import io
import typing

from django.template.loader import render_to_string
from xhtml2pdf import pisa


class PurchaseOrderPdfError(Exception):
    """Raised when xhtml2pdf reports one or more errors rendering the PDF."""


def render_purchase_order_pdf(context: typing.Dict) -> bytes:
    html = render_to_string("purchase_order_pdf.html", context)
    buffer = io.BytesIO()
    result = pisa.CreatePDF(src=html, dest=buffer)
    if result.err:
        raise PurchaseOrderPdfError(
            "xhtml2pdf reported {} error(s) rendering the purchase order PDF.".format(result.err)
        )
    return buffer.getvalue()
