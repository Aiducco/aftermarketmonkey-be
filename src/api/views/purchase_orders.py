import json
import logging
import typing

import simplejson
from django import http, views

from src.api.services import purchase_orders as purchase_orders_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[PURCHASE-ORDERS]"


def _require_auth(
    request: http.HttpRequest,
) -> typing.Tuple[typing.Optional[int], typing.Optional[int], typing.Optional[http.HttpResponse]]:
    """Returns (company_id, user_id, error_response). error_response is None on success."""
    if not request.user or not request.user.is_authenticated:
        logger.warning("{} User not authenticated for {}".format(_LOG_PREFIX, request.path))
        return None, None, http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"message": "User not authenticated"}),
            status=401,
        )
    company_id = getattr(request, "company_id", None)
    if not company_id:
        return None, None, http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps({"message": "No company found in token"}),
            status=400,
        )
    return company_id, request.user.id, None


def _json_response(data: typing.Any, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps({"data": data}),
        status=status,
    )


def _error_response(message: str, status: int = 400) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps({"message": message}),
        status=status,
    )


def _parse_json_body(request: http.HttpRequest) -> typing.Tuple[typing.Optional[dict], typing.Optional[http.HttpResponse]]:
    try:
        body = json.loads(request.body) if request.body else {}
    except json.JSONDecodeError:
        return None, _error_response("Invalid JSON body")
    if not isinstance(body, dict):
        return None, _error_response("Invalid JSON body")
    return body, None


class CartView(views.View):
    """GET /purchase-orders/cart/ — Add-to-PO cart (one DRAFT PurchaseOrder per distributor).
    POST /purchase-orders/cart/items/ — Add (or increment) a line item."""

    def get(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err
        try:
            return _json_response(purchase_orders_services.get_cart(company_id))
        except Exception:
            logger.exception("{} Error fetching cart for company_id={}".format(_LOG_PREFIX, company_id))
            return _error_response("Error fetching cart", status=500)


class CartItemsView(views.View):
    """POST /purchase-orders/cart/items/ — {provider_id, master_part_id, quantity}.
    Both ids are exactly what GET /parts/<id>/ already returns (providers[].provider_id and
    the part's own id) — no separate lookup needed to add something to the cart."""

    def post(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, user_id, err = _require_auth(request)
        if err:
            return err
        body, err = _parse_json_body(request)
        if err:
            return err

        try:
            result = purchase_orders_services.add_cart_item(
                company_id=company_id,
                user_id=user_id,
                provider_id=body.get("provider_id"),
                master_part_id=body.get("master_part_id"),
                quantity=body.get("quantity"),
            )
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e))
        except Exception:
            logger.exception("{} Error adding cart item for company_id={}".format(_LOG_PREFIX, company_id))
            return _error_response("Error adding cart item", status=500)

        return _json_response(result, status=201)


class CartItemDetailView(views.View):
    """PATCH /purchase-orders/cart/items/<line_item_id>/ — {quantity}
    DELETE /purchase-orders/cart/items/<line_item_id>/"""

    def patch(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err
        body, err = _parse_json_body(request)
        if err:
            return err

        line_item_id = kwargs.get("line_item_id")
        try:
            result = purchase_orders_services.update_cart_item(
                company_id=company_id, line_item_id=line_item_id, quantity=body.get("quantity")
            )
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e), status=404)
        except Exception:
            logger.exception("{} Error updating cart item id={}".format(_LOG_PREFIX, line_item_id))
            return _error_response("Error updating cart item", status=500)

        return _json_response(result)

    def delete(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        line_item_id = kwargs.get("line_item_id")
        try:
            result = purchase_orders_services.remove_cart_item(company_id=company_id, line_item_id=line_item_id)
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e), status=404)
        except Exception:
            logger.exception("{} Error removing cart item id={}".format(_LOG_PREFIX, line_item_id))
            return _error_response("Error removing cart item", status=500)

        return _json_response(result)


class CartReviewView(views.View):
    """POST /purchase-orders/cart/review/ — {ship_to: {...}, purchase_order_ids?: [...],
    reference?, ship_methods?: {"<purchase_order_id>": "<method_code>"}}.
    Groups the current DRAFT carts into a PurchaseOrderGroup and enqueues a QUOTE job for
    each. ship_methods is per-PO (not per-request) since method codes are distributor-specific
    — see GET .../shipping-methods/?company_provider_id="""

    def post(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, user_id, err = _require_auth(request)
        if err:
            return err
        body, err = _parse_json_body(request)
        if err:
            return err

        try:
            result = purchase_orders_services.review_cart(
                company_id=company_id,
                user_id=user_id,
                ship_to=body.get("ship_to") or {},
                purchase_order_ids=body.get("purchase_order_ids"),
                group_reference=body.get("reference"),
                ship_methods=body.get("ship_methods"),
            )
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e))
        except Exception:
            logger.exception("{} Error reviewing cart for company_id={}".format(_LOG_PREFIX, company_id))
            return _error_response("Error reviewing cart", status=500)

        return _json_response(result, status=202)


class PurchaseOrdersView(views.View):
    """GET /purchase-orders/ — PO history list. Optional ?status=&company_provider_id="""

    def get(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        status_param = request.GET.get("status")
        cp_param = request.GET.get("company_provider_id")
        try:
            result = purchase_orders_services.list_purchase_orders(
                company_id=company_id,
                status=status_param,
                company_provider_id=int(cp_param) if cp_param else None,
            )
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e))
        except Exception:
            logger.exception("{} Error listing purchase orders for company_id={}".format(_LOG_PREFIX, company_id))
            return _error_response("Error listing purchase orders", status=500)

        return _json_response(result)


class PurchaseOrderDetailView(views.View):
    """GET /purchase-orders/<id>/"""

    def get(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        po_id = kwargs.get("id")
        try:
            result = purchase_orders_services.get_purchase_order_detail(company_id=company_id, purchase_order_id=po_id)
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e), status=404)
        except Exception:
            logger.exception("{} Error fetching purchase order id={}".format(_LOG_PREFIX, po_id))
            return _error_response("Error fetching purchase order", status=500)

        return _json_response(result)


class PurchaseOrderSubmitView(views.View):
    """POST /purchase-orders/<id>/submit/ — enqueues a SUBMIT job (places a real order)."""

    def post(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        po_id = kwargs.get("id")
        try:
            result = purchase_orders_services.submit_purchase_order(company_id=company_id, purchase_order_id=po_id)
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e))
        except Exception:
            logger.exception("{} Error submitting purchase order id={}".format(_LOG_PREFIX, po_id))
            return _error_response("Error submitting purchase order", status=500)

        return _json_response(result, status=202)


class PurchaseOrderCancelView(views.View):
    """POST /purchase-orders/<id>/cancel/"""

    def post(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        po_id = kwargs.get("id")
        try:
            result = purchase_orders_services.cancel_purchase_order(company_id=company_id, purchase_order_id=po_id)
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e))
        except Exception:
            logger.exception("{} Error cancelling purchase order id={}".format(_LOG_PREFIX, po_id))
            return _error_response("Error cancelling purchase order", status=500)

        return _json_response(result, status=202)


class PurchaseOrderRefreshStatusView(views.View):
    """POST /purchase-orders/<id>/refresh-status/"""

    def post(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        po_id = kwargs.get("id")
        try:
            result = purchase_orders_services.refresh_purchase_order_status(
                company_id=company_id, purchase_order_id=po_id
            )
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e), status=404)
        except Exception:
            logger.exception("{} Error refreshing status for purchase order id={}".format(_LOG_PREFIX, po_id))
            return _error_response("Error refreshing purchase order status", status=500)

        return _json_response(result, status=202)


class PurchaseOrderRequoteView(views.View):
    """POST /purchase-orders/<id>/requote/ — re-run the quote (e.g. after quote_is_stale is
    true) without rebuilding the cart. Reuses the ship-to/ship-method already on the PO."""

    def post(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        po_id = kwargs.get("id")
        try:
            result = purchase_orders_services.requote_purchase_order(company_id=company_id, purchase_order_id=po_id)
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e))
        except Exception:
            logger.exception("{} Error re-quoting purchase order id={}".format(_LOG_PREFIX, po_id))
            return _error_response("Error re-quoting purchase order", status=500)

        return _json_response(result, status=202)


class PurchaseOrderJobView(views.View):
    """GET /purchase-orders/<id>/jobs/<job_id>/ — poll job status."""

    def get(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        po_id = kwargs.get("id")
        job_id = kwargs.get("job_id")
        try:
            result = purchase_orders_services.get_job_status(
                company_id=company_id, purchase_order_id=po_id, job_id=job_id
            )
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e), status=404)
        except Exception:
            logger.exception("{} Error fetching job id={} for purchase order id={}".format(_LOG_PREFIX, job_id, po_id))
            return _error_response("Error fetching job status", status=500)

        return _json_response(result)


class PurchaseOrderGroupDetailView(views.View):
    """GET /purchase-orders/groups/<id>/"""

    def get(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        group_id = kwargs.get("id")
        try:
            result = purchase_orders_services.get_purchase_order_group_detail(company_id=company_id, group_id=group_id)
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e), status=404)
        except Exception:
            logger.exception("{} Error fetching purchase order group id={}".format(_LOG_PREFIX, group_id))
            return _error_response("Error fetching purchase order group", status=500)

        return _json_response(result)


class PurchaseOrderGroupSubmitView(views.View):
    """POST /purchase-orders/groups/<id>/submit/ — submits every QUOTED PO in the group."""

    def post(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        group_id = kwargs.get("id")
        try:
            result = purchase_orders_services.submit_purchase_order_group(company_id=company_id, group_id=group_id)
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e))
        except Exception:
            logger.exception("{} Error submitting purchase order group id={}".format(_LOG_PREFIX, group_id))
            return _error_response("Error submitting purchase order group", status=500)

        return _json_response(result, status=202)


class PurchaseOrderCapabilitiesView(views.View):
    """GET /purchase-orders/capabilities/ — per connected distributor, can_order_in_app."""

    def get(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        try:
            result = purchase_orders_services.get_order_capabilities(company_id=company_id)
        except Exception:
            logger.exception("{} Error fetching order capabilities for company_id={}".format(_LOG_PREFIX, company_id))
            return _error_response("Error fetching order capabilities", status=500)

        return _json_response(result)


class ShippingMethodsView(views.View):
    """GET /purchase-orders/shipping-methods/?company_provider_id= — catalog of selectable
    shipping method names for this distributor connection (not priced — see the plan notes
    on get_shipping_quote's ship_method param)."""

    def get(self, request: http.HttpRequest, *args, **kwargs) -> http.HttpResponse:
        company_id, _user_id, err = _require_auth(request)
        if err:
            return err

        company_provider_id = request.GET.get("company_provider_id")
        if not company_provider_id:
            return _error_response("company_provider_id query param is required")

        try:
            result = purchase_orders_services.get_shipping_methods(
                company_id=company_id, company_provider_id=int(company_provider_id)
            )
        except purchase_orders_services.PurchaseOrderServiceError as e:
            return _error_response(str(e))
        except Exception:
            logger.exception(
                "{} Error fetching shipping methods for company_provider_id={}".format(
                    _LOG_PREFIX, company_provider_id
                )
            )
            return _error_response("Error fetching shipping methods", status=500)

        return _json_response(result)
