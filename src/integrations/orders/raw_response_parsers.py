"""
Per-distributor mappers from PurchaseOrderDistributorOrder.raw_response into the purchase-order
detail API's standardized distributor-order fields (distributor_status/tracking/line_items).
Read fresh off the stored raw JSON on every request rather than a separately-synced DB copy, so
the endpoint never depends on a background job having already run. Turn14 first; add an entry
to _PARSERS as each other distributor's raw order-response shape is confirmed.
"""
import typing

from src import enums as src_enums
from src.integrations.orders import turn_14

_EMPTY_PARSED_ORDER = {
    "distributor_status": None,
    "distributor_invoice_ids": [],
    "tracking": [],
    "total": None,
    "freight": None,
    "discount": None,
    "subtotal": None,
    "line_items": [],
}

_PARSERS = {
    src_enums.BrandProviderKind.TURN_14.value: turn_14.parse_order_raw_response,
}


def parse(provider_kind: int, raw_response: typing.Optional[typing.Dict]) -> typing.Dict:
    parser = _PARSERS.get(provider_kind)
    if parser is None:
        return dict(_EMPTY_PARSED_ORDER)
    return parser(raw_response)
