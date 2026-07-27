"""
Per-distributor mappers from a PurchaseOrderDistributorOrder row into the purchase-order detail
API's standardized distributor-order fields (distributor_status/tracking/invoice ids/line
items). Read fresh off already-stored distributor data on every request rather than a
separately-synced DB copy, so the endpoint never depends on a background job having already
run. Each distributor reads from whichever field actually holds its decoded data -- Turn14 and
Meyer from raw_response directly (see turn_14.parse_order_raw_response / meyer.
parse_order_raw_response), Keystone from the pre-decoded processed_order (see keystone.
parse_processed_order, since GetOrderHistory's raw rows aren't usable without
decode_and_merge_order_history's per-line-item merge having already run at refresh time). Add
an entry to _PARSERS as each other distributor's data is confirmed.
"""
import typing

from src import enums as src_enums
from src import models as src_models
from src.integrations.orders import keystone
from src.integrations.orders import meyer
from src.integrations.orders import turn_14

_EMPTY_PARSED_ORDER = {
    "distributor_order_number": None,
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
    src_enums.BrandProviderKind.TURN_14.value: lambda pdo: turn_14.parse_order_raw_response(pdo.raw_response),
    src_enums.BrandProviderKind.KEYSTONE.value: lambda pdo: keystone.parse_processed_order(pdo.processed_order),
    src_enums.BrandProviderKind.MEYER.value: lambda pdo: meyer.parse_order_raw_response(pdo.raw_response),
}


def parse(provider_kind: int, pdo: "src_models.PurchaseOrderDistributorOrder") -> typing.Dict:
    parser = _PARSERS.get(provider_kind)
    if parser is None:
        return dict(_EMPTY_PARSED_ORDER)
    return parser(pdo)
