"""
Maps a distributor's ``Providers.kind`` to its ``OrderHistoryReader`` implementation --
additive sibling to ``src.integrations.orders.registry.py``, which maps the same kind to the
write-capable ``DistributorOrderAdapter``. Deliberately NOT imported by, and does not import,
``registry.py`` -- see docs/tasks/external-orders/research-be.md's "Adapters" section for why
these two hierarchies are kept structurally separate.

Only 4 of the 5 distributors this feature was scoped against are registered here -- Motor
State's own live API rejects a no-number listing call outright (HTTP 400, contradicting its
OpenAPI spec), so it has no reader and ``supports_order_discovery`` reports False for it. It
still appears in the FE-facing ``polled_connections`` list with ``status: "not_supported"`` --
see ``src.api.services.purchase_orders._polled_connections``.
"""
import logging
import typing

from src import enums as src_enums
from src import models as src_models
from src.integrations.orders import history

logger = logging.getLogger(__name__)

_HISTORY_READERS: typing.Dict[int, typing.Type[history.OrderHistoryReader]] = {}


def register_history_reader(provider_kind: int, reader_cls: typing.Type[history.OrderHistoryReader]) -> None:
    _HISTORY_READERS[provider_kind] = reader_cls


def supports_order_discovery(provider_kind: int) -> bool:
    """Whether a real order-history reader CLASS is registered for this provider kind -- the
    4-of-5 distributors that can actually discover an order whose number we don't already know
    (see research-be.md's Verdict for why Motor State dropped out)."""
    return provider_kind in _HISTORY_READERS


def get_history_reader(
    company_provider: src_models.CompanyProviders,
    order_account: src_models.CompanyProviderOrderAccount,
) -> typing.Optional[history.OrderHistoryReader]:
    """
    None when no reader is registered for this provider kind, or one is but this account's
    credentials can't construct it -- mirrors ``registry.get_adapter``'s same
    ValueError-swallowing convention, for the same reason: callers expect "no reader" to be a
    normal, handleable outcome (a skipped account in one poll run), not an unhandled exception.
    """
    reader_cls = _HISTORY_READERS.get(company_provider.provider.kind)
    if reader_cls is None:
        return None
    try:
        return reader_cls(company_provider, order_account)
    except ValueError:
        logger.info(
            "History reader for provider kind=%s company_provider_id=%s order_account_id=%s could not be "
            "constructed (order credentials likely missing/invalid for this account).",
            company_provider.provider.kind,
            company_provider.id,
            order_account.id,
        )
        return None


# Self-register each implemented reader. Imported here (rather than via a Django
# AppConfig.ready() hook, which this project doesn't use) so registration happens as soon as
# anything imports this module -- same pattern registry.py itself uses.
from src.integrations.orders.history_readers import (  # noqa: E402
    keystone as _keystone_history,
)
from src.integrations.orders.history_readers import (  # noqa: E402
    meyer as _meyer_history,
)
from src.integrations.orders.history_readers import (  # noqa: E402
    premier as _premier_history,
)
from src.integrations.orders.history_readers import (  # noqa: E402
    turn_14 as _turn_14_history,
)

register_history_reader(src_enums.BrandProviderKind.MEYER.value, _meyer_history.MeyerOrderHistoryReader)
register_history_reader(
    src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value, _premier_history.PremierOrderHistoryReader
)
register_history_reader(src_enums.BrandProviderKind.KEYSTONE.value, _keystone_history.KeystoneOrderHistoryReader)
register_history_reader(src_enums.BrandProviderKind.TURN_14.value, _turn_14_history.Turn14OrderHistoryReader)
