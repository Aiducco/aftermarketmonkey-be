"""
Common interface for on-demand, single-item live inventory lookups against a distributor's
API - distinct from the read-only bulk feed sync in src/integrations/services/ (which pulls
whole-brand paginated inventory on a schedule) and from src/integrations/orders/ (which places
orders). This is the "refresh this one item's inventory right now" capability behind the
part-detail refresh action.

Kept as its own registry (mirroring src/integrations/orders/registry.py) since not every
distributor's feed API exposes a single-item lookup even where its ordering API does (or vice
versa) - the two capabilities land independently per distributor.
"""
import abc

from src import models as src_models


class LiveInventoryProvider(abc.ABC):
    """One instance per CompanyProviders connection (holds that connection's feed credentials)."""

    provider_kind: int  # src.enums.BrandProviderKind value, set by each subclass

    def __init__(self, company_provider: src_models.CompanyProviders) -> None:
        self.company_provider = company_provider

    @abc.abstractmethod
    def refresh(self, provider_part: src_models.ProviderPart) -> src_models.ProviderPartInventory:
        """
        Fetches this item's current inventory from the distributor, writes it to the raw
        per-distributor inventory table (so the next bulk sync sees consistent data), then
        upserts ProviderPartInventory and returns the saved row.

        Raises live_inventory.exceptions.LiveInventoryNotFoundError if the distributor has no
        inventory record for this item, or LiveInventoryTransportError on an API/transport
        failure.
        """
        raise NotImplementedError
