"""
Who may use Inventory Management (the ShopMonkey inventory sync).

Two independent ways in, either is enough:
  - the plan includes it (PLANS[...]["features"]["inventory_management"], see billing.has_inventory_management), or
  - the company carries FEATURE_SHOPMONKEY_INVENTORY_SYNC -- the original per-company switch, kept for the pilot
    customer (Trident) and as a manual override for anyone granted access outside their plan.

Every gate for the module goes through this one function: the API, the login token's nav claim, the scheduled sync
and the cache warmer. Keeping it in one place is what lets the plan switch be flipped without touching any of them.
"""
import typing

from src.api.services import billing as billing_services
from src.api.services import feature_flags as feature_flags_services


def company_has_inventory_access(company_id: typing.Optional[int]) -> bool:
    if not company_id:
        return False
    return feature_flags_services.company_has_feature(
        company_id, feature_flags_services.FEATURE_SHOPMONKEY_INVENTORY_SYNC
    ) or billing_services.has_inventory_management(company_id)
