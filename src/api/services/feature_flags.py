"""
Per-company feature gating, independent of plan tier -- see CompanyFeatureFlags in src/models.py.

A staged-rollout mechanism: a feature is invisible/unavailable for every company until explicitly
enabled for it, one company (or a short allowlist) at a time, via enable_company_feature /
disable_company_feature (also exposed as the enable_company_feature management command). Not a
replacement for plan-tier limits (see src.api.services.billing) -- this is for "not ready for
everyone yet", not "paid tier only".
"""
import typing

from src import models as src_models

# Feature names -- add one constant per gated feature as it's introduced. Checked against
# CompanyFeatureFlags.feature (a plain string column, not an enum, so a new feature never needs
# a migration).
FEATURE_SHOP_MANAGEMENT = "shop_management"


def company_has_feature(company_id: typing.Optional[int], feature: str) -> bool:
    if not company_id:
        return False
    return src_models.CompanyFeatureFlags.objects.filter(
        company_id=company_id, feature=feature, enabled=True
    ).exists()


def enable_company_feature(company_id: int, feature: str) -> None:
    src_models.CompanyFeatureFlags.objects.update_or_create(
        company_id=company_id, feature=feature, defaults={"enabled": True}
    )


def disable_company_feature(company_id: int, feature: str) -> None:
    src_models.CompanyFeatureFlags.objects.update_or_create(
        company_id=company_id, feature=feature, defaults={"enabled": False}
    )
