"""
Reading the old standalone ShopMonkey tool's database (~/Documents/code/shopmonkey) and porting one brand
of it onto a company's ShopMonkeySyncBrand / ShopMonkeySyncPart rows.

This module writes DATA ONLY. It never calls ShopMonkey to create, update or delete anything, and it holds no
sync logic: the one sync path in src.integrations.services.shopmonkey_sync stays the only thing that pushes.
What a port carries across is exactly what cannot be re-derived -- which ShopMonkey part each master part IS
(its id), plus what was last pushed for it (SKU, name, prices, bin code) so that the first real sync sees
"unchanged" or a genuine difference, rather than rewriting ~every part to our own conventions.

Parts are linked, never guessed: an old part joins a master part only on an exact part-number match that is
unique, and only when the brand's vendor actually carries that master part. Everything else is reported and
left alone in ShopMonkey.

Lookups go through the indexed path only -- master_parts by (brand_id), then provider_parts by
(master_part_id, provider_id). provider_parts has no index on provider_external_id, and filtering on it in
production is a scan of millions of rows; nothing here may do that.
"""
import collections
import dataclasses
import logging
import os
import re
import typing

import psycopg2
from django.core.management.base import CommandError
from django.db import transaction
from dotenv import dotenv_values

from src import enums as src_enums
from src import models as src_models

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[SHOPMONKEY-IMPORT]"

DEFAULT_OLD_REPO = "~/Documents/code/shopmonkey"

# Old tool's InventoryVendor enum value -> our Providers.kind. SDC (4) has no target on purpose: it is a
# catalog, not a distributor of ours.
OLD_PROVIDER_TO_KIND: typing.Dict[int, typing.Optional[int]] = {
    1: src_enums.BrandProviderKind.TURN_14.value,
    2: src_enums.BrandProviderKind.KEYSTONE.value,
    3: src_enums.BrandProviderKind.WHEELPROS.value,  # WHEEL_PROS there, WHEELPROS here
    4: None,  # SDC
    5: src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
    6: src_enums.BrandProviderKind.MEYER.value,
}

_CHUNK = 500

Part = typing.Dict[str, typing.Any]


def connect_old_db(old_repo: str):
    """A read-only session on the old tool's database. Credentials come from the old repo's own .env, so
    nothing is copied or typed anywhere."""
    env_path = os.path.join(os.path.expanduser(old_repo), ".env")
    if not os.path.exists(env_path):
        raise CommandError("No .env at {} -- pass --old-repo.".format(env_path))
    env = dotenv_values(env_path)
    keys = ("DATABASE_HOST", "DATABASE_PORT", "DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD")
    missing = [k for k in keys if not env.get(k)]
    if missing:
        raise CommandError("Old repo .env is missing {}.".format(", ".join(missing)))
    connection = psycopg2.connect(
        host=env["DATABASE_HOST"],
        port=env["DATABASE_PORT"],
        dbname=env["DATABASE_NAME"],
        user=env["DATABASE_USER"],
        password=env["DATABASE_PASSWORD"],
        connect_timeout=10,
    )
    connection.set_session(readonly=True, autocommit=True)
    return connection


def _norm(value: typing.Optional[str]) -> str:
    return re.sub(r"\s+", "", (value or "")).upper()


def _rows(connection, sql: str, params: typing.Sequence = ()) -> typing.List[typing.Dict[str, typing.Any]]:
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


@dataclasses.dataclass
class BrandImportPlan:
    company_id: int
    old_brand: typing.Dict[str, typing.Any]
    our_brand: src_models.Brands
    provider: src_models.Providers
    vendor: src_models.ShopMonkeySyncVendor
    existing_sync_brand: typing.Optional[src_models.ShopMonkeySyncBrand]
    linkable: typing.List[typing.Tuple[Part, src_models.MasterPart]] = dataclasses.field(default_factory=list)
    not_carried: typing.List[typing.Tuple[Part, src_models.MasterPart]] = dataclasses.field(default_factory=list)
    unresolved: typing.List[Part] = dataclasses.field(default_factory=list)
    ambiguous: typing.List[Part] = dataclasses.field(default_factory=list)
    collisions: typing.List[Part] = dataclasses.field(default_factory=list)
    gone: typing.List[Part] = dataclasses.field(default_factory=list)
    already_imported: int = 0
    problems: typing.List[str] = dataclasses.field(default_factory=list)


def plan_brand_import(
    company_id: int,
    old_brand_name: str,
    our_brand_id: int,
    old_repo: str = DEFAULT_OLD_REPO,
    old_vendor_name: typing.Optional[str] = None,
    live_index: typing.Optional[typing.Dict[str, str]] = None,
) -> BrandImportPlan:
    connection = connect_old_db(old_repo)
    try:
        sql = (
            "SELECT b.id, b.name, b.sink_external_id AS sm_brand_id, b.status_name, "
            "v.provider, v.provider_name, v.vendor_bin_name, v.sink_external_id AS sm_vendor_id "
            "FROM inventory_brand b JOIN inventory_vendor v ON v.id = b.vendor_id WHERE b.name = %s"
        )
        params: typing.List[typing.Any] = [old_brand_name]
        if old_vendor_name:
            sql += " AND v.provider_name = %s"
            params.append(old_vendor_name)
        brands = _rows(connection, sql, params)
        if not brands:
            raise CommandError("No old brand named {!r}.".format(old_brand_name))
        if len(brands) > 1:
            raise CommandError(
                "{!r} exists under several vendors ({}); pass --old-vendor.".format(
                    old_brand_name, ", ".join(b["provider_name"] for b in brands)
                )
            )
        old_brand = brands[0]
        old_parts = _rows(
            connection,
            "SELECT sku, number, name, external_id, retail_cost_in_cents, wholesale_cost_in_cents, status_name "
            "FROM inventory_part WHERE brand_id = %s ORDER BY id",
            [old_brand["id"]],
        )
    finally:
        connection.close()

    kind = OLD_PROVIDER_TO_KIND.get(old_brand["provider"])
    if kind is None:
        raise CommandError("Old vendor {} has no equivalent distributor here.".format(old_brand["provider_name"]))
    provider = src_models.Providers.objects.get(kind=kind)
    our_brand = src_models.Brands.objects.filter(id=our_brand_id).first()
    if not our_brand:
        raise CommandError("No brand with id {}.".format(our_brand_id))
    vendor = src_models.ShopMonkeySyncVendor.objects.filter(company_id=company_id, provider=provider).first()
    if not vendor or not vendor.shopmonkey_external_id:
        raise CommandError("No linked vendor for {} -- run import_shopmonkey_vendors first.".format(provider.kind_name))

    existing = src_models.ShopMonkeySyncBrand.objects.filter(company_id=company_id, brand=our_brand).first()
    plan = BrandImportPlan(
        company_id=company_id,
        old_brand=dict(old_brand, parts=old_parts),
        our_brand=our_brand,
        provider=provider,
        vendor=vendor,
        existing_sync_brand=existing,
    )

    if vendor.shopmonkey_external_id != old_brand["sm_vendor_id"]:
        plan.problems.append(
            "vendor id differs: ours {} vs old {}".format(vendor.shopmonkey_external_id, old_brand["sm_vendor_id"])
        )
    if not old_brand["sm_brand_id"]:
        plan.problems.append("the old brand was never created on ShopMonkey (no id to carry)")
    if existing:
        if existing.vendor_id != vendor.id:
            plan.problems.append("a sync brand already exists on a different vendor")
        if existing.shopmonkey_external_id and existing.shopmonkey_external_id != old_brand["sm_brand_id"]:
            plan.problems.append(
                "sync brand already holds a different ShopMonkey brand id ({})".format(existing.shopmonkey_external_id)
            )

    # Resolve against THIS brand's master parts only (indexed, small) -- never a scan of provider_parts.
    master_parts = list(src_models.MasterPart.objects.filter(brand_id=our_brand_id))
    by_number: typing.Dict[str, typing.List[src_models.MasterPart]] = collections.defaultdict(list)
    by_sku: typing.Dict[str, typing.List[src_models.MasterPart]] = collections.defaultdict(list)
    for mp in master_parts:
        by_number[_norm(mp.part_number)].append(mp)
        if mp.sku:
            by_sku[_norm(mp.sku)].append(mp)

    carried: typing.Set[int] = set()
    ids = [mp.id for mp in master_parts]
    for i in range(0, len(ids), _CHUNK):
        carried.update(
            src_models.ProviderPart.objects.filter(
                master_part_id__in=ids[i : i + _CHUNK], provider=provider
            ).values_list("master_part_id", flat=True)
        )

    already: typing.Set[int] = set()
    if existing:
        already = set(
            src_models.ShopMonkeySyncPart.objects.filter(sync_brand=existing).values_list("master_part_id", flat=True)
        )

    if live_index is not None:
        elsewhere = [
            part["sku"]
            for part in old_parts
            if part["external_id"] in live_index and live_index[part["external_id"]] != old_brand["sm_brand_id"]
        ]
        if elsewhere:
            plan.problems.append(
                "{} part(s) live under another brand: {}".format(len(elsewhere), ", ".join(elsewhere[:5]))
            )

    claimed: typing.Set[int] = set()
    for part in old_parts:
        candidates = by_number.get(_norm(part["number"]), [])
        if not candidates:
            candidates = by_sku.get(_norm(part["sku"]), [])
        if len(candidates) > 1:
            plan.ambiguous.append(part)
            continue
        if not candidates:
            plan.unresolved.append(part)
            continue
        mp = candidates[0]
        if mp.id in claimed:
            plan.collisions.append(part)
            continue
        claimed.add(mp.id)
        if mp.id in already:
            plan.already_imported += 1
        elif live_index is not None and part["external_id"] not in live_index:
            # The part no longer exists in ShopMonkey (a GET returns 404): it was deleted after the old tool
            # last tracked it. It cannot be updated, so it is not linked -- and it is kept out of the plan
            # rather than holding up the brand. Note what that means for the first sync: if the vendor still
            # carries the master part, the sync will create it again, as a new part. Whether a deletion was
            # deliberate is something only the shop can say, so these are reported, never decided here.
            plan.gone.append(part)
        elif mp.id in carried:
            plan.linkable.append((part, mp))
        else:
            plan.not_carried.append((part, mp))
    return plan


def apply_brand_import(plan: BrandImportPlan) -> typing.Dict[str, int]:
    """Writes the sync brand -- DISABLED: nothing syncs until someone enables it after reviewing a rehearsal --
    and one ShopMonkeySyncPart per linkable old part. Idempotent."""
    if plan.problems:
        raise CommandError("Refusing to apply with open problems: {}".format("; ".join(plan.problems)))

    old_brand = plan.old_brand
    with transaction.atomic():
        sync_brand = plan.existing_sync_brand
        created_brand = False
        if sync_brand is None:
            sync_brand = src_models.ShopMonkeySyncBrand.objects.create(
                company_id=plan.company_id,
                brand=plan.our_brand,
                vendor=plan.vendor,
                shopmonkey_external_id=old_brand["sm_brand_id"],
                entity="Part",
                enabled=False,
                status=src_enums.ShopMonkeySyncStatus.ACTIVE.value,
                status_name=src_enums.ShopMonkeySyncStatus.ACTIVE.name,
            )
            created_brand = True
        elif not sync_brand.shopmonkey_external_id:
            sync_brand.shopmonkey_external_id = old_brand["sm_brand_id"]
            sync_brand.status = src_enums.ShopMonkeySyncStatus.ACTIVE.value
            sync_brand.status_name = src_enums.ShopMonkeySyncStatus.ACTIVE.name
            sync_brand.save(update_fields=["shopmonkey_external_id", "status", "status_name", "updated_at"])

        rows = []
        for part, mp in plan.linkable:
            status = src_enums.ShopMonkeySyncPartStatus[part["status_name"]]
            rows.append(
                src_models.ShopMonkeySyncPart(
                    sync_brand=sync_brand,
                    master_part=mp,
                    shopmonkey_external_id=part["external_id"],
                    last_pushed_name=part["name"],
                    last_pushed_sku=part["sku"],
                    last_pushed_retail_cost_cents=part["retail_cost_in_cents"],
                    last_pushed_wholesale_cost_cents=part["wholesale_cost_in_cents"],
                    last_pushed_bin_location=old_brand["vendor_bin_name"],
                    sourced_from_vendor=plan.vendor,
                    status=status.value,
                    status_name=status.name,
                )
            )
        src_models.ShopMonkeySyncPart.objects.bulk_create(rows, batch_size=_CHUNK)

    logger.info(
        "{} Imported brand {!r} for company_id={}: sync_brand_id={} created_brand={} parts={}".format(
            _LOG_PREFIX, old_brand["name"], plan.company_id, sync_brand.id, created_brand, len(rows)
        )
    )
    return {"sync_brand_id": sync_brand.id, "created_brand": int(created_brand), "parts_created": len(rows)}
