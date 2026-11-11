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
from src.integrations.clients.shopmonkey import exceptions as shopmonkey_exceptions
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services

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


def build_live_index(client, old_parts: typing.List[Part]) -> typing.Dict[str, str]:
    """
    {ShopMonkey part id -> its brand id} for every old part that still exists in the account, by asking for each.

    Only a 404 means "deleted". Anything else -- a timeout, a 500, a rejected key -- is re-raised and aborts the
    plan: a part wrongly classified as deleted would be recorded as SKIPPED and quietly never synced, which is a
    much worse failure than not finishing.
    """
    index: typing.Dict[str, str] = {}
    for part in old_parts:
        part_id = part.get("external_id")
        if not part_id:
            continue
        try:
            live = client.get_inventory_part(part_id)
        except shopmonkey_exceptions.ShopMonkeyValidationError as e:
            if e.code == "404":
                continue
            raise
        live = live.get("data", live) if isinstance(live, dict) and "data" in live else live
        if live and not live.get("deleted"):
            index[part_id] = live.get("brandId")
    return index


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
    # Every vendor this brand will draw from, in priority order. vendors[0] is the primary (== `vendor`). More than
    # one only for a brand that came from SDC -- SDC is a catalog, not a distributor of ours, so such a brand has to
    # be re-homed onto real ones. Everything else is exactly one vendor, as in the old tool.
    vendors: typing.List[src_models.ShopMonkeySyncVendor] = dataclasses.field(default_factory=list)
    # (old part, master part, the vendor that will source it: the first in priority order that carries it)
    linkable: typing.List[
        typing.Tuple[Part, src_models.MasterPart, src_models.ShopMonkeySyncVendor]
    ] = dataclasses.field(default_factory=list)
    not_carried: typing.List[typing.Tuple[Part, src_models.MasterPart]] = dataclasses.field(default_factory=list)
    unresolved: typing.List[Part] = dataclasses.field(default_factory=list)
    ambiguous: typing.List[Part] = dataclasses.field(default_factory=list)
    collisions: typing.List[Part] = dataclasses.field(default_factory=list)
    gone: typing.List[typing.Tuple[Part, src_models.MasterPart]] = dataclasses.field(default_factory=list)
    already_imported: int = 0
    problems: typing.List[str] = dataclasses.field(default_factory=list)


def plan_brand_import(
    company_id: int,
    old_brand_name: str,
    our_brand_id: int,
    old_repo: str = DEFAULT_OLD_REPO,
    old_vendor_name: typing.Optional[str] = None,
    live_index: typing.Optional[typing.Dict[str, str]] = None,
    provider_kinds: typing.Optional[typing.List[str]] = None,
    live_client=None,
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

    if live_index is None and live_client is not None:
        live_index = build_live_index(live_client, old_parts)

    kind = OLD_PROVIDER_TO_KIND.get(old_brand["provider"])
    our_brand = src_models.Brands.objects.filter(id=our_brand_id).first()
    if not our_brand:
        raise CommandError("No brand with id {}.".format(our_brand_id))
    if kind is None:
        # SDC. There is no matching distributor, so the caller must say which ones (first = primary).
        if not provider_kinds:
            raise CommandError(
                "Old vendor {} has no equivalent distributor here -- pass --provider (first is primary).".format(
                    old_brand["provider_name"]
                )
            )
        providers = []
        for kind_name in provider_kinds:
            found = src_models.Providers.objects.filter(kind_name=kind_name).first()
            if not found:
                raise CommandError("No provider named {!r}.".format(kind_name))
            providers.append(found)
    else:
        if provider_kinds:
            raise CommandError(
                "Several vendors for one brand are only for brands that came from SDC; {!r} came from {}.".format(
                    old_brand["name"], old_brand["provider_name"]
                )
            )
        providers = [src_models.Providers.objects.get(kind=kind)]
    vendors = []
    for prov in providers:
        v = src_models.ShopMonkeySyncVendor.objects.filter(company_id=company_id, provider=prov).first()
        if not v or not v.shopmonkey_external_id:
            raise CommandError("No linked vendor for {} -- run import_shopmonkey_vendors first.".format(prov.kind_name))
        vendors.append(v)
    provider, vendor = providers[0], vendors[0]

    existing = src_models.ShopMonkeySyncBrand.objects.filter(company_id=company_id, brand=our_brand).first()
    plan = BrandImportPlan(
        company_id=company_id,
        old_brand=dict(old_brand, parts=old_parts),
        our_brand=our_brand,
        provider=provider,
        vendor=vendor,
        vendors=vendors,
        existing_sync_brand=existing,
    )

    # An SDC brand's old vendor id is SDC's, which no vendor of ours carries, so there is nothing to compare.
    if kind is not None and vendor.shopmonkey_external_id != old_brand["sm_vendor_id"]:
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

    # master part -> the first vendor, in priority order, that carries it. Indexed path only:
    # provider_parts by (master_part_id, provider_id).
    carriers: typing.Dict[int, typing.Set[int]] = collections.defaultdict(set)
    provider_ids = [v.provider_id for v in vendors]
    ids = [mp.id for mp in master_parts]
    for i in range(0, len(ids), _CHUNK):
        for mp_id, prov_id in src_models.ProviderPart.objects.filter(
            master_part_id__in=ids[i : i + _CHUNK], provider_id__in=provider_ids
        ).values_list("master_part_id", "provider_id"):
            carriers[mp_id].add(prov_id)
    carried_by: typing.Dict[int, src_models.ShopMonkeySyncVendor] = {}
    for mp_id, provs in carriers.items():
        carried_by[mp_id] = next(v for v in vendors if v.provider_id in provs)

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
        # The old tool's number and sku, against our part number and sku, in every pairing. A distributor's own
        # SKU usually carries a brand prefix ("BFG99506") that its part number lacks ("99506"), and which of our
        # fields holds which form varies by distributor -- so matching number-to-number and sku-to-sku alone left
        # parts unlinked in ShopMonkey while the sync created a second copy of each. First pairing with any hit wins;
        # more than one hit within it is ambiguous and never guessed.
        candidates = []
        for index, key in (
            (by_number, part["number"]),
            (by_sku, part["number"]),
            (by_number, part["sku"]),
            (by_sku, part["sku"]),
        ):
            candidates = index.get(_norm(key), [])
            if candidates:
                break
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
            # last tracked it. It cannot be updated, so it is not linked. And because the shop asked for such
            # parts to stay deleted, it is recorded as SKIPPED (see apply_brand_import): without that row the
            # sync would see a carried master part with no sync part and create it again.
            plan.gone.append((part, mp))
        elif mp.id in carried_by:
            plan.linkable.append((part, mp, carried_by[mp.id]))
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
            # configure_sync_brand is the ONLY function that writes a brand's vendor set; the importer uses it
            # too, rather than a second way to create one, so the brand it writes is the brand the UI would.
            sync_brand = shopmonkey_sync_services.configure_sync_brand(
                company_id=plan.company_id,
                brand_id=plan.our_brand.id,
                provider_ids=[v.provider_id for v in plan.vendors],
                primary_provider_id=plan.vendors[0].provider_id,
                enabled=False,
            )
            sync_brand.shopmonkey_external_id = old_brand["sm_brand_id"]
            sync_brand.status = src_enums.ShopMonkeySyncStatus.ACTIVE.value
            sync_brand.status_name = src_enums.ShopMonkeySyncStatus.ACTIVE.name
            sync_brand.save(update_fields=["shopmonkey_external_id", "status", "status_name", "updated_at"])
            created_brand = True
        elif not sync_brand.shopmonkey_external_id:
            sync_brand.shopmonkey_external_id = old_brand["sm_brand_id"]
            sync_brand.status = src_enums.ShopMonkeySyncStatus.ACTIVE.value
            sync_brand.status_name = src_enums.ShopMonkeySyncStatus.ACTIVE.name
            sync_brand.save(update_fields=["shopmonkey_external_id", "status", "status_name", "updated_at"])

        rows = []
        for part, mp, sourced_vendor in plan.linkable:
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
                    sourced_from_vendor=sourced_vendor,
                    status=status.value,
                    status_name=status.name,
                )
            )
        src_models.ShopMonkeySyncPart.objects.bulk_create(rows, batch_size=_CHUNK)

        # Parts deleted in ShopMonkey after the old tool last tracked them, which the shop asked to keep out.
        # Recorded as SKIPPED so the sync never re-creates, updates or marks them; the dead id and the reason
        # stay on the row so the Parts table shows why it is there.
        skipped = []
        for part, mp in plan.gone:
            skipped.append(
                src_models.ShopMonkeySyncPart(
                    sync_brand=sync_brand,
                    master_part=mp,
                    shopmonkey_external_id=part["external_id"],
                    last_pushed_name=part["name"],
                    last_pushed_sku=part["sku"],
                    last_pushed_retail_cost_cents=part["retail_cost_in_cents"],
                    last_pushed_wholesale_cost_cents=part["wholesale_cost_in_cents"],
                    last_pushed_bin_location=old_brand["vendor_bin_name"],
                    sourced_from_vendor=None,
                    status=src_enums.ShopMonkeySyncPartStatus.SKIPPED.value,
                    status_name=src_enums.ShopMonkeySyncPartStatus.SKIPPED.name,
                    last_error=(
                        "Deleted in ShopMonkey (a GET returns 404) after the old tool last tracked it. "
                        "Kept out on request, so the sync will not re-create it."
                    ),
                )
            )
        src_models.ShopMonkeySyncPart.objects.bulk_create(skipped, batch_size=_CHUNK)

    logger.info(
        "{} Imported brand {!r} for company_id={}: sync_brand_id={} created_brand={} parts={} skipped={}".format(
            _LOG_PREFIX, old_brand["name"], plan.company_id, sync_brand.id, created_brand, len(rows), len(skipped)
        )
    )
    return {
        "sync_brand_id": sync_brand.id,
        "created_brand": int(created_brand),
        "parts_created": len(rows),
        "parts_skipped": len(skipped),
    }


def _status_from_live_name(name: typing.Optional[str]) -> src_enums.ShopMonkeySyncPartStatus:
    name = name or ""
    if name.startswith("[ERROR UPDATING PART]"):
        return src_enums.ShopMonkeySyncPartStatus.ERROR
    if name.startswith("[OBSOLETE]"):
        return src_enums.ShopMonkeySyncPartStatus.INACTIVE
    return src_enums.ShopMonkeySyncPartStatus.ACTIVE


def _sku_key(sku: typing.Optional[str]) -> str:
    return re.sub(r"[^A-Z0-9]", "", (sku or "").upper())


def adopt_untracked_live_parts(
    sync_brand: src_models.ShopMonkeySyncBrand,
    live_records: typing.List[typing.Dict[str, typing.Any]],
    dry_run: bool = False,
) -> typing.Dict[str, int]:
    """
    Link, instead of duplicate, the parts ShopMonkey already holds that no sync part points at.

    The old tool's database does not track every part in the account (about 6,400 were never recorded, and it keeps
    adding more), so a master part we would create may already exist upstream with the same SKU. Creating it would
    give the shop a second one. Where a part with that SKU already exists under THIS brand's ShopMonkey brand and
    nothing else is linked to it, we record the link and what it holds today, and from then on the sync updates it.

    Only the same ShopMonkey brand counts: the same SKU under a different brand is reported but never adopted, and a
    SKU that appears twice in the brand is ambiguous and left for a person. ``live_records`` is the account listing
    ({id, sku, brandId, name, retailCostCents, wholesaleCostCents, binLocation}); the caller owns how fresh it is.
    """
    vendors = [sync_brand.vendor] + [
        link.vendor
        for link in src_models.ShopMonkeySyncBrandVendor.objects.filter(sync_brand=sync_brand)
        .select_related("vendor")
        .order_by("priority")
    ]
    provider_ids = [v.provider_id for v in vendors]
    master_parts = {mp.id: mp for mp in src_models.MasterPart.objects.filter(brand_id=sync_brand.brand_id)}
    ids = list(master_parts)
    carriers: typing.Dict[int, typing.Set[int]] = collections.defaultdict(set)
    for i in range(0, len(ids), _CHUNK):
        for mp_id, prov_id in src_models.ProviderPart.objects.filter(
            master_part_id__in=ids[i : i + _CHUNK], provider_id__in=provider_ids
        ).values_list("master_part_id", "provider_id"):
            carriers[mp_id].add(prov_id)
    linked = set(
        src_models.ShopMonkeySyncPart.objects.filter(sync_brand=sync_brand).values_list("master_part_id", flat=True)
    )
    tracked_ids = set(
        src_models.ShopMonkeySyncPart.objects.exclude(shopmonkey_external_id=None).values_list(
            "shopmonkey_external_id", flat=True
        )
    )

    live_by_sku: typing.Dict[str, typing.List[typing.Dict[str, typing.Any]]] = collections.defaultdict(list)
    for rec in live_records:
        if rec.get("brandId") == sync_brand.shopmonkey_external_id and _sku_key(rec.get("sku")):
            live_by_sku[_sku_key(rec["sku"])].append(rec)

    rows, ambiguous, already_tracked = [], 0, 0
    for mp_id, provs in carriers.items():
        if mp_id in linked:
            continue
        master_part = master_parts[mp_id]
        recs = live_by_sku.get(_sku_key(master_part.sku or master_part.part_number), [])
        if not recs:
            continue
        if len(recs) > 1:
            ambiguous += 1
            continue
        rec = recs[0]
        if rec["id"] in tracked_ids:
            already_tracked += 1
            continue
        tracked_ids.add(rec["id"])
        status = _status_from_live_name(rec.get("name"))
        rows.append(
            src_models.ShopMonkeySyncPart(
                sync_brand=sync_brand,
                master_part=master_part,
                shopmonkey_external_id=rec["id"],
                last_pushed_name=rec.get("name"),
                last_pushed_sku=rec.get("sku"),
                last_pushed_retail_cost_cents=rec.get("retailCostCents"),
                last_pushed_wholesale_cost_cents=rec.get("wholesaleCostCents"),
                last_pushed_bin_location=rec.get("binLocation"),
                sourced_from_vendor=next(v for v in vendors if v.provider_id in provs),
                status=status.value,
                status_name=status.name,
            )
        )
    if not dry_run:
        with transaction.atomic():
            src_models.ShopMonkeySyncPart.objects.bulk_create(rows, batch_size=_CHUNK)
    logger.info(
        "{} Adopted {} live part(s) for sync_brand_id={} ({} ambiguous, {} already linked elsewhere).".format(
            _LOG_PREFIX, len(rows), sync_brand.id, ambiguous, already_tracked
        )
    )
    return {"adopted": len(rows), "ambiguous": ambiguous, "already_tracked": already_tracked}
