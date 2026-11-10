"""
Port the old standalone ShopMonkey tool's vendors (~/Documents/code/shopmonkey, ``inventory_vendor``)
into ShopMonkeySyncVendor for one company, and create a vendor row for every other distributor the
company is connected to.

    manage.py import_shopmonkey_vendors --company-id 16            # dry run (the default)
    manage.py import_shopmonkey_vendors --company-id 16 --apply    # write

This writes DATA ONLY. It never creates, updates or deletes anything in ShopMonkey -- the only
calls it makes there are GETs -- and it contains no sync logic: the one sync path in
src.integrations.services.shopmonkey_sync stays the only thing that pushes. What it carries across
is the part of the old tool's state that cannot be re-derived, namely the ShopMonkey vendor ids it
created and the ``binLocation`` code each distributor is marked with on every part. Without them a
first sync would create a second set of vendors beside the ones already in the account and mark
parts with a different code than the ~94K already there carry.

The old tool's vendors are a hardcoded enum of six, not a table of our providers, so the mapping is
explicit below and SDC deliberately has no target: SDC is a catalog, not a distributor of ours.

For every other connected provider there is no history to carry, so the row is created with the
default bin code and, where the account already has a vendor of the same name, that vendor's id is
adopted instead of leaving the first sync to create a duplicate beside it. Ambiguous names (two
vendors called MEYER, say) are never adopted by guesswork; they are reported.

Existing rows are never overwritten. A value that differs from what would be written is reported as
a conflict and left alone.
"""
import logging
import os
import re
import typing

import psycopg2
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from dotenv import dotenv_values

from src import enums as src_enums
from src import models as src_models
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[IMPORT-SHOPMONKEY-VENDORS]"

# Old tool's InventoryVendor enum value -> our Providers.kind. SDC (4) has no target on purpose.
_OLD_PROVIDER_TO_KIND: typing.Dict[int, typing.Optional[int]] = {
    1: src_enums.BrandProviderKind.TURN_14.value,
    2: src_enums.BrandProviderKind.KEYSTONE.value,
    3: src_enums.BrandProviderKind.WHEELPROS.value,  # WHEEL_PROS there, WHEELPROS here
    4: None,  # SDC
    5: src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
    6: src_enums.BrandProviderKind.MEYER.value,
}

_DEFAULT_OLD_REPO = "~/Documents/code/shopmonkey"


def _norm(value: typing.Optional[str]) -> str:
    return re.sub(r"[^A-Z0-9]", "", (value or "").upper())


def _read_old_vendors(old_repo: str) -> typing.List[typing.Dict[str, typing.Any]]:
    """Read-only. Credentials come from the old repo's own .env so nothing is copied or typed."""
    env_path = os.path.join(os.path.expanduser(old_repo), ".env")
    if not os.path.exists(env_path):
        raise CommandError("No .env at {} -- pass --old-repo.".format(env_path))
    env = dotenv_values(env_path)
    missing = [
        k
        for k in ("DATABASE_HOST", "DATABASE_PORT", "DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD")
        if not env.get(k)
    ]
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
    try:
        connection.set_session(readonly=True, autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT provider, provider_name, name, vendor_bin_name, sink_external_id, status_name "
                "FROM inventory_vendor ORDER BY provider"
            )
            columns = [c[0] for c in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        connection.close()


def _live_vendors_by_name(company_id: int) -> typing.Tuple[typing.Dict[str, typing.List[str]], typing.Set[str]]:
    """GET only. Returns (normalised name -> [vendor ids], all vendor ids)."""
    client = shopmonkey_sync_services._client_for_company(company_id)
    by_name: typing.Dict[str, typing.List[str]] = {}
    ids: typing.Set[str] = set()
    for vendor in client.get_vendors():
        ids.add(vendor["id"])
        by_name.setdefault(_norm(vendor.get("name")), []).append(vendor["id"])
    return by_name, ids


def _plan(
    company_id: int,
    old_vendors: typing.List[typing.Dict[str, typing.Any]],
    live_by_name: typing.Dict[str, typing.List[str]],
    live_ids: typing.Set[str],
) -> typing.Tuple[typing.List[typing.Dict[str, typing.Any]], typing.List[typing.Dict[str, typing.Any]]]:
    """Returns (rows, skipped old vendors)."""
    old_by_kind = {}
    skipped = []
    for old in old_vendors:
        kind = _OLD_PROVIDER_TO_KIND.get(old["provider"], "unknown")
        if kind is None or kind == "unknown":
            skipped.append(old)
            continue
        old_by_kind[kind] = old

    connected_ids = shopmonkey_sync_services.company_connected_provider_ids(company_id)
    providers = {p.kind: p for p in src_models.Providers.objects.filter(id__in=connected_ids)}
    # A mapped provider is ported even if it were not currently connected: its vendor id is history.
    for kind in old_by_kind:
        if kind not in providers:
            provider = src_models.Providers.objects.filter(kind=kind).first()
            if provider:
                providers[kind] = provider

    existing = {v.provider_id: v for v in src_models.ShopMonkeySyncVendor.objects.filter(company_id=company_id)}

    rows = []
    for kind, provider in sorted(providers.items(), key=lambda kv: kv[1].id):
        old = old_by_kind.get(kind)
        notes: typing.List[str] = []

        if old:
            source = "OLD:{}".format(old["provider_name"])
            sm_id = old["sink_external_id"]
            bin_name = old["vendor_bin_name"]
            if sm_id and sm_id not in live_ids:
                notes.append("old vendor id not found in the live account")
            expected_bin = shopmonkey_sync_services._bin_name_for_provider(provider)
            if bin_name != expected_bin:
                notes.append("old bin {!r} != code in shopmonkey_sync {!r}".format(bin_name, expected_bin))
        else:
            source = "new"
            bin_name = shopmonkey_sync_services._bin_name_for_provider(provider)
            matches = list(
                dict.fromkeys(
                    live_by_name.get(_norm(provider.kind_name), []) + live_by_name.get(_norm(provider.name), [])
                )
            )
            if len(matches) == 1:
                sm_id = matches[0]
                source = "new (adopts live vendor)"
            else:
                sm_id = None
                if len(matches) > 1:
                    notes.append("{} live vendors share this name; not adopted".format(len(matches)))

        status = src_enums.ShopMonkeySyncStatus.ACTIVE if sm_id else src_enums.ShopMonkeySyncStatus.INACTIVE

        row = existing.get(provider.id)
        changes: typing.Optional[typing.Dict[str, typing.Any]] = None
        if row is None:
            action = "create"
        else:
            changes = {}
            if sm_id and not row.shopmonkey_external_id:
                changes["shopmonkey_external_id"] = sm_id
            elif sm_id and row.shopmonkey_external_id != sm_id:
                notes.append("CONFLICT: existing vendor id {} != {}".format(row.shopmonkey_external_id, sm_id))
            if bin_name and not row.bin_name:
                changes["bin_name"] = bin_name
            elif bin_name and row.bin_name != bin_name:
                notes.append("CONFLICT: existing bin {!r} != {!r}".format(row.bin_name, bin_name))
            if changes and "shopmonkey_external_id" in changes:
                changes["status"] = status.value
            action = "update" if changes else "unchanged"

        rows.append(
            {
                "provider": provider,
                "source": source,
                "sm_id": sm_id,
                "bin_name": bin_name,
                "status": status,
                "action": action,
                "changes": changes if action == "update" else None,
                "existing": row,
                "notes": notes,
            }
        )

    bins: typing.Dict[str, typing.List[str]] = {}
    for r in rows:
        bins.setdefault(r["bin_name"], []).append(r["provider"].kind_name)
    for r in rows:
        clash = [k for k in bins[r["bin_name"]] if k != r["provider"].kind_name]
        if clash:
            r["notes"].append("bin {!r} also used by {}".format(r["bin_name"], ", ".join(clash)))

    return rows, skipped


class Command(BaseCommand):
    help = "Port the old ShopMonkey tool's vendors into ShopMonkeySyncVendor for one company (dry run by default)."

    def add_arguments(self, parser):
        parser.add_argument("--company-id", type=int, required=True)
        parser.add_argument("--old-repo", default=_DEFAULT_OLD_REPO, help="Path to the old standalone tool.")
        parser.add_argument("--apply", action="store_true", help="Write. Without it nothing is written.")

    def handle(self, *args, **options):
        company_id = options["company_id"]
        company = src_models.Company.objects.filter(id=company_id).first()
        if not company:
            raise CommandError("No company with id {}.".format(company_id))
        self.stdout.write("Company {}: {} ({})".format(company.id, company.name, company.slug))

        old_vendors = _read_old_vendors(options["old_repo"])
        self.stdout.write("Old inventory_vendor rows: {}".format(len(old_vendors)))
        live_by_name, live_ids = _live_vendors_by_name(company_id)
        self.stdout.write("Vendors in the live ShopMonkey account: {}\n".format(len(live_ids)))

        rows, skipped = _plan(company_id, old_vendors, live_by_name, live_ids)

        self.stdout.write(
            "{:<4} {:<26} {:<24} {:<5} {:<38} {:<10} {}".format(
                "id", "provider", "source", "bin", "shopmonkey vendor id", "action", "notes"
            )
        )
        for r in rows:
            self.stdout.write(
                "{:<4} {:<26} {:<24} {:<5} {:<38} {:<10} {}".format(
                    r["provider"].id,
                    r["provider"].kind_name,
                    r["source"],
                    r["bin_name"] or "-",
                    r["sm_id"] or "(created on first sync)",
                    r["action"],
                    "; ".join(r["notes"]),
                )
            )
        for old in skipped:
            self.stdout.write(
                "skipped: old {} (provider={}) -- no equivalent distributor here".format(
                    old["provider_name"], old["provider"]
                )
            )

        counts = {a: sum(1 for r in rows if r["action"] == a) for a in ("create", "update", "unchanged")}
        conflicts = sum(1 for r in rows if any(n.startswith("CONFLICT") for n in r["notes"]))
        self.stdout.write(
            "\ncreate={create} update={update} unchanged={unchanged} conflicts={c}".format(c=conflicts, **counts)
        )

        if not options["apply"]:
            self.stdout.write(self.style.WARNING("DRY RUN -- nothing written. Re-run with --apply."))
            return

        with transaction.atomic():
            for r in rows:
                if r["action"] == "create":
                    src_models.ShopMonkeySyncVendor.objects.create(
                        company_id=company_id,
                        provider=r["provider"],
                        shopmonkey_external_id=r["sm_id"],
                        bin_name=r["bin_name"],
                        status=r["status"].value,
                        status_name=r["status"].name,
                    )
                elif r["action"] == "update":
                    for field, value in r["changes"].items():
                        setattr(r["existing"], field, value)
                    r["existing"].status_name = src_enums.ShopMonkeySyncStatus(r["existing"].status).name
                    r["existing"].save()
        logger.info("{} Applied for company_id={}: {}".format(_LOG_PREFIX, company_id, counts))
        self.stdout.write(self.style.SUCCESS("Applied."))
