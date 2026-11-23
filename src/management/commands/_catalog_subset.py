"""
Table plan shared by export_catalog_subset and import_catalog_subset (staging catalog subset).

The plan is derived from the models, not a hand-kept list, so a new model is classified automatically:

- EXCLUDED: customer data. Every table with a foreign key (direct or transitive, nullable or not) to ``company``
  or ``auth_user``, plus the explicit EXCLUDED_TABLES (CRM leads, emails, audits, EDI, job bookkeeping).
  Never selected, never granted to the export role.
- WHOLE: small reference tables (estimated rows <= whole_max, or in ALWAYS_WHOLE) whose parents are all WHOLE.
- SUBSET: everything else, one of
    root      provider_parts: ``per_provider`` rows per provider, spread across brands, in-stock first
    pulled    master_parts: exactly the rows the chosen provider_parts reference
    dependent a table with a NOT NULL foreign key to a SUBSET table: the rows whose parents were chosen
              (fitments, part data, inventory, kit components, specs, raw-feed fitment/inventory tables)
    sampled   any other big table (raw distributor feeds such as premier_parts): ``per_provider`` rows, spread
              across brand_id when the table has one. NOT matched row-for-row to the chosen provider_parts: the
              provider_external_id format differs per distributor, and the app reads the normalised tables.
  A NULLABLE foreign key from a SUBSET table to a SUBSET parent is set to NULL when the parent row was not
  chosen, so the subset always loads with every constraint on.
"""

import dataclasses

from django.apps import apps

ROOT = "provider_parts"
PULLED = {"master_parts": (ROOT, "master_part_id")}
EXCLUDED_ROOTS = {"company", "auth_user"}
EXCLUDED_TABLES = {
    "lead",
    "lead_email",
    "realtruck_leads",
    "realtruck_lead_email",
    "leer_leads",
    "instantly_reply",
    "apollo_phone_reveal",
    "notification_email_log",
    "support_tickets",
    "edi_control_numbers",
    "edi_documents",
    "scheduled_task_execution",
    "api_rate_buckets",
    "integration_pricing_sync_job",
    "connection_attempts",
    "part_request_audit",
    "live_inventory_house_account_audit",
}
# Copied whole even when large: vehicle search and fitment display need the full vehicle tables.
ALWAYS_WHOLE = {
    "vcdb_vehicles",
    "us_zip_code",
    "wheelpros_vehicles",
    "wheelpros_vehicle_axles",
    "thoroughbred_vehicles",
}
DEFAULT_WHOLE_MAX = 250_000


@dataclasses.dataclass(frozen=True)
class ForeignKey:
    column: str
    parent: str
    nullable: bool


@dataclasses.dataclass
class Table:
    name: str
    pk: str
    columns: list
    fks: list
    has_brand_id: bool


@dataclasses.dataclass
class Plan:
    order: list  # every exported table, parents before children
    mode: dict  # table -> "whole" | "root" | "pulled" | "dependent" | "sampled"
    excluded: list


def catalog_tables() -> dict:
    """Every concrete table of the src app, with its columns and foreign keys, from the model metadata."""
    tables = {}
    for model in apps.get_app_config("src").get_models():
        meta = model._meta
        if meta.proxy or not meta.managed:
            continue
        fks = [
            ForeignKey(f.column, f.related_model._meta.db_table, bool(f.null))
            for f in meta.concrete_fields
            if f.is_relation and (f.many_to_one or f.one_to_one)
        ]
        columns = [f.column for f in meta.concrete_fields]
        tables[meta.db_table] = Table(meta.db_table, meta.pk.column, columns, fks, "brand_id" in columns)
    return tables


def excluded_tables(tables: dict) -> set:
    excluded = set(EXCLUDED_ROOTS) | (EXCLUDED_TABLES & set(tables))
    changed = True
    while changed:
        changed = False
        for t in tables.values():
            if t.name not in excluded and any(fk.parent in excluded for fk in t.fks):
                excluded.add(t.name)
                changed = True
    return excluded & set(tables)


def exportable_tables(tables: dict) -> list:
    """Tables the read-only export role needs SELECT on (everything not excluded)."""
    excluded = excluded_tables(tables)
    return sorted(name for name in tables if name not in excluded)


def build_plan(tables: dict, row_estimates: dict, whole_max: int = DEFAULT_WHOLE_MAX) -> Plan:
    excluded = excluded_tables(tables)
    names = [n for n in tables if n not in excluded]
    for parent in {fk.parent for n in names for fk in tables[n].fks}:
        if parent not in tables and parent not in excluded:
            raise ValueError(f"foreign key to unknown table {parent}")

    forced_subset = {ROOT, *PULLED}
    whole = {n for n in names if n not in forced_subset and (n in ALWAYS_WHOLE or row_estimates.get(n, 0) <= whole_max)}
    changed = True
    while changed:  # a small table pointing at a subset table must be filtered too
        changed = False
        for n in list(whole):
            if any(fk.parent not in whole and fk.parent != n for fk in tables[n].fks):
                whole.discard(n)
                changed = True

    mode = {}
    for n in names:
        if n in whole:
            mode[n] = "whole"
        elif n == ROOT:
            mode[n] = "root"
        elif n in PULLED:
            mode[n] = "pulled"
        elif any(not fk.nullable and fk.parent != n for fk in tables[n].fks if fk.parent not in whole):
            mode[n] = "dependent"
        else:
            mode[n] = "sampled"

    # Parents before children, except that a pulled table comes right after the root that drives it.
    deps = {n: {fk.parent for fk in tables[n].fks if fk.parent != n} for n in names}
    for pulled, (root, _column) in PULLED.items():
        deps[root].discard(pulled)
        deps[pulled] = deps[pulled] | {root}
    order, done = [], set()
    while len(order) < len(names):
        ready = sorted(n for n in names if n not in done and deps[n] <= done)
        if not ready:
            raise ValueError(f"foreign key cycle among {sorted(set(names) - done)}")
        for n in ready:
            order.append(n)
            done.add(n)
    return Plan(order=order, mode=mode, excluded=sorted(excluded))


def load_order(plan: Plan, tables: dict) -> list:
    """Order that satisfies every foreign key on import (a pulled table's rows are referenced by its root)."""
    names = plan.order
    deps = {n: {fk.parent for fk in tables[n].fks if fk.parent != n and fk.parent in names} for n in names}
    order, done = [], set()
    while len(order) < len(names):
        ready = [n for n in names if n not in done and deps[n] <= done]
        if not ready:
            raise ValueError(f"foreign key cycle among {sorted(set(names) - done)}")
        for n in ready:
            order.append(n)
            done.add(n)
    return order
