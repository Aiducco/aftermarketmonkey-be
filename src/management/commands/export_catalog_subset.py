"""
Export a scrubbed catalog subset (no customer data) from a source database into gzipped CSV COPY files, for
staging. See workspace docs/plans/staging-environment.md (task S-3) and _catalog_subset.py for what is selected.

Read-only by construction:
  - it uses its own psycopg2 connection to --source-dsn (or $CATALOG_SOURCE_DSN), never Django's default database,
    so the Django settings only provide model metadata (conf.settings_test is fine locally);
  - the session is READ ONLY / REPEATABLE READ (one consistent snapshot);
  - it refuses to run unless the role is a non-superuser that can write to no table in the schema and cannot read
    ``company`` or ``auth_user`` (create it with --print-grants).

Usage:
    # 1. SQL for the read-only role (run by a human on the source server):
    python manage.py export_catalog_subset --print-grants --settings=conf.settings_test
    # 2. Export (off-peak; the provider_parts sampling scans each provider's rows once):
    CATALOG_SOURCE_DSN='host=... dbname=... user=staging_export password=... sslmode=prefer' \\
        python manage.py export_catalog_subset --out /data/catalog-subset --settings=conf.settings_test
"""

import datetime
import gzip
import json
import os
import shutil

import psycopg2
import psycopg2.extensions
from django.core.management.base import BaseCommand, CommandError

from src.management.commands import _catalog_subset as subset

CHUNK = 20_000


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _chunks(values: list, size: int = CHUNK):
    for i in range(0, len(values), size):
        yield values[i : i + size]


class Command(BaseCommand):
    help = "Export a scrubbed catalog subset (no customer data) from a read-only source DB into COPY files."

    def add_arguments(self, parser):
        parser.add_argument("--out", help="Output directory (must be empty or not exist).")
        parser.add_argument("--source-dsn", default=os.environ.get("CATALOG_SOURCE_DSN", ""))
        parser.add_argument("--per-provider", type=int, default=5000, help="provider_parts per provider (default 5000)")
        parser.add_argument("--whole-max", type=int, default=subset.DEFAULT_WHOLE_MAX)
        parser.add_argument("--min-free-gb", type=float, default=5.0)
        parser.add_argument("--print-grants", action="store_true", help="Print SQL for the read-only role and exit.")
        parser.add_argument("--role", default="staging_export", help="Role name used by --print-grants.")

    def handle(self, *args, **opts):
        tables = subset.catalog_tables()
        if opts["print_grants"]:
            self._print_grants(tables, opts["role"])
            return
        if not opts["out"]:
            raise CommandError("--out is required")
        if not opts["source_dsn"]:
            raise CommandError("--source-dsn or $CATALOG_SOURCE_DSN is required")
        out = os.path.abspath(opts["out"])
        if os.path.exists(out) and os.listdir(out):
            raise CommandError(f"{out} is not empty")
        os.makedirs(out, exist_ok=True)
        free_gb = shutil.disk_usage(out).free / 1e9
        if free_gb < opts["min_free_gb"]:
            raise CommandError(f"only {free_gb:.1f} GB free under {out} (need {opts['min_free_gb']})")

        conn = psycopg2.connect(opts["source_dsn"], application_name="export_catalog_subset")
        conn.set_session(readonly=True, isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
        try:
            cur = conn.cursor()
            source = {k: v for k, v in conn.get_dsn_parameters().items() if k in ("host", "port", "dbname", "user")}
            self.stdout.write(f"Source: {source}")
            self._assert_read_only(cur)
            self._check_columns(cur, tables)
            estimates = self._row_estimates(cur, tables)
            plan = subset.build_plan(tables, estimates, opts["whole_max"])
            ids = self._select_ids(cur, tables, plan, opts["per_provider"])
            manifest = self._write(cur, tables, plan, ids, out)
        finally:
            conn.rollback()
            conn.close()

        manifest.update(
            {
                "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "source": source,
                "per_provider": opts["per_provider"],
                "whole_max": opts["whole_max"],
                "excluded": plan.excluded,
            }
        )
        with open(os.path.join(out, "manifest.json"), "w") as fh:
            json.dump(manifest, fh, indent=2)
        total = sum(t["rows"] for t in manifest["tables"])
        size = sum(os.path.getsize(os.path.join(out, t["file"])) for t in manifest["tables"]) / 1e6
        self.stdout.write(
            self.style.SUCCESS(
                f"Exported {len(manifest['tables'])} tables, {total} rows, {size:.1f} MB to {out}; "
                f"{len(plan.excluded)} customer tables excluded."
            )
        )

    # ---- safety -------------------------------------------------------------------------------------------------

    def _assert_read_only(self, cur):
        cur.execute("SHOW transaction_read_only")
        if cur.fetchone()[0] != "on":
            raise CommandError("session is not read-only")
        cur.execute(
            "SELECT rolsuper, rolcreaterole, rolcreatedb, rolbypassrls FROM pg_roles WHERE rolname = current_user"
        )
        row = cur.fetchone()
        if row is None or any(row):
            raise CommandError(
                "refusing: the export role must not be a superuser or have CREATEROLE/CREATEDB/BYPASSRLS"
            )
        cur.execute(
            """
            SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
              AND (has_table_privilege(c.oid, 'INSERT') OR has_table_privilege(c.oid, 'UPDATE')
                   OR has_table_privilege(c.oid, 'DELETE') OR has_table_privilege(c.oid, 'TRUNCATE'))
            ORDER BY 1 LIMIT 5
            """
        )
        writable = [r[0] for r in cur.fetchall()]
        if writable:
            raise CommandError(f"refusing: the export role can write to {writable}; use a SELECT-only role")
        cur.execute(
            """
            SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname IN ('company', 'auth_user') AND has_table_privilege(c.oid, 'SELECT')
            """
        )
        readable = [r[0] for r in cur.fetchall()]
        if readable:
            raise CommandError(
                f"refusing: the export role can read customer tables {readable}; grant catalog tables only"
            )

    def _check_columns(self, cur, tables):
        cur.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'")
        present = {}
        for table, column in cur.fetchall():
            present.setdefault(table, set()).add(column)
        problems = []
        for name in subset.exportable_tables(tables):
            if name not in present:
                problems.append(f"{name}: table missing or not readable")
                continue
            missing = [c for c in tables[name].columns if c not in present[name]]
            if missing:
                problems.append(f"{name}: columns missing on source {missing}")
        if problems:
            raise CommandError(
                "source schema does not match the models (missing grant or unapplied migration?):\n  "
                + "\n  ".join(problems)
            )

    def _row_estimates(self, cur, tables):
        cur.execute(
            """
            SELECT c.relname, c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
            """
        )
        estimates = dict(cur.fetchall())
        for name in subset.exportable_tables(tables):
            if estimates.get(name, -1) < 0:  # never analysed: count exactly
                cur.execute(f"SELECT count(*) FROM {_q(name)}")
                estimates[name] = cur.fetchone()[0]
        return estimates

    # ---- selection ----------------------------------------------------------------------------------------------

    def _select_ids(self, cur, tables, plan, per_provider):
        ids = {}
        for name in plan.order:
            mode = plan.mode[name]
            t = tables[name]
            if mode == "whole":
                continue
            if mode == "root":
                ids[name] = self._root_ids(cur, per_provider)
            elif mode == "pulled":
                root, column = subset.PULLED[name]
                chosen = set()
                for chunk in _chunks(ids[root]):
                    cur.execute(f"SELECT DISTINCT {_q(column)} FROM {_q(root)} WHERE id = ANY(%s)", [chunk])
                    chosen.update(r[0] for r in cur.fetchall() if r[0] is not None)
                ids[name] = sorted(chosen)
            elif mode == "dependent":
                filters = [fk for fk in t.fks if not fk.nullable and fk.parent in ids and fk.parent != name]
                ids[name] = self._dependent_ids(cur, t, filters, ids)
            else:  # sampled
                order = f"{_q(t.pk)} DESC"
                if t.has_brand_id:
                    sql = (
                        f"SELECT pk FROM (SELECT {_q(t.pk)} AS pk, ROW_NUMBER() OVER (PARTITION BY brand_id "
                        f"ORDER BY {order}) AS rn FROM {_q(name)}) s ORDER BY rn, pk DESC LIMIT %s"
                    )
                else:
                    sql = f"SELECT {_q(t.pk)} FROM {_q(name)} ORDER BY {order} LIMIT %s"
                cur.execute(sql, [per_provider])
                ids[name] = [r[0] for r in cur.fetchall()]
            self.stdout.write(f"  {name:45} {mode:9} {len(ids[name]):>9} rows")
        return ids

    def _root_ids(self, cur, per_provider):
        """provider_parts: per provider, spread across brands, in-stock and not discontinued first."""
        cur.execute("SELECT DISTINCT provider_id FROM provider_parts ORDER BY 1")
        provider_ids = [r[0] for r in cur.fetchall()]
        chosen = []
        for provider_id in provider_ids:
            cur.execute(
                """
                SELECT id FROM (
                    SELECT pp.id, ROW_NUMBER() OVER (
                        PARTITION BY mp.brand_id
                        ORDER BY (COALESCE(inv.warehouse_total_qty, 0) > 0) DESC, pp.is_discontinued, pp.id
                    ) AS rn
                    FROM provider_parts pp
                    JOIN master_parts mp ON mp.id = pp.master_part_id
                    LEFT JOIN provider_part_inventory inv ON inv.provider_part_id = pp.id
                    WHERE pp.provider_id = %s
                ) s ORDER BY rn, id LIMIT %s
                """,
                [provider_id, per_provider],
            )
            got = [r[0] for r in cur.fetchall()]
            chosen.extend(got)
            self.stdout.write(f"    provider {provider_id}: {len(got)} provider_parts")
        return sorted(chosen)

    def _dependent_ids(self, cur, t, filters, ids):
        first, rest = filters[0], filters[1:]
        where_rest = "".join(f" AND {_q(fk.column)} = ANY(%s)" for fk in rest)
        params_rest = [ids[fk.parent] for fk in rest]
        chosen = []
        for chunk in _chunks(ids[first.parent]):
            cur.execute(
                f"SELECT {_q(t.pk)} FROM {_q(t.name)} WHERE {_q(first.column)} = ANY(%s){where_rest}",
                [chunk, *params_rest],
            )
            chosen.extend(r[0] for r in cur.fetchall())
        return sorted(set(chosen))

    # ---- output -------------------------------------------------------------------------------------------------

    def _select_list(self, cur, t, ids, mode):
        """Columns, with nullable foreign keys to unchosen subset rows set to NULL."""
        parts = []
        for column in t.columns:
            fk = next((f for f in t.fks if f.column == column), None)
            if fk and fk.nullable and fk.parent in ids and mode != "whole":
                parts.append(
                    cur.mogrify(
                        f"CASE WHEN {_q(column)} = ANY(%s) THEN {_q(column)} END AS {_q(column)}", [ids[fk.parent]]
                    ).decode()
                )
            else:
                parts.append(_q(column))
        return ", ".join(parts)

    def _write(self, cur, tables, plan, ids, out):
        entries = []
        for n, name in enumerate(subset.load_order(plan, tables), start=1):
            t, mode = tables[name], plan.mode[name]
            filename = f"{n:03d}_{name}.csv.gz"
            select = self._select_list(cur, t, ids, mode)
            with gzip.open(os.path.join(out, filename), "wb") as fh:
                if mode == "whole":
                    cur.copy_expert(
                        f"COPY (SELECT {select} FROM {_q(name)}) TO STDOUT WITH (FORMAT csv, HEADER true)", fh
                    )
                    cur.execute(f"SELECT count(*) FROM {_q(name)}")
                    rows = cur.fetchone()[0]
                else:
                    rows = len(ids[name])
                    for i, chunk in enumerate(_chunks(ids[name]) if ids[name] else [[]]):
                        sql = cur.mogrify(
                            f"COPY (SELECT {select} FROM {_q(name)} WHERE {_q(t.pk)} = ANY(%s) ORDER BY {_q(t.pk)}) "
                            f"TO STDOUT WITH (FORMAT csv, HEADER {'true' if i == 0 else 'false'})",
                            [chunk],
                        ).decode()
                        cur.copy_expert(sql, fh)
            entries.append({"table": name, "mode": mode, "columns": t.columns, "rows": rows, "file": filename})
        return {"tables": entries}

    def _print_grants(self, tables, role):
        names = subset.exportable_tables(tables)
        grant_list = ",\n  ".join(_q(n) for n in names)
        self.stdout.write(
            f"""-- Read-only role for export_catalog_subset ({len(names)} catalog tables; customer tables excluded).
-- Run as the database owner on the source server, replacing <PASSWORD>. Re-run after new catalog models ship.
CREATE ROLE {role} LOGIN PASSWORD '<PASSWORD>' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
ALTER ROLE {role} SET default_transaction_read_only = on;
ALTER ROLE {role} CONNECTION LIMIT 2;
GRANT CONNECT ON DATABASE "<DBNAME>" TO {role};
GRANT USAGE ON SCHEMA public TO {role};
GRANT SELECT ON
  {grant_list}
TO {role};
"""
        )
