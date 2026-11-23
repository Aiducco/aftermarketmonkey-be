"""
Load a catalog subset written by export_catalog_subset into the STAGING database (task S-3).

Runs only with --settings=conf.settings_staging (Postgres, never the production host; that module refuses a
production DATABASE_HOST itself). The schema must already exist (`manage.py migrate`). All tables load in one
transaction in foreign-key order with every constraint on, then sequences are moved past the imported ids.

Usage (on the staging VM, inside the be container):
    python manage.py import_catalog_subset /data/catalog-subset --settings=conf.settings_staging
    python manage.py import_catalog_subset /data/catalog-subset --replace --settings=conf.settings_staging
--replace empties the catalog tables first with TRUNCATE ... CASCADE, which also empties staging rows that
reference them (test companies' pricing, carts, orders): run seed_staging again afterwards.
"""

import gzip
import json
import os
import time

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from src.management.commands import _catalog_subset as subset

ALLOWED_SETTINGS = {"conf.settings_staging"}


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


class Command(BaseCommand):
    help = "Load a catalog subset (export_catalog_subset output) into the staging database."

    def add_arguments(self, parser):
        parser.add_argument("directory")
        parser.add_argument("--replace", action="store_true", help="TRUNCATE ... CASCADE the catalog tables first.")

    def handle(self, *args, **opts):
        self._assert_staging()
        directory = os.path.abspath(opts["directory"])
        with open(os.path.join(directory, "manifest.json")) as fh:
            manifest = json.load(fh)
        entries = manifest["tables"]
        models = subset.catalog_tables()
        excluded = subset.excluded_tables(models)
        for entry in entries:
            name = entry["table"]
            if name in excluded:
                raise CommandError(f"manifest contains customer table {name}; refusing")
            if name not in models:
                raise CommandError(f"manifest table {name} has no model here (code older than the export?)")
            extra = set(entry["columns"]) - set(models[name].columns)
            if extra:
                raise CommandError(f"{name}: columns {sorted(extra)} are not in the models here")

        names = [e["table"] for e in entries]
        started = time.monotonic()
        with transaction.atomic(), connection.cursor() as cur:
            non_empty = [n for n in names if self._has_rows(cur, n)]
            if non_empty and not opts["replace"]:
                raise CommandError(f"target tables already have rows: {non_empty[:10]}... use --replace")
            if opts["replace"]:
                cur.execute(f"TRUNCATE {', '.join(_q(n) for n in names)} RESTART IDENTITY CASCADE")
                self.stdout.write(f"Truncated {len(names)} catalog tables (CASCADE)")
            for entry in entries:
                cols = ", ".join(_q(c) for c in entry["columns"])
                with gzip.open(os.path.join(directory, entry["file"]), "rb") as fh:
                    cur.copy_expert(f"COPY {_q(entry['table'])} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)", fh)
                self.stdout.write(f"  {entry['table']:45} {entry['mode']:9} {entry['rows']:>9} rows")
            self._reset_sequences(cur, models, names)
        with connection.cursor() as cur:
            for name in names:
                cur.execute(f"ANALYZE {_q(name)}")
        total = sum(e["rows"] for e in entries)
        self.stdout.write(
            self.style.SUCCESS(f"Imported {len(entries)} tables, {total} rows in {time.monotonic() - started:.0f}s")
        )

    def _assert_staging(self):
        if settings.SETTINGS_MODULE not in ALLOWED_SETTINGS:
            raise CommandError("import_catalog_subset only runs with --settings=conf.settings_staging")
        db = settings.DATABASES["default"]
        if "postgresql" not in db["ENGINE"]:
            raise CommandError("import_catalog_subset needs Postgres (COPY)")
        if db["HOST"] in settings.PRODUCTION_HOSTS:
            raise CommandError("DATABASE_HOST is production; refusing")

    @staticmethod
    def _has_rows(cur, name):
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {_q(name)})")
        return cur.fetchone()[0]

    @staticmethod
    def _reset_sequences(cur, models, names):
        for name in names:
            pk = models[name].pk
            cur.execute("SELECT pg_get_serial_sequence(%s, %s)", [_q(name), pk])
            seq = cur.fetchone()[0]
            if seq:
                cur.execute(
                    f"SELECT setval(%s, COALESCE(MAX({_q(pk)}), 1), MAX({_q(pk)}) IS NOT NULL) FROM {_q(name)}", [seq]
                )
