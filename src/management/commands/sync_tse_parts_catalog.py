"""
Populate TSEParts from the already-imported ``tristate_parts`` table (TriStatePart -- Tri-State's
own catalog slice of AAM/PACE's shared Algolia index, see import_tristate_parts_from_algolia).
Thin wrapper around src.integrations.services.tristate.sync_tse_parts_from_tristate_part_seed --
see that function's docstring for why this is safe to also run on the regular nightly cycle
(ingest_all_providers._run_tristate calls the same function directly).

    python manage.py sync_tse_parts_catalog
"""
from django.core.management.base import BaseCommand

from src.integrations.services import tristate as tristate_services


class Command(BaseCommand):
    help = "Copy tristate_parts (Algolia-sourced) into TSEParts for every known TSEBrand."

    def handle(self, *args, **options):
        total = tristate_services.sync_tse_parts_from_tristate_part_seed()
        self.stdout.write(self.style.SUCCESS("Done. {} TSEParts rows upserted.".format(total)))
