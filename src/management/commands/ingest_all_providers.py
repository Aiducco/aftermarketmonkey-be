"""
Single scheduled entrypoint: all distributor **source fetches** in order, then
``sync_all_master_parts`` (applies all ``sync_derived_from_*`` in one pass), then one
Meilisearch reindex.

Creates a parent ``ScheduledTaskExecution`` for the whole run and a **child** execution per
sub-step (each provider source ingest, ``sync_all_master_parts``, and Meilisearch) so
ScheduledTaskExecution shows start/finish of each part.
"""
import contextlib
import logging
import typing

from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import (
    atech,
    dlg,
    keystone,
    master_parts,
    meyer,
    rough_country,
    turn_14,
    wheelpros,
)
from src.search.meilisearch_client import is_configured, reindex_all_master_parts

logger = logging.getLogger(__name__)

_TASK_NAME = "ingest_all_providers"


class Command(BaseCommand):
    help = (
        "Fetch/source-ingest for each distributor in order, then ``sync_all_master_parts``, then one "
        "Meilisearch reindex. Each step records its own ``ScheduledTaskExecution`` (per provider, "
        "sync, reindex) plus a parent run row."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-meilisearch",
            action="store_true",
            help="Complete DB ingest and master sync; skip the final full Meilisearch reindex.",
        )
        parser.add_argument(
            "--reindex-batch-size",
            type=int,
            default=None,
            help="Override documents per Meilisearch batch (default: MEILISEARCH_REINDEX_BATCH_SIZE / settings).",
        )
        parser.add_argument(
            "--reindex-upload-workers",
            type=int,
            default=None,
            help="Override parallel Meilisearch upload workers (default: MEILISEARCH_REINDEX_UPLOAD_WORKERS / settings).",
        )

    def _ingest_log(self, message: str) -> None:
        """Log line for scheduled/operational monitoring (file + console)."""
        line = "[{}] {}".format(_TASK_NAME, message)
        logger.info(line)
        self.stdout.write(line)

    @contextlib.contextmanager
    def _audited_step(
        self,
        task_name: str,
        success_message: str,
    ) -> typing.Iterator[None]:
        """
        ``start_scheduled_task_execution`` at entry, ``mark_scheduled_task_completed`` or
        ``mark_scheduled_task_failed`` on exit. Yields so the block runs the work.
        """
        ex = audit_scheduled_tasks.start_scheduled_task_execution(task_name)
        self._ingest_log("subtask started | task_name={}".format(task_name))
        try:
            yield
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(ex, error_message=str(e))
            self._ingest_log("subtask failed | task_name={} | error={!s}".format(task_name, e))
            raise
        audit_scheduled_tasks.mark_scheduled_task_completed(ex, message=success_message)
        self._ingest_log("subtask completed | task_name={}".format(task_name))

    def handle(self, *args, **options):
        self._ingest_log("start")
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        try:
            self._run_turn14()
            self._run_keystone()
            self._run_meyer()
            self._run_atech()
            self._run_rough_country()
            self._run_dlg()
            self._run_wheelpros()

            with self._audited_step(
                "ingest_all_providers_sync_all_master_parts",
                "Full derived sync (Turn14, Keystone, Meyer, A-Tech, RC, DLG, WheelPros) complete.",
            ):
                self._ingest_log("all provider source ingests done; running sync_all_master_parts()")
                master_parts.sync_all_master_parts()
            self.stdout.write(self.style.SUCCESS("Master parts sync completed."))

            if not options.get("skip_meilisearch") and is_configured():
                with self._audited_step(
                    "ingest_all_providers_meilisearch_reindex",
                    "Meilisearch full reindex (delete + bulk index) complete.",
                ):
                    self._ingest_log("Meilisearch full reindex (delete + index)")
                    bs = options.get("reindex_batch_size")
                    w = options.get("reindex_upload_workers")
                    kwargs: typing.Dict[str, int] = {}
                    if bs is not None:
                        kwargs["batch_size"] = bs
                    if w is not None:
                        kwargs["max_upload_workers"] = w
                    ok, fail = reindex_all_master_parts(**kwargs)
                    self._ingest_log(
                        "Meilisearch reindex finished | indexed={} failed={}".format(ok, fail)
                    )
                    self.stdout.write(
                        self.style.SUCCESS(
                            "Meilisearch: indexed {} parts, failed {}.".format(ok, fail)
                        )
                    )
            elif not options.get("skip_meilisearch"):
                ex = audit_scheduled_tasks.start_scheduled_task_execution(
                    "ingest_all_providers_meilisearch_reindex"
                )
                audit_scheduled_tasks.mark_scheduled_task_skipped(
                    ex, message="Meilisearch not configured (MEILISEARCH_HOST empty)."
                )
                self._ingest_log("Meilisearch not configured; subtask recorded as SKIPPED")
            else:
                ex = audit_scheduled_tasks.start_scheduled_task_execution(
                    "ingest_all_providers_meilisearch_reindex"
                )
                audit_scheduled_tasks.mark_scheduled_task_skipped(
                    ex, message="Skipped: --skip-meilisearch"
                )
                self._ingest_log("Meilisearch reindex skipped; subtask recorded as SKIPPED")

            end_msg = "Completed full scheduled ingest and master parts sync"
            if options.get("skip_meilisearch"):
                end_msg += " (Meilisearch skipped)."
            elif is_configured():
                end_msg += "; Meilisearch reindex ran."
            else:
                end_msg += " (Meilisearch not configured)."
            audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=end_msg)
            self._ingest_log("run completed successfully")
            self.stdout.write(self.style.SUCCESS("Successfully completed {}.".format(_TASK_NAME)))
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise

    def _run_turn14(self) -> None:
        with self._audited_step(
            "ingest_all_providers_turn14",
            "Turn14 source fetch complete: brands, item updates, inventory updates (derived in sync_all).",
        ):
            self._ingest_log("Turn14: brands + items + inventory fetches only; derived in sync_all")
            turn_14.fetch_and_save_turn_14_brands()
            synced = turn_14.sync_unmapped_turn_14_brands_to_brands()
            if synced:
                n = len(synced)
                self._ingest_log(
                    "Turn14: {} new brand(s); fetching items, media, pricing, inventory for those".format(
                        n
                    )
                )
                turn_14.fetch_and_save_turn_14_items_for_turn14_brands(synced)
                turn_14.fetch_and_save_turn_14_brand_data_for_turn14_brands(synced)
                turn_14.fetch_and_save_turn_14_brand_pricing_for_turn14_brands(synced)
                turn_14.fetch_and_save_turn_14_brand_inventory_for_turn14_brands(synced)
            else:
                self._ingest_log("Turn14: no new brands; skipped brand-scoped fetches")
            self._ingest_log("Turn14: fetching item updates from API")
            turn_14.fetch_and_save_turn_14_items_updates()
            self._ingest_log("Turn14: fetching inventory updates from API")
            turn_14.fetch_and_save_turn_14_inventory_updates()

    def _run_keystone(self) -> None:
        with self._audited_step(
            "ingest_all_providers_keystone",
            "Keystone source fetch complete: brands, parts (derived in sync_all).",
        ):
            self._ingest_log("Keystone: brands + parts fetches only")
            keystone.fetch_and_save_keystone_brands()
            keystone.sync_unmapped_keystone_brands_to_brands()
            keystone.fetch_and_save_all_keystone_brand_parts()

    def _run_meyer(self) -> None:
        with self._audited_step(
            "ingest_all_providers_meyer",
            "Meyer source fetch complete: catalog, unmapped brand sync (derived in sync_all).",
        ):
            self._ingest_log("Meyer: SFTP catalog + unmapped brand sync")
            meyer.fetch_and_save_meyer_catalog_and_inventory(force_download=False)
            meyer.sync_unmapped_meyer_brands_to_brands()

    def _run_atech(self) -> None:
        with self._audited_step(
            "ingest_all_providers_atech",
            "A-Tech source fetch complete (derived in sync_all).",
        ):
            self._ingest_log("A-Tech: feed download + upsert")
            atech.fetch_and_save_atech_catalog(force_download=False)

    def _run_rough_country(self) -> None:
        with self._audited_step(
            "ingest_all_providers_rough_country",
            "Rough Country source fetch + unmapped brand sync complete (derived in sync_all).",
        ):
            self._ingest_log("Rough Country: feed + unmapped brand sync")
            rough_country.fetch_and_save_rough_country(
                file_url=None,
                local_file_path=None,
                download=True,
            )
            rough_country.sync_unmapped_rough_country_brands_to_brands()

    def _run_dlg(self) -> None:
        with self._audited_step(
            "ingest_all_providers_dlg",
            "DLG source fetch complete (derived in sync_all).",
        ):
            self._ingest_log("DLG: inventory SFTP + upsert")
            dlg.fetch_and_save_dlg_catalog(force_download=False)
            dlg.sync_unmapped_dlg_brands_to_brands()

    def _run_wheelpros(self) -> None:
        with self._audited_step(
            "ingest_all_providers_wheelpros",
            "WheelPros source fetch (wheel, tire, accessories) + unmapped brand sync complete (derived in sync_all).",
        ):
            self._ingest_log("WheelPros: wheel, tire, accessories + unmapped brand sync")
            for ft in ("wheel", "tire", "accessories"):
                self._ingest_log("WheelPros: fetching feed={}".format(ft))
                wheelpros.fetch_and_save_wheelpros(
                    local_file_path=None,
                    download=True,
                    local_only=False,
                    feed_type=ft,
                )
            wheelpros.sync_unmapped_wheelpros_brands_to_brands()
