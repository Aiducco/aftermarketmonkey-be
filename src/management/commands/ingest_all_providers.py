"""
Single scheduled entrypoint: all distributor **source fetches** sequentially, then
``sync_all_master_parts_global`` (catalog + inventory, no pricing), then pricing jobs
enqueued for all active company-providers, then Meilisearch zero-downtime reindex.

Architecture overview:
  Phase 1 — Source fetch (all providers sequential):
    All distributors (Turn14, Keystone, Meyer, A-Tech, Rough Country, DLG, WheelPros,
    Premier) run one-at-a-time with memory reclaimed between each to stay within
    server RAM limits. Per-company pricing is no longer embedded here, so each
    provider only loads its own catalog/inventory — much lighter than before.
    WheelPros fetches its three feeds (wheel / tire / accessories) concurrently
    internally, but the provider itself still runs sequentially relative to others.

  Phase 2 — Global catalog + inventory sync:
    ``sync_all_master_parts_global()`` upserts MasterPart / ProviderPart / ProviderPartInventory
    for every provider. Turn14 always runs first inside this function; all other providers
    follow sequentially with memory reclamation between each. No pricing in this phase.

  Phase 3 — Enqueue per-company pricing jobs:
    ``enqueue_all_active_company_provider_pricing_jobs()`` creates an
    ``IntegrationPricingSyncJob`` row for every active company-provider. The
    ``process_integration_pricing_sync_jobs`` management command (run by a separate cron)
    picks these up and runs per-company pricing syncs asynchronously, decoupling them from
    the long catalog sync window.

  Phase 4 — Meilisearch zero-downtime reindex (nightly, via index_parts_meilisearch):
    ``reindex_all_master_parts_zero_downtime()`` indexes into a staging index then
    atomically swaps it with the live index so users never see an empty search.
    This phase is intentionally **disabled** here and runs from its own nightly cron
    to avoid OOM during the 4-hour ingest window.

Creates a parent ``ScheduledTaskExecution`` for the whole run and a **child** execution per
sub-step so the admin panel shows start/finish of each part.
"""
import concurrent.futures
import contextlib
import logging
import typing

from django.db import connection

from django.core.management.base import BaseCommand

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import (
    atech,
    dlg,
    integration_pricing_sync_jobs,
    keystone,
    master_parts,
    meyer,
    premier,
    rough_country,
    turn_14,
    wheelpros,
)
from src.search.meilisearch_client import is_configured, reindex_all_master_parts_zero_downtime

logger = logging.getLogger(__name__)

_TASK_NAME = "ingest_all_providers"


class Command(BaseCommand):
    help = (
        "Source fetch for all distributors (sequential, with memory reclamation between each), "
        "then sync_all_master_parts_global (catalog + inventory, no pricing), then enqueue "
        "per-company pricing jobs, then Meilisearch zero-downtime reindex (skipped here; run "
        "index_parts_meilisearch nightly)."
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
        continue_on_error: bool = False,
    ) -> typing.Iterator[None]:
        """
        ``start_scheduled_task_execution`` at entry, ``mark_scheduled_task_completed`` or
        ``mark_scheduled_task_failed`` on exit. Yields so the block runs the work.

        If ``continue_on_error=True`` the exception is swallowed after being recorded so the
        parent run can continue with the next provider.
        """
        ex = audit_scheduled_tasks.start_scheduled_task_execution(task_name)
        self._ingest_log("subtask started | task_name={}".format(task_name))
        try:
            yield
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(ex, error_message=str(e))
            self._ingest_log("subtask failed | task_name={} | error={!s}".format(task_name, e))
            if not continue_on_error:
                raise
            self._ingest_log("subtask failure ignored (continue_on_error=True) | task_name={}".format(task_name))
            return
        audit_scheduled_tasks.mark_scheduled_task_completed(ex, message=success_message)
        self._ingest_log("subtask completed | task_name={}".format(task_name))

    def handle(self, *args, **options):
        audit_scheduled_tasks.cleanup_stale_started_executions([
            _TASK_NAME,
            "ingest_all_providers_turn14",
            "ingest_all_providers_keystone",
            "ingest_all_providers_meyer",
            "ingest_all_providers_atech",
            "ingest_all_providers_rough_country",
            "ingest_all_providers_dlg",
            "ingest_all_providers_wheelpros",
            "ingest_all_providers_premier",
            "ingest_all_providers_sync_all_master_parts",
            "ingest_all_providers_enqueue_pricing_jobs",
            "ingest_all_providers_meilisearch_reindex",
        ])
        self._ingest_log("start")
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        try:
            # ----------------------------------------------------------------
            # Phase 1: All providers fetch sequentially.
            # Memory is reclaimed between each provider so the process stays
            # within server RAM limits. Per-company pricing is no longer done
            # here (moved to Phase 3 pricing jobs), so each fetch only loads
            # catalog + inventory — much lighter than before.
            # WheelPros fetches its three feeds concurrently internally but
            # is still sequential relative to every other provider.
            # ----------------------------------------------------------------
            self._ingest_log("phase 1 | starting sequential source fetches for all providers")
            phase1_providers: typing.List[typing.Tuple[str, typing.Callable[[], None]]] = [
                ("turn14",        self._run_turn14),
                ("keystone",      self._run_keystone),
                ("meyer",         self._run_meyer),
                ("atech",         self._run_atech),
                ("rough_country", self._run_rough_country),
                ("dlg",           self._run_dlg),
                ("wheelpros",     self._run_wheelpros),
                ("premier",       self._run_premier),
            ]
            for name, run_fn in phase1_providers:
                self._ingest_log("phase 1 | starting {} fetch".format(name))
                try:
                    run_fn()
                    self._ingest_log("phase 1 | {} fetch complete".format(name))
                except Exception as exc:  # noqa: BLE001 – _audited_step already swallows; belt-and-suspenders
                    self._ingest_log("phase 1 | {} fetch failed: {!s}".format(name, exc))
                master_parts._reclaim_memory()
                connection.close()

            self._ingest_log("phase 1 | all provider source fetches complete")

            # ----------------------------------------------------------------
            # Phase 2: Global catalog + inventory sync (Turn14 first inside).
            # No pricing — pricing is handled per-company via the job queue.
            # ----------------------------------------------------------------
            with self._audited_step(
                "ingest_all_providers_sync_all_master_parts",
                "Global catalog + inventory sync (all providers, no pricing) complete.",
            ):
                self._ingest_log(
                    "phase 2 | running sync_all_master_parts_global() "
                    "(Turn14 first, then others; no pricing)"
                )
                master_parts.sync_all_master_parts_global()
            self.stdout.write(self.style.SUCCESS("Master parts global sync completed."))

            # ----------------------------------------------------------------
            # Phase 3: Enqueue per-company pricing sync jobs.
            # ----------------------------------------------------------------
            with self._audited_step(
                "ingest_all_providers_enqueue_pricing_jobs",
                "Per-company pricing sync jobs enqueued for all active company-providers.",
            ):
                self._ingest_log("phase 3 | enqueueing pricing sync jobs for all active company-providers")
                n_jobs = integration_pricing_sync_jobs.enqueue_all_active_company_provider_pricing_jobs()
                self._ingest_log("phase 3 | enqueued {} pricing sync job(s)".format(n_jobs))
            self.stdout.write(self.style.SUCCESS("Pricing sync jobs enqueued: {}.".format(n_jobs)))

            # ----------------------------------------------------------------
            # Phase 4: Meilisearch zero-downtime reindex.
            # Intentionally disabled here — run ``index_parts_meilisearch`` from its
            # own nightly cron to avoid OOM during the 4-hour ingest window.
            # ----------------------------------------------------------------
            ex = audit_scheduled_tasks.start_scheduled_task_execution(
                "ingest_all_providers_meilisearch_reindex"
            )
            audit_scheduled_tasks.mark_scheduled_task_skipped(
                ex, message="Meilisearch reindex disabled in ingest_all_providers; run index_parts_meilisearch separately."
            )
            self._ingest_log("phase 4 | Meilisearch reindex skipped (run index_parts_meilisearch nightly)")

            end_msg = (
                "Completed full scheduled ingest, global master parts sync, and pricing job enqueueing "
                "(Meilisearch reindex runs separately via nightly cron)."
            )
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
            continue_on_error=True,
        ):
            self._ingest_log("Turn14: brands + items + inventory fetches only; derived in sync_all")
            turn_14.fetch_and_save_turn_14_brands()
            synced = turn_14.sync_unmapped_turn_14_brands_to_brands()
            if synced:
                n = len(synced)
                self._ingest_log(
                    "Turn14: {} new brand(s); fetching items, media, inventory for those "
                    "(per-company pricing handled by Phase 3 pricing jobs)".format(n)
                )
                turn_14.fetch_and_save_turn_14_items_for_turn14_brands(synced)
                turn_14.fetch_and_save_turn_14_brand_data_for_turn14_brands(synced)
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
            continue_on_error=True,
        ):
            self._ingest_log("Keystone: brands + parts fetches only")
            keystone.fetch_and_save_keystone_brands()
            keystone.sync_unmapped_keystone_brands_to_brands()
            keystone.fetch_and_save_all_keystone_brand_parts()

    def _run_meyer(self) -> None:
        with self._audited_step(
            "ingest_all_providers_meyer",
            "Meyer source fetch complete: catalog, unmapped brand sync (derived in sync_all).",
            continue_on_error=True,
        ):
            self._ingest_log("Meyer: SFTP catalog + unmapped brand sync")
            meyer.fetch_and_save_meyer_catalog_and_inventory(force_download=False)
            meyer.sync_unmapped_meyer_brands_to_brands()

    def _run_atech(self) -> None:
        with self._audited_step(
            "ingest_all_providers_atech",
            "A-Tech source fetch complete (derived in sync_all).",
            continue_on_error=True,
        ):
            self._ingest_log("A-Tech: feed download + upsert")
            atech.fetch_and_save_atech_catalog(force_download=False)

    def _run_rough_country(self) -> None:
        with self._audited_step(
            "ingest_all_providers_rough_country",
            "Rough Country source fetch + unmapped brand sync complete (derived in sync_all).",
            continue_on_error=True,
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
            continue_on_error=True,
        ):
            self._ingest_log("DLG: inventory SFTP + upsert")
            dlg.fetch_and_save_dlg_catalog(force_download=False)
            dlg.sync_unmapped_dlg_brands_to_brands()

    def _run_wheelpros(self) -> None:
        with self._audited_step(
            "ingest_all_providers_wheelpros",
            "WheelPros source fetch (wheel, tire, accessories) + unmapped brand sync complete (derived in sync_all).",
            continue_on_error=True,
        ):
            self._ingest_log("WheelPros: wheel, tire, accessories (parallel) + unmapped brand sync")

            def _fetch_feed(ft: str) -> None:
                self._ingest_log("WheelPros: fetching feed={}".format(ft))
                wheelpros.fetch_and_save_wheelpros(
                    local_file_path=None,
                    download=True,
                    local_only=False,
                    feed_type=ft,
                )
                connection.close()

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=3,
                thread_name_prefix="wheelpros_feed",
            ) as wp_pool:
                futs = {wp_pool.submit(_fetch_feed, ft): ft for ft in ("wheel", "tire", "accessories")}
                for fut in concurrent.futures.as_completed(futs):
                    ft = futs[fut]
                    try:
                        fut.result()
                        self._ingest_log("WheelPros: feed={} finished".format(ft))
                    except Exception as exc:  # noqa: BLE001
                        self._ingest_log("WheelPros: feed={} failed: {!s}".format(ft, exc))

            wheelpros.sync_unmapped_wheelpros_brands_to_brands()

    def _run_premier(self) -> None:
        with self._audited_step(
            "ingest_all_providers_premier",
            "Premier source fetch complete: brands, parts (derived in sync_all).",
            continue_on_error=True,
        ):
            self._ingest_log("Premier: brands + parts fetches only")
            premier.fetch_and_save_premier_brands()
            premier.sync_unmapped_premier_brands_to_brands()
            premier.fetch_and_save_all_premier_brand_parts()
