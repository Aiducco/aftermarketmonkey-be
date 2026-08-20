"""
Periodic backfill of MasterPartData (and the fill-if-blank core fields on MasterPart) from a
distributor whose raw data we already hold. See src/integrations/services/master_part_enrichment.py
for the mapping rules and why the output shape copies ASAP Network's.

Typical runs:
    # eyeball what a run would write, without writing
    manage.py enrich_master_part_data --sample 3

    # fill every Turn14 part that has no master_part_data row yet, in a bounded bite
    manage.py enrich_master_part_data --source turn14 --mode missing-row --limit 100000

    # only the gallery + rich description, one brand, and refresh search for what changed
    manage.py enrich_master_part_data --fields images,description --brand-ids 42 --reindex-meilisearch
"""
import typing

from django.core.management.base import BaseCommand, CommandError

from src.audit import scheduled_tasks as audit_scheduled_tasks
from src.integrations.services import master_part_enrichment
from src.search import meilisearch_client

_TASK_NAME = "enrich_master_part_data"


class Command(BaseCommand):
    help = (
        "Fill MasterPartData (images, description HTML, installation instructions, youtube video, "
        "field specs) and fill-if-blank MasterPart core fields from a distributor source. "
        "Idempotent: filled parts drop out of the candidate set on the next run."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--source",
            default=master_part_enrichment.DEFAULT_SOURCE,
            choices=list(master_part_enrichment.SOURCES),
            help=(
                "Which distributor's raw data to enrich from (default: {}). Recommended run "
                "order for the rest, richest-first so fill-if-blank resolves precedence: {}.".format(
                    master_part_enrichment.DEFAULT_SOURCE,
                    " -> ".join(master_part_enrichment.RECOMMENDED_ORDER),
                )
            ),
        )
        parser.add_argument(
            "--fields",
            default=",".join(master_part_enrichment.ALL_FIELDS),
            help=(
                "Comma-separated fields to fill. MasterPartData: {}. MasterPart (fill-if-blank): {}. "
                "Default: all.".format(
                    ", ".join(master_part_enrichment.DATA_FIELDS),
                    ", ".join(master_part_enrichment.CORE_FIELDS),
                )
            ),
        )
        parser.add_argument(
            "--mode",
            default=master_part_enrichment.MODE_MISSING_ROW,
            choices=list(master_part_enrichment.MODES),
            help=(
                "Which parts to consider: missing-row (no master_part_data row, the default), "
                "blank-fields (row exists, selected field still blank), stale (source refreshed "
                "its payload after we last wrote), all."
            ),
        )
        parser.add_argument(
            "--brand-ids",
            default=None,
            help="Comma-separated internal Brands ids to restrict the run to.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Stop after examining this many candidate provider parts.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=master_part_enrichment.BATCH_SIZE,
            help="Candidates per batch (default: {}).".format(master_part_enrichment.BATCH_SIZE),
        )
        parser.add_argument(
            "--start-after-provider-part-id",
            type=int,
            default=0,
            help="Resume keyset pagination after this provider_parts.id.",
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Replace existing values instead of only filling blanks.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Build payloads and report what would change; write nothing.",
        )
        parser.add_argument(
            "--sample",
            type=int,
            default=0,
            help="Print the composed payload for this many candidates, then exit without writing.",
        )
        parser.add_argument(
            "--reindex-meilisearch",
            action="store_true",
            help="Reindex the master parts this run touched (image_url/description are indexed).",
        )

    def handle(self, *args, **options):
        fields = [f.strip() for f in (options["fields"] or "").split(",") if f.strip()]
        unknown = [f for f in fields if f not in master_part_enrichment.ALL_FIELDS]
        if unknown:
            raise CommandError("Unknown field(s): {}".format(", ".join(unknown)))
        if not fields:
            raise CommandError("--fields resolved to an empty list.")

        brand_ids = self._parse_brand_ids(options.get("brand_ids"))

        if options["sample"]:
            self._print_samples(
                source_name=options["source"],
                mode=options["mode"],
                count=options["sample"],
                brand_ids=brand_ids,
                start_after_provider_part_id=options["start_after_provider_part_id"],
            )
            return

        audit_scheduled_tasks.cleanup_stale_started_executions(_TASK_NAME, max_age_minutes=720)
        execution = audit_scheduled_tasks.start_scheduled_task_execution(_TASK_NAME)
        self.stdout.write(
            "Enriching master parts: source={} mode={} fields={}{}".format(
                options["source"],
                options["mode"],
                ",".join(fields),
                " (dry run)" if options["dry_run"] else "",
            )
        )
        try:
            stats = master_part_enrichment.enrich_master_parts(
                source_name=options["source"],
                fields=fields,
                mode=options["mode"],
                brand_ids=brand_ids,
                limit=options["limit"],
                batch_size=options["batch_size"],
                start_after_provider_part_id=options["start_after_provider_part_id"],
                overwrite=options["overwrite"],
                dry_run=options["dry_run"],
                collect_touched_ids=options["reindex_meilisearch"],
            )

            message = stats.as_message()
            self.stdout.write(self.style.SUCCESS(message))

            if options["reindex_meilisearch"] and not options["dry_run"]:
                message = "{} | {}".format(message, self._reindex(stats.touched_master_part_ids))

            audit_scheduled_tasks.mark_scheduled_task_completed(execution, message=message)
        except Exception as e:
            audit_scheduled_tasks.mark_scheduled_task_failed(execution, error_message=str(e))
            self.stdout.write(self.style.ERROR("Error: {}".format(str(e))))
            raise

    def _parse_brand_ids(self, raw: typing.Optional[str]) -> typing.Optional[typing.List[int]]:
        if not raw:
            return None
        try:
            return [int(x.strip()) for x in raw.split(",") if x.strip()]
        except ValueError:
            raise CommandError("--brand-ids must be a comma-separated list of integers.")

    def _reindex(self, master_part_ids: typing.List[int]) -> str:
        if not master_part_ids:
            return "meilisearch: nothing touched"
        if not meilisearch_client.is_configured():
            return "meilisearch: not configured, skipped"

        from src import models as src_models

        ok, failed = meilisearch_client.add_documents_in_batches(
            src_models.MasterPart.objects.all(),
            ids=master_part_ids,
        )
        result = "meilisearch: indexed {} parts, failed {}".format(ok, failed)
        self.stdout.write(self.style.SUCCESS(result))
        return result

    def _print_samples(
        self,
        *,
        source_name: str,
        mode: str,
        count: int,
        brand_ids: typing.Optional[typing.List[int]],
        start_after_provider_part_id: int,
    ) -> None:
        samples = master_part_enrichment.preview_payloads(
            source_name=source_name,
            mode=mode,
            count=count,
            brand_ids=brand_ids,
            start_after_provider_part_id=start_after_provider_part_id,
        )
        if not samples:
            self.stdout.write(self.style.WARNING("No candidates matched."))
            return

        for master_part_id, payload in samples:
            self.stdout.write(self.style.MIGRATE_HEADING("\nmaster_part_id={}".format(master_part_id)))
            self.stdout.write("  source_external_id: {}".format(payload.source_external_id))
            self.stdout.write("  core_image_url:     {}".format(payload.core_image_url))
            self.stdout.write("  core_description:   {}".format(payload.core_description))
            self.stdout.write("  core_gtin:          {}".format(payload.core_gtin))
            self.stdout.write("  youtube_video:      {}".format(payload.youtube_video))
            self.stdout.write("  color / series:     {} / {}".format(payload.color, payload.series))
            self.stdout.write("  warranty:           {}".format(payload.warranty))
            self.stdout.write("  images ({}):".format(len(payload.images or [])))
            for url in (payload.images or [])[:5]:
                self.stdout.write("    - {}".format(url))
            self.stdout.write("  installation_instructions:")
            for url in payload.installation_instructions or []:
                self.stdout.write("    - {}".format(url))
            self.stdout.write("  field_specs:")
            for spec in payload.field_specs or []:
                self.stdout.write("    - {}: {}".format(spec["spec_name"], spec["spec_value"]))
            self.stdout.write("  description (HTML):")
            self.stdout.write("    {}".format(payload.description or ""))
