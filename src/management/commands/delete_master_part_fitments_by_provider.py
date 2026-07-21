from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import master_parts


class Command(BaseCommand):
    help = (
        "On-demand cleanup: batched delete of MasterPartFitment rows for one provider kind "
        "(e.g. TURN_14). Intended to clear the single-year-per-row backlog written before "
        "sync_master_part_fitments_from_turn14_vcdb started collapsing years into ranges - run "
        "this, then re-run sync_master_part_fitments_from_turn14_vcdb to repopulate. Defaults to "
        "--dry-run (counts matching rows only); pass --apply to actually delete. Not scheduled - "
        "run manually."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--provider-kind",
            type=str,
            default="TURN_14",
            help="Providers.kind_name to delete MasterPartFitment rows for (default: TURN_14).",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=master_parts.FITMENT_DELETE_BATCH_SIZE,
            help="Rows deleted per statement.",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Actually delete. Without this flag, only counts matching rows and exits.",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=1,
            help=(
                "Concurrent delete workers, each scoped to a disjoint id range (default: 1, "
                "sequential). Keep modest - more workers means more concurrent I/O/WAL/lock "
                "pressure on a table still serving live app traffic."
            ),
        )

    def handle(self, *args, **options):
        kind = options["provider_kind"]
        batch_size = max(1, int(options["batch_size"]))
        workers = max(1, int(options["workers"]))
        apply = options["apply"]

        if apply:
            confirm = input(
                "This will permanently delete MasterPartFitment rows for provider kind={} using {} worker(s). "
                "Type the provider kind to confirm: ".format(kind, workers)
            )
            if confirm.strip() != kind:
                raise CommandError("Confirmation did not match provider kind={}. Aborting.".format(kind))

        result = master_parts.delete_master_part_fitments_by_provider_kind(
            kind, batch_size=batch_size, dry_run=not apply, workers=workers
        )
        if apply:
            self.stdout.write(self.style.SUCCESS("Deleted {} MasterPartFitment rows for kind={}.".format(result, kind)))
        else:
            self.stdout.write(
                "[dry-run] {} MasterPartFitment rows match kind={}. Re-run with --apply to delete.".format(
                    result, kind
                )
            )
