"""
Import the categories / brands / series indexes of one or more PACE members into their
pace_<member>_{categories,brands,series} tables.

Member-agnostic: ``--member`` selects which registered members to run, and omitting it runs all
of them. See src/management/commands/_pace_search.py for the member registry, the proxy
requirement and the 1,000-result cap.

These three indexes are small enough to page straight through (TSE: 718 / 670 / 9,396 docs),
with one caveat -- a plain walk cannot exceed 1,000 results, so any index over that is
truncated. TSE's series index reports 9,396 documents but yields only 1,000, and that is
expected here rather than a failure: this command is the taxonomy pass, and the parts commands
are the ones built to work around the cap. The shortfall is reported per index.

Two things worth knowing about the data:
  - Every document embeds a full ``parts`` array of that node's part numbers, so these are
    reverse indexes, not just taxonomy. The brands index is what seeds import_pace_parts_by_id.
  - Categories have no top-level documents: all of them carry a ``parent_name``, and the parents
    exist only as those strings. Derive the top level from distinct ``parent_slug`` values.

    python manage.py import_pace_taxonomy --member DPN
    python manage.py import_pace_taxonomy                      # every member
    python manage.py import_pace_taxonomy --member TSE --index brands --dry-run
"""
import logging

from django.core.management.base import BaseCommand

from src.management.commands import _pace_search as pace

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[IMPORT-PACE-TAXONOMY]"

# index suffix -> (hit mapper, columns to overwrite on conflict)
_KINDS = {
    "categories": (pace.category_instance, pace.CATEGORY_UPDATE_FIELDS),
    "brands": (pace.brand_instance, pace.BRAND_UPDATE_FIELDS),
    "series": (pace.series_instance, pace.SERIES_UPDATE_FIELDS),
}

# Documents carry full parts arrays, so pages are payload-heavy; 100 keeps a page well under a
# megabyte while still making even the largest taxonomy index only ~170 requests.
_PAGE_SIZE = 100


def _model_for(member: pace.Member, kind: str):
    return {
        "categories": member.category_model,
        "brands": member.brand_model,
        "series": member.series_model,
    }[kind]


class Command(BaseCommand):
    help = (
        "Import PACE members' categories/brands/series indexes into their pace_<member>_* "
        "tables. Requires the StormProxies gateways. Not scheduled."
    )

    def add_arguments(self, parser):
        pace.add_member_argument(parser)
        parser.add_argument(
            "--index", action="append", choices=sorted(_KINDS),
            help="Only this index (repeatable). Defaults to all three.",
        )
        parser.add_argument("--dry-run", action="store_true", help="Fetch and report, write nothing.")
        parser.add_argument(
            "--sleep", type=float, default=pace.DEFAULT_REQUEST_SPACING_SECONDS,
            metavar="SECONDS", help="Minimum gap between requests (default {}).".format(
                pace.DEFAULT_REQUEST_SPACING_SECONDS),
        )

    def handle(self, *args, **options):
        members = pace.resolve_members(options["member"])
        kinds = options["index"] or sorted(_KINDS)
        dry_run = options["dry_run"]
        spacing = max(0.0, options["sleep"])

        self.stdout.write("Importing {} for {} member(s){}".format(
            ", ".join(kinds), len(members), " [DRY RUN]" if dry_run else ""))
        self.stdout.flush()

        requests_made = 0
        for member in members:
            self.stdout.write("")
            self.stdout.write("{} {}".format(member.code, member.name))
            self.stdout.flush()

            for kind in kinds:
                index = member.index(kind)
                to_instance, update_fields = _KINDS[kind]
                model = _model_for(member, kind)
                searcher = pace.Searcher(index, spacing_seconds=spacing)

                try:
                    hits = searcher.walk("", page_size=_PAGE_SIZE)
                except Exception as e:  # noqa: BLE001 -- one index must not abort the sweep
                    self.stdout.write(self.style.ERROR("  {}: FAILED {}".format(index, str(e)[:160])))
                    self.stdout.flush()
                    requests_made += searcher.request_count
                    continue

                requests_made += searcher.request_count
                instances = [i for i in (to_instance(member, h) for h in hits) if i is not None]

                # What the index claims it holds, so a cap-induced shortfall is visible.
                _, total = searcher.slice_hits("", page_size=1)
                requests_made += 1

                if dry_run:
                    written = len(instances)
                    self.stdout.write("  {}: {} of {} docs -- not written".format(
                        index, written, total))
                else:
                    written = pace.upsert(model, instances, update_fields)
                    self.stdout.write(self.style.SUCCESS("  {}: {} of {} docs -> {}".format(
                        index, written, total, model._meta.db_table)))

                if total > written:
                    self.stdout.write(self.style.WARNING(
                        "    {} doc(s) beyond the {}-result cap -- a plain walk cannot reach "
                        "them".format(total - written, pace.MAX_TOTAL_HITS)))
                self.stdout.flush()
                logger.info("%s %s %s: %s of %s", _LOG_PREFIX, member.code, index, written, total)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            "Done: {} member(s), {} search requests".format(len(members), requests_made)))
        self.stdout.flush()
