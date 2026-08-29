"""
Scrape simpletire.com's public tire catalog into ``simpletire_skus`` -- one row per SKU (size).

The machinery, and the reasoning behind hitting their JSON endpoint rather than parsing the page,
is in ``src/integrations/services/simpletire.py``. Read that before changing anything here.

Scale: ~11,000 product lines and ~70,000 requests for a full pass. At the default 5 req/s that is
roughly four hours. It is resumable -- every finished product line is appended to a checkpoint
file and skipped on restart -- so run it under ``nohup``/``screen`` and let it finish, or run it
brand by brand.

Typical runs:

    # see what would be crawled, cost nothing
    manage.py fetch_simpletire_catalog --list-brands

    # one brand end to end (good first run: ~3 lines, seconds)
    manage.py fetch_simpletire_catalog --brand antares-tires

    # rehearse a few lines without writing anything
    manage.py fetch_simpletire_catalog --limit-lines 5 --max-sizes 2 --dry-run

    # the real thing
    nohup manage.py fetch_simpletire_catalog --concurrency 6 --rate 5 > /tmp/simpletire.log 2>&1 &

    # after a crash: identical command. Finished lines are skipped.
    # to start over instead, pass --no-resume (or delete the checkpoint file)
"""
import pathlib

from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import simpletire

DEFAULT_CHECKPOINT_PATH = pathlib.Path("logs/simpletire_catalog_checkpoint.jsonl")


class Command(BaseCommand):
    help = (
        "Crawl simpletire.com and upsert every tire SKU it lists into simpletire_skus. Resumable; "
        "safe to re-run. Use --dry-run to fetch without writing."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--brand",
            action="append",
            default=None,
            metavar="SLUG",
            help=(
                "Limit to a brand slug as it appears in the URL, e.g. antares-tires. Repeatable. "
                "Omit for the whole catalog."
            ),
        )
        parser.add_argument(
            "--limit-lines",
            type=int,
            default=None,
            help="Crawl at most this many product lines. For rehearsals, not for production runs.",
        )
        parser.add_argument(
            "--max-sizes",
            type=int,
            default=None,
            help="Crawl at most this many sizes per product line. Rehearsal only -- it writes an "
            "incomplete catalog, and resume will not come back for the rest.",
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=simpletire.DEFAULT_CONCURRENCY,
            help=f"Product lines fetched in parallel (default: {simpletire.DEFAULT_CONCURRENCY}).",
        )
        parser.add_argument(
            "--rate",
            type=float,
            default=simpletire.DEFAULT_RATE_PER_SECOND,
            help=(
                f"Requests per second across all workers (default: {simpletire.DEFAULT_RATE_PER_SECOND}). "
                "Their robots.txt disallows /api/*; keep this neighbourly."
            ),
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=simpletire.DEFAULT_BATCH_SIZE,
            help=f"Rows per upsert batch (default: {simpletire.DEFAULT_BATCH_SIZE}).",
        )
        parser.add_argument(
            "--checkpoint",
            default=str(DEFAULT_CHECKPOINT_PATH),
            help=f"Resume file, one JSON record per finished product line (default: {DEFAULT_CHECKPOINT_PATH}).",
        )
        parser.add_argument(
            "--no-resume",
            action="store_true",
            help="Ignore the checkpoint and re-crawl everything. The upsert makes this safe, just slow.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and parse, write nothing -- to the table or the checkpoint.",
        )
        parser.add_argument(
            "--list-brands",
            action="store_true",
            help="Print the brand slugs from the sitemap and exit. Two requests, no crawl.",
        )

    def handle(self, *args, **options):
        if options["concurrency"] < 1:
            raise CommandError("--concurrency must be at least 1")
        if options["rate"] <= 0:
            raise CommandError("--rate must be greater than 0")

        stats = simpletire.CrawlStats()

        if options["list_brands"]:
            self._list_brands(stats)
            return

        self.stdout.write("Reading /sitemap/product-line.xml ...")
        refs = simpletire.fetch_product_line_refs(stats=stats)
        self.stdout.write(f"  {len(refs)} product lines listed")

        refs = self._filter(refs, brands=options["brand"], limit=options["limit_lines"])
        if not refs:
            raise CommandError("No product lines matched. Check --brand against --list-brands.")

        dry_run = options["dry_run"]
        checkpoint_path = None if dry_run else pathlib.Path(options["checkpoint"])

        self.stdout.write(
            f"Crawling {len(refs)} product lines "
            f"(concurrency={options['concurrency']}, rate={options['rate']}/s"
            f"{', DRY RUN' if dry_run else f', checkpoint={checkpoint_path}'})"
        )

        result = simpletire.run_crawl(
            refs=refs,
            concurrency=options["concurrency"],
            rate_per_second=options["rate"],
            batch_size=options["batch_size"],
            checkpoint_path=checkpoint_path,
            resume=not options["no_resume"],
            dry_run=dry_run,
            max_sizes_per_line=options["max_sizes"],
            progress=self.stdout.write,
        )
        result.requests_made += stats.requests_made
        self._report(result, dry_run=dry_run)

    def _list_brands(self, stats: simpletire.CrawlStats) -> None:
        brand_slugs = simpletire.fetch_brand_slugs(stats=stats)
        line_counts: dict[str, int] = {}
        for ref in simpletire.fetch_product_line_refs(stats=stats):
            line_counts[ref.brand_slug] = line_counts.get(ref.brand_slug, 0) + 1

        for slug in sorted(set(brand_slugs) | set(line_counts)):
            self.stdout.write(f"  {slug:<40} {line_counts.get(slug, 0):>5} product lines")
        self.stdout.write(
            self.style.SUCCESS(f"{len(set(brand_slugs) | set(line_counts))} brands, {sum(line_counts.values())} lines")
        )

    def _filter(
        self,
        refs: list[simpletire.ProductLineRef],
        *,
        brands: list[str] | None,
        limit: int | None,
    ) -> list[simpletire.ProductLineRef]:
        if brands:
            wanted = {slug.strip().lower() for slug in brands if slug.strip()}
            refs = [ref for ref in refs if ref.brand_slug.lower() in wanted]
            found = {ref.brand_slug.lower() for ref in refs}
            for slug in sorted(wanted - found):
                self.stdout.write(self.style.WARNING(f"  no product lines for brand slug '{slug}'"))
            self.stdout.write(f"  {len(refs)} product lines after brand filter")
        if limit is not None:
            refs = refs[:limit]
        return refs

    def _report(self, stats: simpletire.CrawlStats, *, dry_run: bool) -> None:
        verb = "would write" if dry_run else "wrote"
        self.stdout.write("")
        self.stdout.write(f"  product lines crawled : {stats.lines_done}")
        self.stdout.write(f"  skipped (checkpoint)  : {stats.lines_skipped}")
        self.stdout.write(f"  gone from the site    : {stats.lines_missing}")
        self.stdout.write(f"  no purchasable sizes  : {stats.lines_empty}")
        self.stdout.write(f"  requests made         : {stats.requests_made}")
        self.stdout.write(f"  SKUs {verb:<14}: {stats.skus_written}")

        if stats.lines_failed:
            self.stdout.write(
                self.style.WARNING(
                    f"  failed                : {stats.lines_failed} "
                    "(not checkpointed -- re-run the same command to retry them)"
                )
            )
        self.stdout.write(self.style.SUCCESS("Done."))
