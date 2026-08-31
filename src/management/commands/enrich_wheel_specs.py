"""
Populate ``wheel_specs`` from the structured wheel feeds.

See ``src/integrations/services/wheel_enrichment.py`` for the machinery and
``src/domain/wheel_size.py`` for the parser that canonicalises bolt patterns.

**No LLM is called.** Unlike the tire pipeline, every field here comes from a feed column or a
rule you can read, so a dry run costs nothing and the whole catalog can be rebuilt in a minute.

    manage.py enrich_wheel_specs                              # every feed, report only
    manage.py enrich_wheel_specs --feeds thewheelgroup        # one feed
    manage.py enrich_wheel_specs --apply                      # commit

A master part described by two feeds is resolved by ``FEED_ORDER``, not by whichever ran last, and
every collision is reported. There are none today; the rule exists because catalogs grow.
"""
from django.core.management.base import BaseCommand, CommandError

from src.integrations.services import tire_enrichment, wheel_enrichment


class Command(BaseCommand):
    help = "Build wheel_specs from the Wheel Pros structured feed. Read-only unless --apply is given."

    def add_arguments(self, parser):
        parser.add_argument(
            "--feeds",
            default=None,
            help="Comma-separated feeds to read. Default: all, in precedence order ({}).".format(
                ", ".join(wheel_enrichment.FEED_ORDER)
            ),
        )
        parser.add_argument("--brands", default=None, help="Comma-separated brand names. Omit for all.")
        parser.add_argument("--limit", type=int, default=None, help="Max master parts to process.")
        parser.add_argument("--apply", action="store_true", help="Write wheel_specs.")

    def handle(self, *args, **options):
        brand_ids = None
        if options["brands"]:
            names = [n.strip() for n in options["brands"].split(",") if n.strip()]
            resolved = tire_enrichment.resolve_brand_ids(names)
            unknown = sorted(set(names) - set(resolved))
            if unknown:
                raise CommandError("Unknown brand(s): {}".format(", ".join(unknown)))
            brand_ids = list(resolved.values())

        feeds = None
        if options["feeds"]:
            feeds = [f.strip() for f in options["feeds"].split(",") if f.strip()]
            unknown = sorted(set(feeds) - set(wheel_enrichment.FEEDS))
            if unknown:
                raise CommandError(
                    "Unknown feed(s): {}. Known: {}".format(
                        ", ".join(unknown), ", ".join(sorted(wheel_enrichment.FEEDS))
                    )
                )

        stats = wheel_enrichment.run(
            feeds=feeds, brand_ids=brand_ids, limit=options["limit"], apply_changes=options["apply"]
        )

        self.stdout.write("\nScanned                   {}".format(stats.scanned))
        built = stats.built
        self.stdout.write("  specs built             {}".format(built))
        for name in wheel_enrichment.FEED_ORDER:
            if name in stats.per_feed:
                self.stdout.write("    {:<22}{}".format(name, stats.per_feed[name]))
        if stats.collisions:
            self.stdout.write(
                self.style.WARNING("  described by 2 feeds    {}  (resolved by precedence)".format(stats.collisions))
            )
            for detail, count in sorted(stats.collision_detail.items(), key=lambda kv: -kv[1])[:6]:
                self.stdout.write("      {:<40}{}".format(detail, count))
        for reason, count in sorted(stats.skipped.items()):
            self.stdout.write(self.style.WARNING("  skipped: {:<34} {}".format(reason, count)))

        if built:
            self.stdout.write("\nCoverage")
            for label, value in (
                ("bolt pattern", built - stats.no_bolt_pattern - stats.blank_drilled),
                ("offset", stats.with_offset),
                ("finish family", stats.with_finish_family),
                ("model name", stats.with_model_name),
            ):
                self.stdout.write("  {:<24}{:>7}  {:5.1f}%".format(label, value, 100 * value / built))
            self.stdout.write("  {:<24}{:>7}".format("undrilled (blank)", stats.blank_drilled))
            if stats.size_disputed:
                self.stdout.write(
                    self.style.WARNING(
                        "  {:<24}{:>7}  feed and title disagree; feed kept".format("size disputed", stats.size_disputed)
                    )
                )

        if stats.samples:
            self.stdout.write("\nSamples")
            for line in stats.samples:
                self.stdout.write("  {}".format(line))

        if options["apply"]:
            self.stdout.write(self.style.SUCCESS("\nWritten: {} wheel_specs".format(stats.written)))
            if stats.product_type_set:
                self.stdout.write("  product_type -> wheel   {}".format(stats.product_type_set))
        else:
            self.stdout.write(self.style.WARNING("\nNothing written -- pass --apply to commit."))
