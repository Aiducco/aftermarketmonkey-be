"""
Backfill ``MasterPartFitment`` for one brand from Summit Racing's applications tab.

A gap filler for brands with no ACES feed -- see
``src/integrations/services/summit_racing_fitment.py`` for what the data is and is not.

Requires a browser-minted ``reese84`` cookie for summitracing.com (Imperva bot protection), via
``--cookie-file``, ``SUMMIT_RACING_COOKIE``, or a repo-root ``summit_racing_cookie.local.txt``
(gitignored) -- the same convention ``backfill_atech_summit_image_urls`` uses. The token cannot
be minted from Python; copy any summitracing.com request out of browser DevTools as cURL and
keep its ``Cookie:`` header. It is **not** bound to the IP that minted it, which is what lets
this fan out across proxy exits.

Usage::

    # bounded smoke test, no writes
    python3 manage.py sync_master_part_fitments_from_summit \
        --brand-id 5715 --sku-prefix bdx- --limit 25 --dry-run

    # full brand sweep
    python3 manage.py sync_master_part_fitments_from_summit \
        --brand-id 5715 --sku-prefix bdx- --workers 8
"""

from __future__ import annotations

import logging
import os
import pathlib
import time

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

import src.models as src_models
from src.integrations.clients.summit_racing import client as summit_client
from src.integrations.services import summit_racing_fitment

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[summit-racing-fitment]"


def _load_cookie(cookie_file: str | None) -> str:
    if cookie_file:
        return pathlib.Path(cookie_file).read_text(encoding="utf-8").strip()
    env = (os.environ.get("SUMMIT_RACING_COOKIE") or "").strip()
    if env:
        return env
    default_path = pathlib.Path(str(settings.BASE_DIR)) / "summit_racing_cookie.local.txt"
    if default_path.is_file():
        cookie = default_path.read_text(encoding="utf-8").strip()
        if cookie:
            return cookie
    raise CommandError(
        "No cookie: put one in summit_racing_cookie.local.txt at the project root, "
        "set SUMMIT_RACING_COOKIE, or pass --cookie-file."
    )


class Command(BaseCommand):
    help = (
        "Backfill MasterPartFitment for one brand by scraping Summit Racing's applications tab. "
        "Cookie: --cookie-file, SUMMIT_RACING_COOKIE, or summit_racing_cookie.local.txt."
    )

    def add_arguments(self, parser):
        parser.add_argument("--brand-id", type=int, default=None, help="Brands.id to sweep.")
        parser.add_argument("--brand-name", type=str, default=None, help="Exact Brands.name, if id is unknown.")
        parser.add_argument(
            "--sku-prefix",
            type=str,
            required=True,
            help="Summit brand-code prefix including the dash, e.g. 'bdx-' for BDS.",
        )
        parser.add_argument("--workers", type=int, default=8, help="Parallel workers; each uses its own exit IP.")
        parser.add_argument(
            "--pace-seconds",
            type=float,
            default=1.5,
            help=(
                "Minimum gap between requests *per worker*. Aggregate rate is workers/pace. "
                "Imperva throttles per exit IP, so lower this only if you also add gateways."
            ),
        )
        parser.add_argument("--timeout", type=float, default=60.0, help="Per-request HTTP timeout.")
        parser.add_argument("--limit", type=int, default=None, help="Stop after this many resolved parts.")
        parser.add_argument(
            "--max-pages-per-part",
            type=int,
            default=40,
            help="Cap pages per part (25 rows each). 40 covers every part observed; raise for outliers.",
        )
        parser.add_argument("--batch-size", type=int, default=200, help="Fitment rows per upsert.")
        parser.add_argument(
            "--include-existing",
            action="store_true",
            help="Also sweep parts that already have fitment (default: only parts with none).",
        )
        parser.add_argument("--cookie-file", type=str, default=None, help="File holding the raw Cookie header.")
        parser.add_argument(
            "--sku-cache",
            type=str,
            default=None,
            help="Path to cache the sitemap SKU harvest (default: <BASE_DIR>/data/summit_skus_<prefix>.txt).",
        )
        parser.add_argument("--refresh-sku-index", action="store_true", help="Re-harvest the sitemap SKU index.")
        parser.add_argument(
            "--provider-kind",
            type=int,
            default=None,
            help="Attribute rows to this Providers.kind (default: NULL source_provider).",
        )
        parser.add_argument("--no-proxy", action="store_true", help="Go direct instead of through StormProxies.")
        parser.add_argument("--dry-run", action="store_true", help="Fetch and parse but write nothing.")

    def handle(self, *args, **options):
        brand_id = options["brand_id"]
        if brand_id is None:
            if not options["brand_name"]:
                raise CommandError("Pass --brand-id or --brand-name.")
            brands = list(src_models.Brands.objects.filter(name=options["brand_name"]).values_list("id", flat=True))
            if len(brands) != 1:
                raise CommandError(
                    "Expected exactly one brand named {!r}, found {}.".format(options["brand_name"], len(brands))
                )
            brand_id = brands[0]

        cookie_header = _load_cookie(options["cookie_file"])
        gateways = [""] if options["no_proxy"] else (summit_client.configured_gateways() or [""])

        prefix = options["sku_prefix"].lower()
        cache_path = pathlib.Path(
            options["sku_cache"]
            or (pathlib.Path(str(settings.BASE_DIR)) / "data" / "summit_skus_{}.txt".format(prefix.strip("-")))
        )

        started = time.monotonic()
        banner = (
            "{} starting brand_id={} prefix={} workers={} pace={}s gateways={} only_missing={} "
            "dry_run={}{}".format(
                _LOG_PREFIX,
                brand_id,
                prefix,
                options["workers"],
                options["pace_seconds"],
                len(gateways),
                not options["include_existing"],
                options["dry_run"],
                " limit={}".format(options["limit"]) if options["limit"] else "",
            )
        )
        self.stdout.write(banner)
        logger.info(banner)

        result = summit_racing_fitment.sync_master_part_fitments_from_summit(
            brand_id=brand_id,
            cookie_header=cookie_header,
            sku_prefix=prefix,
            sku_cache_path=cache_path,
            refresh_sku_index=options["refresh_sku_index"],
            gateways=gateways,
            workers=max(1, options["workers"]),
            pace_seconds=options["pace_seconds"],
            timeout=options["timeout"],
            only_missing=not options["include_existing"],
            limit=options["limit"],
            max_pages_per_part=options["max_pages_per_part"],
            batch_size=options["batch_size"],
            provider_kind=options["provider_kind"],
            dry_run=options["dry_run"],
            on_progress=self.stdout.write,
        )

        elapsed = time.monotonic() - started
        message = "{} done in {:.1f}s || {}".format(_LOG_PREFIX, elapsed, result.summary())
        logger.info(message)
        if result.stopped_early_reason:
            self.stdout.write(self.style.WARNING(message))
            raise CommandError("Sweep stopped early: {}".format(result.stopped_early_reason))
        self.stdout.write(self.style.SUCCESS(message))
