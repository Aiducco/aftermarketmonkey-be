"""
Import a PACE member's parts index into pace_<member>_parts by slicing the catalog into distinct
price points. FALLBACK importer -- prefer import_pace_parts_by_id, which is both more complete
and cheaper; this exists for the brands that one cannot reach.

Member-agnostic -- ``--member`` selects which registered members to run, all if omitted. See
_pace_search.py for the member registry, the proxy requirement and the 1,000-result cap.

WHEN TO REACH FOR THIS INSTEAD OF --by-id
-----------------------------------------
import_pace_parts_by_id needs a part-number list, which it gets from the member's brands index.
Two cases leave it empty-handed, and this command is the answer to both:

  - A brand code present in a parts index but absent from its brands index. TSE's parts index
    carries 723 distinct brand codes while its brands index holds 670, so 53 codes (~179,862
    documents, largest ``A6LB`` at 129,614) have no id list at all.
  - Parts missing from a brands document's ``parts`` array, which is not complete -- 42% of the
    Covercraft parts reachable by other means are absent from it.

WHAT IT COSTS, measured on TSE
------------------------------
    WeatherTech (BHTJ)  363 price points,  5 over cap -> 21,373 of 21,738   98%
    Covercraft  (BBLQ)  245 price points, 16 over cap -> 27,219 of 140,442  19%

Coverage depends entirely on how spread out a brand's prices are, and it only ever sees priced
parts: 76% of TSE's index has no ``price`` field at all (NOT price 0 -- the field is absent, so
``price > 0`` acts as "has a price" and ``price = 0`` matches nothing).

THE SLICING
-----------
1. The ``price`` facet gives an exact count per distinct price, and a part has exactly one
   price, so price points partition the priced catalog with no overlap. Each under-cap point is
   a single request at hitsPerPage 1000.
2. Over-cap points are split by ``years``. Inventory was evaluated and rejected: ~95% of this
   catalog is ``inventory = 0``, so that bucket stays over the cap and recovers nothing.
3. Plus an explicit ``aces != 1`` slice, because only ``aces == 1`` documents carry ``years`` --
   year slices alone silently drop the rest (19 parts at WeatherTech's $59.95).
4. A point that is over cap with nothing to split on keeps its first 1,000 and the shortfall is
   counted and reported. For WeatherTech's $55.2075 and $179.01 every facetable attribute was
   probed and all were either absent or a single over-cap bucket, so this loss is unavoidable,
   not a missing idea.

Year slices overlap (a part lists every year it fits), hence the dedupe by objectID. Each price
point is upserted as it lands, so an interrupted run keeps what it already fetched.

    python manage.py import_pace_parts --member TSE --brand A6LB
    python manage.py import_pace_parts --member DPN --dry-run
"""
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from django import db as django_db
from django.core.management.base import BaseCommand, CommandError

from src.management.commands import _pace_search as pace

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[IMPORT-PACE-PARTS]"


def _brand_filter(code: str) -> str:
    return 'aaia_brand_code = "{}"'.format(code)


def _fetch_price_point(searcher, code, price, stats, emit) -> dict[str, dict]:
    """Every reachable part at one price point, deduped by objectID.

    Scoped to one price point so the caller can write it immediately. Dedupe only needs to be
    local: a part has exactly one price, so price points never overlap each other -- it is the
    year slices within a point that do.
    """
    point_filter = "{} AND price = {}".format(_brand_filter(code), price)
    hits, total = searcher.slice_hits(point_filter)
    collected = {h["objectID"]: h for h in hits}

    if total <= pace.MAX_TOTAL_HITS:
        return collected

    stats["over_cap"] += 1

    # Yearless parts first -- the year slices below cannot see them at all.
    nonaces_hits, _ = searcher.slice_hits("{} AND aces != 1".format(point_filter))
    for hit in nonaces_hits:
        collected[hit["objectID"]] = hit

    aces_filter = "{} AND aces = 1".format(point_filter)
    years = searcher.facet(aces_filter, "years")

    if not years:
        stats["truncated_points"] += 1
        stats["lost"] += max(0, total - len(collected))
        emit("    price {}: {} parts, nothing to split on -- keeping {}".format(
            price, total, len(collected)), warn=True)
        return collected

    for year in sorted(years, key=lambda y: -years[y]):
        year_hits, year_total = searcher.slice_hits("{} AND years = {}".format(aces_filter, year))
        for hit in year_hits:
            collected[hit["objectID"]] = hit
        if year_total > pace.MAX_TOTAL_HITS:
            stats["lost"] += year_total - pace.MAX_TOTAL_HITS
            logger.warning("%s %s price %s year %s has %s parts -- keeping first %s",
                           _LOG_PREFIX, code, price, year, year_total, pace.MAX_TOTAL_HITS)

    if len(collected) < total:
        stats["lost"] += total - len(collected)
    return collected


def _process_brand(member, code, name, spacing, dry_run, emit) -> dict:
    result = {"member": member.code, "code": code, "name": name, "written": 0, "expected": 0,
              "price_points": 0, "over_cap": 0, "truncated_points": 0, "lost": 0,
              "requests": 0, "error": ""}
    stats = {"over_cap": 0, "truncated_points": 0, "lost": 0}
    searcher = pace.thread_searcher(member.index("parts"), spacing)
    before = searcher.request_count

    try:
        price_points = searcher.facet(
            "{} AND price > 0".format(_brand_filter(code)), "price")
        if not price_points:
            emit("  {} {} {}: no priced parts".format(member.code, code, name), warn=True)
            return result

        result["price_points"] = len(price_points)
        result["expected"] = sum(price_points.values())
        if len(price_points) >= pace.MAX_TOTAL_HITS:
            emit("  {} {}: price facet returned {} values, the per-facet cap -- there may be "
                 "more price points than this".format(member.code, code, len(price_points)),
                 warn=True)

        # Largest first, so over-cap points surface early in the log.
        for price, _count in sorted(price_points.items(), key=lambda kv: -kv[1]):
            collected = _fetch_price_point(searcher, code, price, stats, emit)
            if not collected:
                continue
            instances = [i for i in (pace.part_instance(member, h) for h in collected.values())
                         if i is not None]
            result["written"] += (
                len(instances) if dry_run
                else pace.upsert(member.part_model, instances, pace.PART_UPDATE_FIELDS))

    except Exception as e:  # noqa: BLE001 -- one brand must not abort the run
        result["error"] = str(e)
        logger.exception("%s %s/%s failed", _LOG_PREFIX, member.code, code)
    finally:
        django_db.connections.close_all()
        result["requests"] = searcher.request_count - before

    result.update(**stats)
    return result


class Command(BaseCommand):
    help = (
        "Import PACE members' parts indexes into pace_<member>_parts by price-point slicing. "
        "Fallback for brands import_pace_parts_by_id cannot reach (no brands-index row, or "
        "part numbers missing from its parts array). Requires the StormProxies gateways."
    )

    def add_arguments(self, parser):
        pace.add_member_argument(parser)
        parser.add_argument("--brand", action="append", help="AAIA brand code (repeatable).")
        parser.add_argument("--all-brands", action="store_true",
                            help="Every brand in the member's brands table.")
        parser.add_argument("--dry-run", action="store_true", help="Fetch and report, write nothing.")
        parser.add_argument("--workers", type=int, default=pace.DEFAULT_WORKERS,
                            help="Brands in parallel (default {}).".format(pace.DEFAULT_WORKERS))
        parser.add_argument("--sleep", type=float, default=pace.DEFAULT_REQUEST_SPACING_SECONDS,
                            help="Minimum gap between requests, PER WORKER (default {}).".format(
                                pace.DEFAULT_REQUEST_SPACING_SECONDS))

    def handle(self, *args, **options):
        members = pace.resolve_members(options["member"])
        dry_run = options["dry_run"]
        spacing = max(0.0, options["sleep"])
        workers = max(1, options["workers"])
        explicit = [c.strip().upper() for c in (options["brand"] or []) if c and c.strip()]

        if not explicit and not options["all_brands"]:
            raise CommandError("pass --brand <AAIA code> (repeatable) or --all-brands")

        jobs = []
        for member in members:
            rows = {
                b.aaia_brand_code: b.name
                for b in member.brand_model.objects.only("aaia_brand_code", "name")
            }
            # Unlike the by-id command, an unknown code is fine here: price slicing needs no
            # part-number list, which is exactly why this is the orphan-brand path.
            codes = explicit or sorted(rows)
            for code in dict.fromkeys(codes):
                jobs.append((member, code, rows.get(code, "")))

        total = len(jobs)
        started = time.monotonic()
        lock = threading.Lock()
        done = {"n": 0, "parts": 0}

        def emit(message: str, warn: bool = False) -> None:
            with lock:
                self.stdout.write(self.style.WARNING(message) if warn else message)
                self.stdout.flush()

        def finish(r: dict) -> None:
            with lock:
                done["n"] += 1
                done["parts"] += r["written"]
                elapsed = time.monotonic() - started
                rate = done["n"] / elapsed if elapsed else 0
                eta = (total - done["n"]) / rate if rate else 0
                if r["error"]:
                    line = "[{}/{}] {} {} FAILED: {}".format(
                        done["n"], total, r["member"], r["code"], r["error"][:110])
                    style = self.style.ERROR
                else:
                    line = "[{}/{}] {} {} {}: {} of {} priced parts".format(
                        done["n"], total, r["member"], r["code"], r["name"],
                        format(r["written"], ","), format(r["expected"], ","))
                    if r["over_cap"]:
                        line += " ({} over-cap".format(r["over_cap"])
                        if r["truncated_points"]:
                            line += ", {} truncated".format(r["truncated_points"])
                        line += ")"
                    if r["lost"]:
                        line += " ~{} unreachable".format(r["lost"])
                    style = self.style.SUCCESS
                line += "  |  {} parts, {:.0f}m elapsed, ~{:.0f}m left".format(
                    format(done["parts"], ","), elapsed / 60, eta / 60)
                self.stdout.write(style(line))
                self.stdout.flush()

        self.stdout.write("Importing {} brand job(s) across {} member(s), {} worker(s), "
                          "{}s/request{}".format(total, len(members), workers, spacing,
                                                 " [DRY RUN]" if dry_run else ""))
        self.stdout.flush()

        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_process_brand, m, c, n, spacing, dry_run, emit)
                       for m, c, n in jobs]
            for future in as_completed(futures):
                r = future.result()
                results.append(r)
                finish(r)

        ok = [r for r in results if not r["error"]]
        failed = [r for r in results if r["error"]]
        parts = sum(r["written"] for r in ok)
        expected = sum(r["expected"] for r in ok)
        lost = sum(r["lost"] for r in ok)
        elapsed = time.monotonic() - started

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            "Done: {} part(s) {} in {:.0f}m, {} requests".format(
                format(parts, ","), "found (dry run)" if dry_run else "upserted",
                elapsed / 60, format(sum(r["requests"] for r in results), ","))))
        if expected:
            self.stdout.write("  coverage: {} of {} priced parts ({:.1f}%)".format(
                format(parts, ","), format(expected, ","), 100 * parts / expected))
        if lost:
            self.stdout.write(self.style.WARNING(
                "  ~{} part(s) unreachable behind the {}-result cap".format(
                    format(lost, ","), pace.MAX_TOTAL_HITS)))
        if failed:
            self.stdout.write(self.style.ERROR("  {} brand job(s) failed:".format(len(failed))))
            for r in failed[:20]:
                self.stdout.write(self.style.ERROR("    {} {}: {}".format(
                    r["member"], r["code"], r["error"][:150])))
        self.stdout.write("  note: priced parts only -- documents with no `price` field are "
                          "invisible to this strategy (76% of the TSE index).")
        self.stdout.flush()
