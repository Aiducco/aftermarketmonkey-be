"""
Import a PACE member's parts index into pace_<member>_parts by fetching documents in batches of
``catalog_object_id``. This is the PRIMARY parts importer; import_pace_parts (price slicing) is
the fallback for brands this cannot reach.

Member-agnostic -- ``--member`` selects which registered members to run, all of them if omitted.
See _pace_search.py for the member registry, the proxy requirement and the 1,000-result cap.

WHY FETCHING BY ID, AND NOT BY PRICE
------------------------------------
Every query is capped at 1,000 results, so the whole problem is how to carve a catalog into
sub-1,000 slices. Slicing by distinct price only works when prices are spread out, measured on
TSE:

    WeatherTech (BHTJ)  363 price points,  5 over cap -> 21,373 of 21,738   98%
    Covercraft  (BBLQ)  245 price points, 16 over cap -> 27,219 of 140,442  19%

Covercraft prices by cover-model tier, so 16 price points hold 92% of the brand ($39.14 alone is
37,174 parts) and nothing splits them -- at that price point years, makes, models, category,
subcategory and series are all either absent or a single over-cap bucket, and ~95% of the
catalog is inventory 0. 27,219 was the measured ceiling, not a bug.

Fetching by id sidesteps the cap: N ids yield at most N documents, so the slice size is chosen
rather than discovered. It also reaches parts with no ``price`` field, which no price walk can
ever see -- 1,293,473 of TSE's 1,693,697 documents (76%).

WHERE THE IDS COME FROM, AND THE ONE THING TO WATCH
---------------------------------------------------
``catalog_object_id`` is ``<aaia_brand_code>-<part number>`` and is 1:1 with documents (verified
over 27,219 Covercraft rows: no part number appeared in more than one document). Each
pace_<member>_brands document carries a ``parts`` array, so the id list comes from our own
database with no discovery pass -- run import_pace_taxonomy first.

Those arrays are NOT complete. For Covercraft, 42% of the parts reachable by other means are
absent from it (90,894 listed vs 155,440 documents). So this command's coverage is bounded by
the brand arrays, and what it misses needs import_pace_parts or a prefix sweep. Both the
per-brand and the whole-index shortfall are reported rather than left implicit.

Adaptive splitting is kept as insurance even though the 1:1 check says it should never fire:
when a response's ``totalHits`` exceeds the hits returned, the batch is halved and retried
recursively. Without it a brand that broke the 1:1 assumption would truncate silently.

    python manage.py import_pace_parts_by_id --member DPN
    python manage.py import_pace_parts_by_id --member TSE --brand BHTJ --dry-run
    python manage.py import_pace_parts_by_id                       # every member
"""
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from django import db as django_db
from django.core.management.base import BaseCommand, CommandError

from src.management.commands import _pace_search as pace

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[IMPORT-PACE-PARTS-BY-ID]"

# Below the 1,000 cap on purpose, leaving headroom in case a brand is not 1:1 after all, so the
# adaptive split stays a rare correction rather than a routine cost.
_DEFAULT_BATCH = 400


def _id_filter(ids: list[str]) -> str:
    # Part numbers were checked to contain no quotes or backslashes, but escape anyway rather
    # than trust that for members added later.
    quoted = ['"{}"'.format(i.replace("\\", "\\\\").replace('"', '\\"')) for i in ids]
    return "catalog_object_id IN [{}]".format(", ".join(quoted))


def _fetch_ids(searcher, ids: list[str], stats: dict, emit) -> dict[str, dict]:
    """Fetch one batch, halving and recursing if the response was capped.

    `totalHits` > hits returned is the only reliable truncation signal: a batch of N ids can
    legitimately match more than N documents, so batch size alone proves nothing.
    """
    if not ids:
        return {}

    hits, total = searcher.slice_hits(_id_filter(ids), page_size=pace.MAX_TOTAL_HITS)
    if total <= len(hits):
        return {h["objectID"]: h for h in hits}

    if len(ids) <= 1:
        # A single id matching over the cap cannot be split further.
        stats["unsplittable"] += 1
        stats["lost"] += total - len(hits)
        emit("    {} matches {} documents, over the {} cap -- keeping {}".format(
            ids[0], total, pace.MAX_TOTAL_HITS, len(hits)), warn=True)
        return {h["objectID"]: h for h in hits}

    stats["splits"] += 1
    middle = len(ids) // 2
    collected = _fetch_ids(searcher, ids[:middle], stats, emit)
    collected.update(_fetch_ids(searcher, ids[middle:], stats, emit))
    return collected


def _process_brand(member, code, name, part_numbers, batch_size, spacing, dry_run, emit) -> dict:
    result = {
        "member": member.code, "code": code, "name": name, "written": 0,
        "expected": len(part_numbers), "splits": 0, "unsplittable": 0, "lost": 0,
        "requests": 0, "error": "",
    }
    stats = {"splits": 0, "unsplittable": 0, "lost": 0}
    searcher = pace.thread_searcher(member.index("parts"), spacing)
    before = searcher.request_count

    try:
        for start in range(0, len(part_numbers), batch_size):
            chunk = part_numbers[start:start + batch_size]
            ids = ["{}-{}".format(code, p) for p in chunk]
            collected = _fetch_ids(searcher, ids, stats, emit)
            if not collected:
                continue
            instances = [
                i for i in (pace.part_instance(member, h) for h in collected.values())
                if i is not None
            ]
            # Written per batch, so an interrupted run keeps what it already fetched.
            result["written"] += (
                len(instances) if dry_run
                else pace.upsert(member.part_model, instances, pace.PART_UPDATE_FIELDS)
            )
    except Exception as e:  # noqa: BLE001 -- one brand must not abort the run
        result["error"] = str(e)
        logger.exception("%s %s/%s failed", _LOG_PREFIX, member.code, code)
    finally:
        # Each worker thread opened its own DB connection; leaving them open leaks server-side
        # connections across a multi-hour sweep.
        django_db.connections.close_all()
        result["requests"] = searcher.request_count - before

    result.update(**stats)
    return result


class Command(BaseCommand):
    help = (
        "Import PACE members' parts indexes into pace_<member>_parts by batching "
        "catalog_object_id, with adaptive splitting against the 1,000-result cap. Reaches "
        "unpriced parts, unlike import_pace_parts. Needs import_pace_taxonomy run first."
    )

    def add_arguments(self, parser):
        pace.add_member_argument(parser)
        parser.add_argument("--brand", action="append", help="AAIA brand code (repeatable).")
        parser.add_argument("--dry-run", action="store_true", help="Fetch and report, write nothing.")
        parser.add_argument("--batch", type=int, default=_DEFAULT_BATCH,
                            help="Ids per request (default {}).".format(_DEFAULT_BATCH))
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
        batch_size = max(1, min(pace.MAX_TOTAL_HITS, options["batch"]))
        wanted_brands = [c.strip().upper() for c in (options["brand"] or []) if c and c.strip()]

        jobs = []
        for member in members:
            rows = {
                b.aaia_brand_code: b
                for b in member.brand_model.objects.only("aaia_brand_code", "name", "parts")
            }
            if not rows:
                self.stdout.write(self.style.WARNING(
                    "  {} {}: {} is empty -- run import_pace_taxonomy --member {} first".format(
                        member.code, member.name, member.brand_model._meta.db_table, member.code)))
                continue
            codes = wanted_brands or list(rows)
            missing = [c for c in codes if c not in rows]
            if wanted_brands and missing:
                raise CommandError("{}: no brand row for {}".format(
                    member.code, ", ".join(missing)))
            for code in dict.fromkeys(c for c in codes if c in rows):
                jobs.append((member, code, rows[code].name, sorted(set(rows[code].parts or []))))

        if not jobs:
            raise CommandError("nothing to import -- run import_pace_taxonomy first")

        # Biggest brands first: sizes span 3 to 123,508 part numbers, so starting the longest
        # jobs first stops one huge brand from running alone while the other workers idle.
        jobs.sort(key=lambda j: -len(j[3]))

        total_ids = sum(len(j[3]) for j in jobs)
        started = time.monotonic()
        lock = threading.Lock()
        done = {"n": 0, "docs": 0, "ids": 0}

        def emit(message: str, warn: bool = False) -> None:
            with lock:
                self.stdout.write(self.style.WARNING(message) if warn else message)
                self.stdout.flush()

        def finish(r: dict) -> None:
            with lock:
                done["n"] += 1
                done["docs"] += r["written"]
                done["ids"] += r["expected"]
                elapsed = time.monotonic() - started
                # ETA by id share, not job count: brand sizes differ by four orders of
                # magnitude, so "jobs done / jobs total" would be badly misleading.
                frac = done["ids"] / total_ids if total_ids else 1
                eta = (elapsed / frac - elapsed) if frac else 0
                if r["error"]:
                    line = "[{}/{}] {} {} {} FAILED: {}".format(
                        done["n"], len(jobs), r["member"], r["code"], r["name"], r["error"][:110])
                    style = self.style.ERROR
                else:
                    line = "[{}/{}] {} {} {}: {} docs from {} ids".format(
                        done["n"], len(jobs), r["member"], r["code"], r["name"],
                        format(r["written"], ","), format(r["expected"], ","))
                    if r["splits"]:
                        line += " ({} split)".format(r["splits"])
                    if r["lost"]:
                        line += " ~{} lost".format(r["lost"])
                    style = self.style.SUCCESS
                line += "  |  {} docs, {:.0f}% of ids, {:.0f}m elapsed, ~{:.0f}m left".format(
                    format(done["docs"], ","), 100 * frac, elapsed / 60, eta / 60)
                self.stdout.write(style(line))
                self.stdout.flush()

        self.stdout.write(
            "Importing {} brand job(s) across {} member(s), {} ids, batch {}, {} worker(s), "
            "{}s/request{}".format(len(jobs), len({j[0].code for j in jobs}),
                                   format(total_ids, ","), batch_size, workers, spacing,
                                   " [DRY RUN]" if dry_run else ""))
        self.stdout.flush()

        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(_process_brand, m, c, n, parts, batch_size, spacing, dry_run, emit)
                for m, c, n, parts in jobs
            ]
            for future in as_completed(futures):
                r = future.result()
                results.append(r)
                finish(r)

        self._report(results, started, dry_run)

    def _report(self, results: list[dict], started: float, dry_run: bool) -> None:
        ok = [r for r in results if not r["error"]]
        failed = [r for r in results if r["error"]]
        elapsed = time.monotonic() - started
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            "Done: {} doc(s) {} from {} ids in {:.0f}m, {} requests".format(
                format(sum(r["written"] for r in ok), ","),
                "found (dry run)" if dry_run else "upserted",
                format(sum(r["expected"] for r in ok), ","), elapsed / 60,
                format(sum(r["requests"] for r in results), ","))))

        for code in dict.fromkeys(r["member"] for r in results):
            member = pace.MEMBERS[code]
            rows = [r for r in ok if r["member"] == code]
            docs = sum(r["written"] for r in rows)
            table_rows = member.part_model.objects.count()
            self.stdout.write("  {} {}: {} doc(s) this run, {} row(s) in {}".format(
                code, member.name, format(docs, ","), format(table_rows, ","),
                member.part_model._meta.db_table))

        splits = sum(r["splits"] for r in ok)
        lost = sum(r["lost"] for r in ok)
        if splits:
            self.stdout.write("  {} batch(es) split -- a catalog_object_id mapped to several "
                              "documents, so the 1:1 assumption does not hold everywhere".format(splits))
        if lost:
            self.stdout.write(self.style.WARNING(
                "  ~{} doc(s) unreachable: a single id matching over {} documents".format(
                    format(lost, ","), pace.MAX_TOTAL_HITS)))
        if failed:
            self.stdout.write(self.style.ERROR("  {} brand job(s) failed:".format(len(failed))))
            for r in failed[:20]:
                self.stdout.write(self.style.ERROR("    {} {}: {}".format(
                    r["member"], r["code"], r["error"][:150])))
            self.stdout.write("  re-run those with --member/--brand; the upsert is idempotent.")

        self.stdout.write(
            "  reminder: coverage is bounded by the brands index `parts` arrays, which are "
            "incomplete (42% short on Covercraft). Brand codes present in a parts index but "
            "absent from its brands index cannot be reached here at all."
        )
        self.stdout.flush()
