"""
One-off import of AAM/PACE's Meilisearch taxonomy for Tri-State's storefront into
pace_tse_categories / pace_tse_brands / pace_tse_series.

Standalone on purpose -- this shares no code and no tables with the Tri-State distributor
integration (the PACE Customer API client, TSEBrand, tristate_parts) and nothing there reads
what this writes. Its own endpoint, its own key, its own proxy handling, all local to this file,
so it can be changed or deleted without touching anything that serves live pricing.

Backend is Meilisearch behind nginx at ``https://search.theaamgroup.com``, the same host
etristate.com's own storefront queries client-side. Confirmed live 2026-10-21:

  - Three sibling indexes exist alongside ``PACE_TSE_parts``: ``PACE_TSE_categories`` (718 docs),
    ``PACE_TSE_brands`` (670), ``PACE_TSE_series`` (9,396).
  - Responses carry exact ``totalHits``/``totalPages`` (not ``estimatedTotalHits``), so paging is
    a plain 1-based ``page``/``hitsPerPage`` walk -- no bucketing workaround needed.
  - Every document embeds a full ``parts`` array of that node's part numbers.

TWO THINGS THAT WILL BITE if this is edited:

1. The host is unreachable from the office IP -- a direct request just times out, no response at
   all. Every call MUST go through a StormProxies gateway, whose exit IP is a fresh datacenter
   address per request. The calling IP also has to be whitelisted in the StormProxies panel.
2. ``STORM_PROXY_ACCOUNT_NAME`` is an email address, so the ``@`` in it breaks any proxy URL
   built by plain interpolation -- the URL then parses as host ``gmail.com`` and requests fails
   with a DNS error that reads like a dead proxy. The username is percent-encoded below.

Roughly 1 in 10 requests dies as a connection timeout rather than an HTTP error, so each page is
retried on the next gateway in rotation before the run gives up.

    python manage.py import_pace_tse_taxonomy
    python manage.py import_pace_tse_taxonomy --index brands --dry-run
"""
import logging
import time
import typing
import urllib.parse

import pgbulk
import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[IMPORT-PACE-TSE-TAXONOMY]"

_SEARCH_BASE_URL = "https://search.theaamgroup.com"

# The storefront's own public search key, exposed client-side by etristate.com to every site
# visitor's browser -- a read-only search credential, not a distributor API key.
_SEARCH_API_KEY = "e1aa20da300595b7e34652a96ae9eefd4b98a5e73e514582da38f0c148a79ecf"

# nginx in front of Meilisearch 403s anything it does not take for the real storefront, so the
# browser's header set is sent verbatim rather than trimmed to what Meilisearch itself needs.
_BROWSER_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"
    ),
    "origin": "https://etristate.com",
    "referer": "https://etristate.com/",
    "content-type": "application/json",
    "accept": "*/*",
    "sec-fetch-site": "cross-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}

# All four StormProxies gateways, rotated per request. Overridable via PACE_TSE_PROXY_GATEWAYS
# (comma-separated host:port) without touching this file.
_DEFAULT_GATEWAYS = (
    "37.48.118.90:13081",
    "83.149.70.159:13081",
    "108.59.14.203:13081",
    "108.59.14.208:13081",
)

# Documents carry full parts arrays, so pages are payload-heavy -- 100 keeps a page well under a
# megabyte while still making the largest index (series) only ~94 requests.
_HITS_PER_PAGE = 100
_REQUEST_TIMEOUT_SECONDS = 30
_MAX_ATTEMPTS_PER_PAGE = 6
_UPSERT_BATCH = 1000

# A page that comes back empty ends the walk even if totalPages claimed more, so a shrinking
# index cannot spin this forever.
_MAX_PAGES = 1000


def _gateways() -> list[str]:
    raw = getattr(settings, "PACE_TSE_PROXY_GATEWAYS", "") or ""
    gateways = [g.strip() for g in raw.split(",") if g.strip()]
    return gateways or list(_DEFAULT_GATEWAYS)


def _proxy_url(gateway: str) -> str:
    """Build a proxy URL with the credentials percent-encoded -- see this module's docstring for
    why plain interpolation silently breaks here."""
    username = getattr(settings, "STORM_PROXY_ACCOUNT_NAME", "") or ""
    password = getattr(settings, "STORM_PROXY_PASSWORD", "") or ""
    host = gateway.replace("http://", "").replace("https://", "")
    if not (username and password):
        return "http://{}".format(host)
    return "http://{}:{}@{}".format(
        urllib.parse.quote(username, safe=""),
        urllib.parse.quote(password, safe=""),
        host,
    )


def _search_page(
    session: requests.Session,
    index: str,
    page: int,
    gateways: list[str],
    page_counter: int,
) -> dict:
    """Fetch one page of one index, rotating to the next gateway on each failed attempt.

    Gateways are offset by ``page_counter`` so consecutive pages do not all start on the same
    gateway -- spreading the load keeps one slow gateway from pacing the whole run.
    """
    url = "{}/indexes/{}/search".format(_SEARCH_BASE_URL, index)
    body = {"q": "", "hitsPerPage": _HITS_PER_PAGE, "page": page}

    last_error: Exception | None = None
    for attempt in range(_MAX_ATTEMPTS_PER_PAGE):
        gateway = gateways[(page_counter + attempt) % len(gateways)]
        proxy_url = _proxy_url(gateway)
        try:
            response = session.post(
                url,
                headers={"Authorization": "Bearer {}".format(_SEARCH_API_KEY), **_BROWSER_HEADERS},
                json=body,
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            last_error = e
            logger.warning(
                "%s %s page %s attempt %s/%s via %s failed: %s",
                _LOG_PREFIX, index, page, attempt + 1, _MAX_ATTEMPTS_PER_PAGE, gateway, e,
            )
            # Backs off gently: the failure mode is a dead exit IP, which the next gateway's
            # fresh exit IP usually fixes immediately, so this is not a rate-limit backoff.
            time.sleep(1.0 + attempt)

    raise CommandError(
        "{} page {} failed after {} attempts across {} gateways: {}".format(
            index, page, _MAX_ATTEMPTS_PER_PAGE, len(gateways), last_error
        )
    )


def _fetch_index(session: requests.Session, index: str, stdout) -> list[dict]:
    """Walk every page of one index. Meilisearch's ``page`` is 1-based here."""
    gateways = _gateways()
    hits: list[dict] = []
    page = 1
    total_pages: int | None = None

    while page <= _MAX_PAGES:
        payload = _search_page(session, index, page, gateways, page_counter=page)
        batch = payload.get("hits") or []
        if not batch:
            break

        hits.extend(batch)
        if total_pages is None:
            total_pages = payload.get("totalPages")
            stdout.write(
                "  {}: {} docs across {} pages".format(
                    index, payload.get("totalHits", "?"), total_pages or "?"
                )
            )

        if total_pages is not None and page >= total_pages:
            break
        page += 1

    return hits


def _str(hit: dict, key: str) -> str:
    """Coerce one field to a non-null string -- the models use empty-string defaults rather than
    nullable text, and these documents omit keys entirely rather than sending null."""
    value = hit.get(key)
    return "" if value is None else str(value)


def _list(hit: dict, key: str) -> list:
    value = hit.get(key)
    return value if isinstance(value, list) else []


def _category_instance(hit: dict) -> src_models.PaceTseCategory | None:
    object_id = _str(hit, "objectID")
    if not object_id:
        return None
    return src_models.PaceTseCategory(
        object_id=object_id,
        name=_str(hit, "name")[:255],
        slug=_str(hit, "slug")[:255],
        parent_name=_str(hit, "parent_name")[:255],
        parent_slug=_str(hit, "parent_slug")[:255],
        parts=_list(hit, "parts"),
        series=_list(hit, "series"),
        brands=_list(hit, "brands"),
        site_id=_str(hit, "site_id")[:16],
        member_code=_str(hit, "member_code")[:16],
        group_name=_str(hit, "group_name")[:32],
    )


def _brand_instance(hit: dict) -> src_models.PaceTseBrand | None:
    object_id = _str(hit, "objectID")
    if not object_id:
        return None
    # This index's objectID is itself the AAIA brand code; fall back to it if the explicit
    # field is ever missing, since aaia_brand_code is the column anything else would join on.
    return src_models.PaceTseBrand(
        object_id=object_id,
        aaia_brand_code=(_str(hit, "aaia_brand_code") or object_id)[:8],
        name=_str(hit, "name")[:255],
        slug=_str(hit, "slug")[:255],
        image=_str(hit, "image"),
        parts=_list(hit, "parts"),
        categories=_list(hit, "categories"),
        subcategories=_list(hit, "subcategories"),
        series=_list(hit, "series"),
        site_id=_str(hit, "site_id")[:16],
        member_code=_str(hit, "member_code")[:16],
        group_name=_str(hit, "group_name")[:32],
    )


def _series_instance(hit: dict) -> src_models.PaceTseSeries | None:
    object_id = _str(hit, "objectID")
    if not object_id:
        return None
    return src_models.PaceTseSeries(
        object_id=object_id,
        name=_str(hit, "name")[:255],
        slug=_str(hit, "slug")[:255],
        brand=_str(hit, "brand")[:255],
        aaia_brand_code=_str(hit, "aaia_brand_code")[:8],
        brand_slug=_str(hit, "brand_slug")[:255],
        parts=_list(hit, "parts"),
        categories=_list(hit, "categories"),
        subcategories=_list(hit, "subcategories"),
        site_id=_str(hit, "site_id")[:16],
        member_code=_str(hit, "member_code")[:16],
        group_name=_str(hit, "group_name")[:32],
    )


# index name -> (model, hit mapper, columns to overwrite on conflict)
_SPECS: dict[str, tuple] = {
    "categories": (
        "PACE_TSE_categories",
        src_models.PaceTseCategory,
        _category_instance,
        ["name", "slug", "parent_name", "parent_slug", "parts", "series", "brands",
         "site_id", "member_code", "group_name", "updated_at"],
    ),
    "brands": (
        "PACE_TSE_brands",
        src_models.PaceTseBrand,
        _brand_instance,
        ["aaia_brand_code", "name", "slug", "image", "parts", "categories", "subcategories",
         "series", "site_id", "member_code", "group_name", "updated_at"],
    ),
    "series": (
        "PACE_TSE_series",
        src_models.PaceTseSeries,
        _series_instance,
        ["name", "slug", "brand", "aaia_brand_code", "brand_slug", "parts", "categories",
         "subcategories", "site_id", "member_code", "group_name", "updated_at"],
    ),
}


def _upsert(model, instances: list, update_fields: list[str]) -> int:
    """Bulk upsert on object_id, so re-running the command is idempotent rather than duplicating."""
    written = 0
    for start in range(0, len(instances), _UPSERT_BATCH):
        batch = instances[start:start + _UPSERT_BATCH]
        pgbulk.upsert(
            model,
            batch,
            unique_fields=["object_id"],
            update_fields=update_fields,
            returning=False,
        )
        written += len(batch)
    return written


class Command(BaseCommand):
    help = (
        "One-off import of AAM/PACE's Meilisearch taxonomy (categories, brands, series) for "
        "Tri-State's storefront into pace_tse_categories / pace_tse_brands / pace_tse_series. "
        "Requires the StormProxies gateways -- the host is unreachable directly. Not scheduled."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--index",
            action="append",
            choices=sorted(_SPECS),
            help="Import only this index (repeatable). Defaults to all three.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and report counts without writing to the database.",
        )

    def handle(self, *args, **options):
        selected = options["index"] or sorted(_SPECS)
        dry_run = options["dry_run"]
        gateways = _gateways()

        self.stdout.write(
            "Importing {} from {} via {} proxy gateway(s){}".format(
                ", ".join(selected), _SEARCH_BASE_URL, len(gateways), " [DRY RUN]" if dry_run else ""
            )
        )

        session = requests.Session()
        totals: list[str] = []

        for key in selected:
            index_name, model, to_instance, update_fields = _SPECS[key]

            hits = _fetch_index(session, index_name, self.stdout)

            instances = []
            skipped = 0
            for hit in hits:
                instance = to_instance(hit)
                if instance is None:
                    skipped += 1
                    continue
                instances.append(instance)

            if skipped:
                self.stdout.write(
                    self.style.WARNING("  {}: skipped {} doc(s) with no objectID".format(index_name, skipped))
                )

            if dry_run:
                self.stdout.write("  {}: {} ready (not written)".format(index_name, len(instances)))
                totals.append("{}={} (dry run)".format(key, len(instances)))
                continue

            written = _upsert(model, instances, update_fields)
            self.stdout.write(
                self.style.SUCCESS("  {}: {} upserted into {}".format(index_name, written, model._meta.db_table))
            )
            totals.append("{}={}".format(key, written))

        logger.info("%s done: %s", _LOG_PREFIX, ", ".join(totals))
        self.stdout.write(self.style.SUCCESS("Done: {}".format(", ".join(totals))))
