"""
Temporary: fetch Summit Racing PDP HTML per A-Tech feed line and set ``AtechParts.image_url``.

Requires a browser ``Cookie`` header for ``summitracing.com`` (bot protection). Use, in order:
``--cookie-file``, ``SUMMIT_RACING_COOKIE``, or a repo-root ``summit_racing_cookie.local.txt``
(gitignored) — do not commit secrets.

Usage::

    # Optional: put the cookie in summit_racing_cookie.local.txt (see .gitignore)
    python3 manage.py backfill_atech_summit_image_urls --limit 100 --dry-run
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.db.models import Q

import src.models as src_models

logger = logging.getLogger(__name__)

_SUMMIT_PART_URL = "https://www.summitracing.com/parts/{}"
_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
}


def _summit_slug(feed_part_number: str) -> str:
    return feed_part_number.strip().lower()


def _absolutize_url(url: str) -> str:
    u = (url or "").strip()
    if u.startswith("//"):
        return "https:" + u
    return u


def _extract_first_image_from_html(html: str) -> Optional[str]:
    marker = 'id="part-media-files"'
    pos = html.find(marker)
    if pos != -1:
        gt = html.find(">", pos)
        if gt != -1:
            end = html.find("</script>", gt + 1)
            if end != -1:
                raw = html[gt + 1 : end].strip()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    data = None
                if isinstance(data, list):
                    for item in data:
                        if not isinstance(item, dict):
                            continue
                        mt = str(item.get("MediaType") or "").lower()
                        if mt and mt != "image":
                            continue
                        fn = item.get("FileName") or item.get("ThumbNail")
                        if isinstance(fn, str) and (fn.startswith("http") or fn.startswith("//")):
                            return _absolutize_url(fn)

    m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
    if m:
        return _absolutize_url(m.group(1))

    return None


def _image_check_headers() -> dict:
    return {
        "accept": "image/avif,image/webp,image/*,*/*;q=0.8",
        "user-agent": _DEFAULT_HEADERS["user-agent"],
    }


def _verify_extracted_image_url(
    image_url: str,
    timeout: float,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Confirm the extracted URL responds (HEAD, then GET+stream if needed).
    Returns (image_url, None) on success, or (None, error_message).
    """
    t = max(3.0, min(float(timeout), 30.0))
    headers = _image_check_headers()
    try:
        head = requests.head(
            image_url,
            headers=headers,
            timeout=t,
            allow_redirects=True,
        )
        if head.status_code not in (404, 405, 501):
            head.raise_for_status()
            return image_url, None
    except requests.RequestException as e:
        # Fall through to GET — some hosts omit HEAD or close oddly.
        _err_head = str(e)
    else:
        _err_head = "HEAD status {}".format(head.status_code)

    try:
        with requests.get(
            image_url,
            headers=headers,
            timeout=t,
            allow_redirects=True,
            stream=True,
        ) as resp:
            resp.raise_for_status()
        return image_url, None
    except requests.RequestException as e:
        return None, "image URL check failed ({}): {}".format(_err_head, e)


def _load_cookie(cookie_file: Optional[str]) -> str:
    if cookie_file:
        with open(cookie_file, "r", encoding="utf-8") as f:
            return f.read().strip()
    env = (os.environ.get("SUMMIT_RACING_COOKIE") or "").strip()
    if env:
        return env
    default_path = os.path.join(str(settings.BASE_DIR), "summit_racing_cookie.local.txt")
    if os.path.isfile(default_path):
        with open(default_path, "r", encoding="utf-8") as f:
            cookie = f.read().strip()
        if cookie:
            return cookie
    raise SystemExit(
        "No cookie: use summit_racing_cookie.local.txt at project root, "
        "or set SUMMIT_RACING_COOKIE, or pass --cookie-file."
    )


def _fetch_one(
    cookie_header: str,
    part_id: int,
    feed_part_number: str,
    timeout: float,
    skip_image_check: bool,
) -> Tuple[int, str, Optional[str], Optional[str]]:
    close_old_connections()
    slug = _summit_slug(feed_part_number)
    url = _SUMMIT_PART_URL.format(slug)
    try:
        r = requests.get(
            url,
            headers={**_DEFAULT_HEADERS, "Cookie": cookie_header},
            timeout=timeout,
        )
        if r.status_code == 404:
            return part_id, feed_part_number, None, "HTTP 404"
        r.raise_for_status()
        img = _extract_first_image_from_html(r.text)
        if not img:
            return part_id, feed_part_number, None, "no image in HTML"
        if skip_image_check:
            logger.info(
                "[atech-summit-image] id=%s feed=%s image_url=%s (skip URL check)",
                part_id,
                feed_part_number,
                img[:160],
            )
            return part_id, feed_part_number, img, None
        verified, verr = _verify_extracted_image_url(img, timeout)
        if verr:
            logger.warning(
                "[atech-summit-image] id=%s feed=%s extracted=%s err=%s",
                part_id,
                feed_part_number,
                img[:120],
                verr,
            )
            return part_id, feed_part_number, None, verr
        logger.info(
            "[atech-summit-image] id=%s feed=%s image_ok url=%s",
            part_id,
            feed_part_number,
            (verified or "")[:160],
        )
        return part_id, feed_part_number, verified, None
    except Exception as e:  # noqa: BLE001 — surfaced per-row
        return part_id, feed_part_number, None, str(e)


class Command(BaseCommand):
    help = (
        "TEMP: For each AtechParts row, GET Summit PDP /parts/<feed_part_number lower> and set "
        "image_url from part-media-files JSON (fallback og:image). "
        "Cookie: --cookie-file, SUMMIT_RACING_COOKIE, or summit_racing_cookie.local.txt at project root."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and log only; do not write image_url.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Max rows to process (default: all).",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=50,
            help="Rows per DB flush (also chunk size for parallel fetches).",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=4,
            help="Concurrent HTTP workers per batch (keep low to avoid blocks).",
        )
        parser.add_argument(
            "--sleep-between-batches",
            type=float,
            default=1.0,
            help="Seconds to sleep between batch chunks.",
        )
        parser.add_argument(
            "--request-timeout",
            type=float,
            default=45.0,
            help="HTTP timeout per part page.",
        )
        parser.add_argument(
            "--cookie-file",
            type=str,
            default=None,
            help="Path to file containing raw Cookie header (alternative to SUMMIT_RACING_COOKIE).",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Include rows that already have image_url.",
        )
        parser.add_argument(
            "--skip-image-check",
            action="store_true",
            help="Do not HEAD/GET the extracted image URL (faster; may save dead links).",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        limit = options["limit"]
        batch_size = max(1, int(options["batch_size"]))
        workers = max(1, int(options["workers"]))
        sleep_sec = float(options["sleep_between_batches"])
        timeout = float(options["request_timeout"])
        force = options["force"]
        skip_image_check = options["skip_image_check"]

        msg_start = (
            "[atech-summit-image] Starting: streaming AtechParts from DB in chunks of "
            "{} rows (no full-table load). workers={} dry_run={} force={} skip_image_check={}"
            "{}".format(
                batch_size,
                workers,
                dry_run,
                force,
                skip_image_check,
                " limit={}".format(limit) if limit is not None else "",
            )
        )
        self.stdout.write(msg_start)
        logger.info(msg_start)

        cookie_header = _load_cookie(options.get("cookie_file"))
        self.stdout.write("[atech-summit-image] Cookie loaded; querying database…")
        logger.info("[atech-summit-image] cookie loaded, building queryset")

        base_qs = src_models.AtechParts.objects.order_by("id")
        if not force:
            base_qs = base_qs.filter(Q(image_url__isnull=True) | Q(image_url=""))

        updated_count = 0
        error_count = 0
        skipped_unchanged = 0
        processed = 0
        after_id = 0
        batch_num = 0

        while True:
            if limit is not None and processed >= limit:
                break

            slice_size = batch_size
            if limit is not None:
                slice_size = min(batch_size, limit - processed)

            logger.info(
                "[atech-summit-image] db_fetch begin batch=%s after_id=%s slice_size=%s",
                batch_num + 1,
                after_id,
                slice_size,
            )
            chunk = list(
                base_qs.filter(id__gt=after_id).values("id", "feed_part_number")[:slice_size]
            )
            if not chunk:
                self.stdout.write(
                    "[atech-summit-image] No more rows from DB (after_id={}).".format(after_id)
                )
                logger.info(
                    "[atech-summit-image] db_fetch empty after_id=%s processed=%s",
                    after_id,
                    processed,
                )
                break

            batch_num += 1
            id_first = chunk[0]["id"]
            id_last = chunk[-1]["id"]
            after_id = id_last
            processed += len(chunk)

            chunk_msg = (
                "[atech-summit-image] DB chunk {} loaded: {} row(s), id {}..{} "
                "(processed so far: {})".format(batch_num, len(chunk), id_first, id_last, processed)
            )
            self.stdout.write(chunk_msg)
            logger.info(chunk_msg)

            pending = []
            with ThreadPoolExecutor(max_workers=workers) as ex:
                for row in chunk:
                    pending.append(
                        ex.submit(
                            _fetch_one,
                            cookie_header,
                            row["id"],
                            row["feed_part_number"],
                            timeout,
                            skip_image_check,
                        )
                    )

            to_update: List[src_models.AtechParts] = []
            for fut in as_completed(pending):
                part_id, feed_pn, img_url, err = fut.result()
                if err:
                    error_count += 1
                    logger.warning(
                        "[atech-summit-image] id=%s feed=%s err=%s",
                        part_id,
                        feed_pn,
                        err,
                    )
                    continue
                if not img_url:
                    skipped_unchanged += 1
                    continue
                if dry_run:
                    self.stdout.write(
                        "  [dry-run] id={} {} -> {}".format(part_id, feed_pn, img_url[:80])
                    )
                    updated_count += 1
                    continue
                to_update.append(src_models.AtechParts(id=part_id, image_url=img_url))

            if not dry_run and to_update:
                src_models.AtechParts.objects.bulk_update(to_update, ["image_url"])
                updated_count += len(to_update)
                self.stdout.write(
                    self.style.SUCCESS(
                        "HTTP batch {}: bulk_updated {} row(s) (db id {}..{})".format(
                            batch_num,
                            len(to_update),
                            id_first,
                            id_last,
                        )
                    )
                )
                logger.info(
                    "[atech-summit-image] http_batch=%s bulk_updated=%s id_range=%s..%s",
                    batch_num,
                    len(to_update),
                    id_first,
                    id_last,
                )

            if sleep_sec > 0 and len(chunk) >= slice_size and (limit is None or processed < limit):
                time.sleep(sleep_sec)

        self.stdout.write(
            "Done. updated={} errors={} empty_or_no_image={} dry_run={}".format(
                updated_count,
                error_count,
                skipped_unchanged,
                dry_run,
            )
        )
        logger.info(
            "[atech-summit-image] done updated=%s errors=%s dry_run=%s",
            updated_count,
            error_count,
            dry_run,
        )
