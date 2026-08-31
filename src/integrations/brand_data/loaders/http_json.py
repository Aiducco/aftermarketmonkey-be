"""
Pull records from a JSON endpoint a brand publishes, driven entirely by the registry row.

The brands that have an API mostly have the same API: a URL, a key in a header, a list of objects
somewhere in the response, and paging by page number or offset. That is little enough to express
in config, so this loader exists rather than one module per brand -- a brand whose endpoint does
not fit this shape gets its own loader instead of a new option here, because the fifth pagination
style is where a config-driven fetcher stops being simpler than the code it replaced.

Config::

    {
      "url": "https://dealers.example.com/api/v2/tires",
      "method": "GET",                       # POST supported; "body" is sent as JSON
      "params": {"market": "US"},
      "headers": {"Accept": "application/json"},
      "auth_header": "X-API-Key",            # or "Authorization" with "auth_scheme": "Bearer"
      "records_path": "data.items",          # dotted; omit when the response is a bare list
      "key_path": "sku",                     # the brand's own id within each record, if any
      "page": {"style": "page", "param": "page", "start": 1,
               "size_param": "pageSize", "size": 200, "max_pages": 200},
      "pace_seconds": 0.25,
      "timeout": 60,
      "field_map": { ... }
    }

The credential is named by ``TireBrandSource.credential_setting`` and read from Django settings at
run time. It is never stored in ``config``: this table is read by support, dumped into fixtures
and printed by the reporting command, and a key in it is a key in all three.

Paging stops at the first empty page, at a short page, or at ``max_pages`` -- and hitting
``max_pages`` is a *failure*, not a quiet truncation. A pull that silently stopped at page 200 of
400 leaves a half-catalog that looks exactly like a complete one.
"""
import json
import time
import typing

import requests
from django.conf import settings

from src.integrations.brand_data import base
from src.integrations.brand_data.registry import loader

DEFAULT_TIMEOUT = 60
DEFAULT_PAGE_SIZE = 200
DEFAULT_MAX_PAGES = 200


@loader("http_json")
def load(ctx: base.LoaderContext) -> typing.Iterator[base.SourceRecord]:
    url = ctx.require("url")
    method = str(ctx.config.get("method") or "GET").upper()
    timeout = int(ctx.config.get("timeout") or DEFAULT_TIMEOUT)
    pace = float(ctx.config.get("pace_seconds") or 0)
    records_path = ctx.config.get("records_path") or ""
    key_path = ctx.config.get("key_path") or ""
    page = dict(ctx.config.get("page") or {})
    style = str(page.get("style") or ("page" if page else "none"))

    headers = _headers(ctx)
    produced = 0
    pages_read = 0
    size = int(page.get("size") or DEFAULT_PAGE_SIZE)
    max_pages = int(page.get("max_pages") or DEFAULT_MAX_PAGES)
    cursor = int(page.get("start") if page.get("start") is not None else (1 if style == "page" else 0))

    session = requests.Session()
    try:
        while True:
            params = dict(ctx.config.get("params") or {})
            if style != "none":
                params[str(page.get("param") or ("page" if style == "page" else "offset"))] = cursor
                if page.get("size_param"):
                    params[str(page["size_param"])] = size

            payload = _request(ctx, session, method, url, params, headers, timeout)
            pages_read += 1
            records = _extract(ctx, payload, records_path)
            ctx.progress(f"  page {pages_read}: {len(records)} records")

            for record in records:
                yield base.SourceRecord(
                    payload=record,
                    key=_dig(record, key_path) if key_path else None,
                    label=f"{url} page {pages_read}",
                )
                produced += 1
                if ctx.limit is not None and produced >= ctx.limit:
                    ctx.input_label = f"{url} ({pages_read} page(s), stopped at --limit)"
                    return

            if style == "none" or not records or len(records) < size:
                break
            if pages_read >= max_pages:
                raise base.SourceFetchError(
                    f"{ctx.source.slug}: stopped at max_pages={max_pages} with a full page still "
                    f"coming back. Raise it -- a truncated pull is indistinguishable from a "
                    f"complete one once it is in the table."
                )
            cursor += 1 if style == "page" else size
            if pace:
                time.sleep(pace)
    finally:
        session.close()

    ctx.input_label = f"{url} ({pages_read} page(s))"


def _headers(ctx: base.LoaderContext) -> typing.Dict[str, str]:
    headers = {"Accept": "application/json"}
    headers.update({str(key): str(value) for key, value in (ctx.config.get("headers") or {}).items()})

    setting_name = ctx.source.credential_setting
    if not setting_name:
        return headers
    secret = getattr(settings, setting_name, None) or ""
    if not secret:
        raise base.SourceConfigError(
            f"{ctx.source.slug}: settings.{setting_name} is empty -- the source names it as its credential."
        )
    scheme = ctx.config.get("auth_scheme") or ""
    headers[str(ctx.config.get("auth_header") or "Authorization")] = f"{scheme} {secret}".strip()
    return headers


def _request(
    ctx: base.LoaderContext,
    session: requests.Session,
    method: str,
    url: str,
    params: typing.Dict[str, typing.Any],
    headers: typing.Dict[str, str],
    timeout: int,
) -> typing.Any:
    try:
        if method == "POST":
            response = session.post(
                url, params=params, json=ctx.config.get("body") or {}, headers=headers, timeout=timeout
            )
        else:
            response = session.request(method, url, params=params, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise base.SourceFetchError(f"{ctx.source.slug}: {method} {url} failed: {exc}") from exc

    if response.status_code >= 400:
        # The body, trimmed, because these endpoints answer 200-with-an-error and
        # 403-with-an-explanation about equally often and the status alone says neither.
        raise base.SourceFetchError(
            f"{ctx.source.slug}: {method} {url} returned {response.status_code}: {response.text[:400]}"
        )

    ctx.observe(response.content)
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise base.SourceFetchError(
            f"{ctx.source.slug}: {url} did not return JSON (got {response.headers.get('Content-Type', 'no content type')}): "
            f"{response.text[:200]}"
        ) from exc


def _extract(ctx: base.LoaderContext, payload: typing.Any, records_path: str) -> typing.List[typing.Any]:
    found = _dig(payload, records_path) if records_path else payload
    if found is None:
        raise base.SourceFetchError(
            f"{ctx.source.slug}: records_path {records_path!r} is not in the response "
            f"(top level keys: {', '.join(sorted(payload)) if isinstance(payload, dict) else type(payload).__name__})"
        )
    if isinstance(found, dict):
        # Some endpoints key their records by id instead of listing them. The values are the
        # records; the keys are usually the same id, and the field map can still name it.
        return list(found.values())
    if not isinstance(found, list):
        raise base.SourceFetchError(f"{ctx.source.slug}: records_path {records_path!r} is not a list of records")
    return found


def _dig(payload: typing.Any, path: str) -> typing.Any:
    current = payload
    for segment in str(path).split("."):
        if isinstance(current, dict):
            current = current.get(segment)
        else:
            return None
        if current is None:
            return None
    return current
