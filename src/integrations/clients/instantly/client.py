"""
Transport client for Instantly's V2 API -- the three endpoints the reply sync needs:

* ``GET  /emails``       the Unibox feed, filtered to inbound. The endpoint we poll.
* ``POST /leads/list``   one lead by address, for re-reading its interest label.
* ``GET  /campaigns``    campaign id -> name, so a deal can be named after the campaign.

Everything here is transport: it unwraps ``items``, pages, and raises typed errors. Mapping an
email onto our own row belongs to ``src/integrations/services/instantly_freshsales_sync.py``.

Four behaviours of the live API that its documentation gets wrong are handled here rather than
left to callers. All four were verified against the production workspace on 2026-10-03:

1. ``next_starting_after`` is returned **even on the last page**, and following it yields a page
   of zero items. :meth:`iter_received_emails` therefore stops on an empty page, never on the
   absence of a cursor -- the documented loop never terminates.
2. ``is_auto_reply`` is documented on the email object but is **never returned**. The API omits
   null fields entirely, so no caller can rely on it; the service derives auto-replies from the
   interest label instead.
3. ``lt_interest_status`` as a filter on ``POST /leads/list`` is **silently ignored** (asking for
   "Interested" returned 100 leads of which 95 were unlabelled). :meth:`find_lead` looks a lead up
   by ``search`` instead, which is exact.
4. ``lead_id`` is **not** returned on an email, only ``lead`` (the address). So the address is the
   join key between an email and its lead.

A fifth, unrelated to the schema: Instantly's edge answers 403 to ``urllib`` with its default
User-Agent, while ``requests`` is fine. Nothing to do about it, but it has cost someone an
afternoon before -- do not "fix" a phantom auth error by adding a browser User-Agent.
"""
import logging
import time
import typing

import requests
from django.conf import settings

from common import utils as common_utils
from src.integrations.clients.instantly import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[INSTANTLY-CLIENT]"

# Instantly's own hard cap on `limit` for the list endpoints.
_MAX_LIMIT = 100

# ``GET /emails`` is metered at 20 requests/minute -- tighter than the rest of the API, and the
# only endpoint we call in a loop. 3.1s between calls keeps a long backfill inside it without any
# coordination; at ~22 replies/month the pacing never actually fires in normal operation, it is
# there so a first-run backfill cannot get us locked out.
_EMAILS_MIN_SECONDS_BETWEEN_CALLS = 3.1

# Module-level so TCP+TLS connections are reused across calls within a run, matching
# src/integrations/clients/geoapify/client.py. The management command is a single-threaded
# process, so one shared Session is safe here; requests.Session is not documented as thread-safe,
# so anything that later calls this from threads needs a per-thread session.
_session = requests.Session()


class InstantlyApiClient(object):
    """One instance per run. Construction only reads settings."""

    def __init__(
        self,
        api_key: typing.Optional[str] = None,
        timeout_seconds: typing.Optional[float] = None,
        base_url: typing.Optional[str] = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else settings.INSTANTLY_API_KEY
        if not self.api_key:
            raise ValueError("Missing INSTANTLY_API_KEY.")
        self.timeout_seconds = timeout_seconds if timeout_seconds is not None else settings.INSTANTLY_TIMEOUT_SECONDS
        self.base_url = (base_url if base_url is not None else settings.INSTANTLY_BASE_URL).rstrip("/")
        # Pacing state for /emails, per instance: a run holds one client, so this is the run's
        # own clock. time.monotonic rather than time.time so a clock adjustment mid-backfill
        # cannot turn into a burst.
        self._last_emails_call_at: typing.Optional[float] = None

    def _request(
        self,
        method: str,
        endpoint: str,
        params: typing.Optional[dict] = None,
        json_body: typing.Optional[dict] = None,
    ) -> dict:
        url = "{}/{}".format(self.base_url, endpoint.lstrip("/"))
        headers = {"Authorization": "Bearer {}".format(self.api_key), "Accept": "application/json"}

        try:
            response = _session.request(
                method=method,
                url=url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except requests.exceptions.Timeout as e:
            raise exceptions.InstantlyTimeout(
                "Timed out after {}s calling {}. Error: {}".format(
                    self.timeout_seconds, endpoint, common_utils.get_exception_message(exception=e)
                )
            )
        except requests.RequestException as e:
            raise exceptions.InstantlyAPIException(
                "Request exception calling {}. Error: {}".format(
                    endpoint, common_utils.get_exception_message(exception=e)
                )
            )

        if response.status_code in (401, 403):
            raise exceptions.InstantlyAuthError(
                "Instantly rejected the key for {} (status_code={}). Either the key is wrong or it "
                "lacks the scope for this endpoint.".format(endpoint, response.status_code)
            )
        if response.status_code == 429:
            raise exceptions.InstantlyRateLimited("Instantly rate-limited {} (429).".format(endpoint))
        if not (200 <= response.status_code < 300):
            raise exceptions.InstantlyAPIException(
                "Instantly error (endpoint={} status_code={}).".format(endpoint, response.status_code)
            )

        try:
            payload = response.json()
        except ValueError as e:
            raise exceptions.InstantlyAPIException(
                "Instantly returned an unparseable body (endpoint={}). Error: {}".format(
                    endpoint, common_utils.get_exception_message(exception=e)
                )
            )

        return payload if isinstance(payload, dict) else {}

    def _pace_emails_call(self) -> None:
        if self._last_emails_call_at is None:
            self._last_emails_call_at = time.monotonic()
            return
        elapsed = time.monotonic() - self._last_emails_call_at
        wait = _EMAILS_MIN_SECONDS_BETWEEN_CALLS - elapsed
        if wait > 0:
            logger.debug("{} Pacing /emails: sleeping {:.1f}s.".format(_LOG_PREFIX, wait))
            time.sleep(wait)
        self._last_emails_call_at = time.monotonic()

    def iter_received_emails(
        self,
        min_timestamp_created: typing.Optional[str] = None,
        campaign_id: typing.Optional[str] = None,
        page_limit: int = _MAX_LIMIT,
        max_pages: int = 200,
    ) -> typing.Iterator[dict]:
        """
        Yield inbound emails oldest-first, paging until a page comes back empty.

        Oldest-first matters: a page boundary crossed mid-run leaves the watermark behind the
        emails already stored rather than ahead of ones that were never read, so the next run
        re-reads instead of skipping.

        ``min_timestamp_created`` is an ISO-8601 string as the API returns them, passed straight
        through. ``max_pages`` is a guard against an API that keeps handing back non-empty pages
        forever, not a real limit -- 200 pages is 20,000 replies.
        """
        params = {
            "email_type": "received",
            "sort_order": "asc",
            "limit": min(page_limit, _MAX_LIMIT),
        }
        if min_timestamp_created:
            params["min_timestamp_created"] = min_timestamp_created
        if campaign_id:
            params["campaign_id"] = campaign_id

        cursor = None
        for page in range(max_pages):
            page_params = dict(params)
            if cursor:
                page_params["starting_after"] = cursor

            self._pace_emails_call()
            payload = self._request("GET", "emails", params=page_params)
            items = payload.get("items") or []

            logger.info("{} /emails page {} returned {} item(s).".format(_LOG_PREFIX, page + 1, len(items)))

            # An empty page ends the walk even when a cursor is still offered. Instantly returns
            # next_starting_after on the last page too, so trusting the cursor loops forever.
            if not items:
                return

            for item in items:
                if isinstance(item, dict):
                    yield item

            cursor = payload.get("next_starting_after")
            if not cursor:
                return

        logger.warning(
            "{} Stopped after max_pages={} -- more replies may remain; the next run resumes from "
            "the watermark.".format(_LOG_PREFIX, max_pages)
        )

    def find_lead(self, email: str) -> typing.Optional[dict]:
        """
        The lead for ``email``, or None. Used to re-read ``lt_interest_status`` after Instantly (or
        a human in Unibox) has labelled a thread that was unlabelled when the reply arrived.

        Looks up by ``search`` rather than by interest filter, because the documented
        ``lt_interest_status`` filter is ignored by the live API -- see the module docstring. The
        response is confirmed to be filtered to the address: one call per address, exact.
        """
        if not email:
            return None
        payload = self._request("POST", "leads/list", json_body={"limit": 1, "search": email})
        items = payload.get("items") or []
        for item in items:
            # Defensive: `search` matched on the address for every case tested, but a substring
            # match would silently attach another shop's interest label to this reply.
            if isinstance(item, dict) and (item.get("email") or "").lower() == email.lower():
                return item
        return None

    def list_campaigns(self) -> typing.Dict[str, str]:
        """Campaign id -> name, for naming deals. One call; there are two campaigns today."""
        names = {}
        cursor = None
        for _ in range(50):
            params = {"limit": _MAX_LIMIT}
            if cursor:
                params["starting_after"] = cursor
            payload = self._request("GET", "campaigns", params=params)
            items = payload.get("items") or []
            if not items:
                break
            for item in items:
                if isinstance(item, dict) and item.get("id"):
                    names[str(item["id"])] = item.get("name") or ""
            cursor = payload.get("next_starting_after")
            if not cursor:
                break
        return names
