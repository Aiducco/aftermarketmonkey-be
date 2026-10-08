"""
Transport client for Freshworks CRM (FreshSales) -- the endpoints the reply sync needs:

* ``POST /sales_accounts/upsert``  the shop as a company
* ``POST /contacts/upsert``        the person who replied
* ``POST /notes``                  the reply text, attached to the contact
* ``POST /deals``                  a positive reply's opportunity
* ``GET  /selector/contact_statuses``  status name -> id, resolved at runtime

Base URL is ``https://<bundle-alias>.myfreshworks.com/crm/sales/api``, where the alias is the
``<name>`` in the URL the CRM is logged into. Auth is ``Authorization: Token token=<key>`` -- note
``Token token=``, not ``Bearer``.

Two things to know before changing anything here.

**Upsert returns 200 when it updated and 201 when it created.** Both are success. The distinction
is surfaced on the return value because a 200 on a contact we believed was new means that shop was
already in the CRM through some other route, which is worth seeing in a log.

**A deal requires ``name``, ``amount`` and ``sales_account_id``.** Pipeline, stage and owner really
are optional and the CRM fills in its own defaults, but there is no way to create a deal without an
account, which is why the sync upserts a sales account first rather than only a contact.

Numeric ids are never hardcoded or configured. ``contact_status_id`` is resolved by name from the
selector endpoint on each run, so a status renamed in the CRM raises rather than writing an id that
no longer means what it used to.
"""
import logging
import typing

import requests
from django.conf import settings

from common import utils as common_utils
from src.integrations.clients.freshsales import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[FRESHSALES-CLIENT]"

_BASE_URL_TEMPLATE = "https://{}.myfreshworks.com/crm/sales/api"

# Module-level session so TCP+TLS is reused across the ~3 calls per reply. Single-threaded command;
# see the same note in src/integrations/clients/geoapify/client.py.
_session = requests.Session()


class FreshsalesApiClient(object):
    """One instance per run. Construction only reads settings."""

    def __init__(
        self,
        api_key: typing.Optional[str] = None,
        bundle_alias: typing.Optional[str] = None,
        timeout_seconds: typing.Optional[float] = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else settings.FRESHSALES_API_KEY
        if not self.api_key:
            raise ValueError("Missing FRESHSALES_API_KEY.")
        alias = bundle_alias if bundle_alias is not None else settings.FRESHSALES_BUNDLE_ALIAS
        if not alias:
            raise ValueError("Missing FRESHSALES_BUNDLE_ALIAS -- the <name> in https://<name>.myfreshworks.com.")
        self.bundle_alias = alias
        self.base_url = _BASE_URL_TEMPLATE.format(alias)
        self.timeout_seconds = timeout_seconds if timeout_seconds is not None else settings.FRESHSALES_TIMEOUT_SECONDS
        # Requests issued by this client, so the caller can stay under the account's hourly budget
        # without counting call sites by hand.
        self.calls_made = 0

    def _request(
        self,
        method: str,
        endpoint: str,
        json_body: typing.Optional[dict] = None,
        params: typing.Optional[dict] = None,
    ) -> typing.Tuple[int, dict]:
        url = "{}/{}".format(self.base_url, endpoint.lstrip("/"))
        headers = {
            "Authorization": "Token token={}".format(self.api_key),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        self.calls_made += 1
        try:
            response = _session.request(
                method=method,
                url=url,
                json=json_body,
                params=params,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except requests.exceptions.Timeout as e:
            raise exceptions.FreshsalesTimeout(
                "Timed out after {}s calling {}. Error: {}".format(
                    self.timeout_seconds, endpoint, common_utils.get_exception_message(exception=e)
                )
            )
        except requests.RequestException as e:
            raise exceptions.FreshsalesAPIException(
                "Request exception calling {}. Error: {}".format(
                    endpoint, common_utils.get_exception_message(exception=e)
                )
            )

        if response.status_code in (401, 403):
            raise exceptions.FreshsalesAuthError(
                "FreshSales refused {} (status_code={}). The key is tied to a user and inherits "
                "its permissions, so this may be a permission gap rather than a bad key.".format(
                    endpoint, response.status_code
                )
            )
        if response.status_code == 429:
            raise exceptions.FreshsalesRateLimited(
                "FreshSales rate-limited {} (429). The 1000/hour budget is per account and shared.".format(endpoint)
            )
        if not (200 <= response.status_code < 300):
            # The error body carries which field the CRM objected to, and is the only way to tell
            # a missing required field from a bad value. Truncated: it can be large.
            raise exceptions.FreshsalesAPIException(
                "FreshSales error (endpoint={} status_code={} body={}).".format(
                    endpoint, response.status_code, (response.text or "")[:500]
                )
            )

        try:
            payload = response.json()
        except ValueError as e:
            raise exceptions.FreshsalesAPIException(
                "FreshSales returned an unparseable body (endpoint={}). Error: {}".format(
                    endpoint, common_utils.get_exception_message(exception=e)
                )
            )

        return response.status_code, (payload if isinstance(payload, dict) else {})

    # -- configuration ------------------------------------------------------------------

    def contact_status_ids_by_name(self) -> typing.Dict[str, int]:
        """
        Contact status name -> id, as the CRM has them right now.

        Resolved per run rather than configured: the ids are account-specific integers, and a
        hardcoded one keeps writing successfully after the status it referred to has been renamed
        or repurposed, which is worse than failing.
        """
        _, payload = self._request("GET", "selector/contact_statuses")
        statuses = payload.get("contact_statuses") or []
        return {
            str(s.get("name")): s.get("id") for s in statuses if isinstance(s, dict) and s.get("name") and s.get("id")
        }

    def resolve_contact_status_id(self, name: str, available: typing.Dict[str, int]) -> int:
        status_id = available.get(name)
        if not status_id:
            raise exceptions.FreshsalesConfigError(
                "FreshSales has no contact status named {!r} (found: {}). Rename it back in the CRM, "
                "or update instantly_freshsales_sync.contact_status_name.".format(
                    name, ", ".join(sorted(available)) or "none"
                )
            )
        return status_id

    # -- writes -------------------------------------------------------------------------

    def upsert_sales_account(self, name: str, fields: typing.Optional[dict] = None) -> typing.Tuple[str, bool]:
        """
        Upsert the shop by name. Returns ``(id, created)``; ``created`` is True on a 201.

        Keyed on name because that is the only unique identifier a sales account has here. Two
        genuinely different shops sharing a name would merge -- acceptable, and visible in the CRM,
        where the alternative (a per-reply account) is not.
        """
        body = {"unique_identifier": {"name": name}, "sales_account": dict(fields or {}, name=name)}
        status_code, payload = self._request("POST", "sales_accounts/upsert", json_body=body)
        account = payload.get("sales_account") or {}
        account_id = account.get("id")
        if not account_id:
            raise exceptions.FreshsalesAPIException(
                "FreshSales upserted a sales account but returned no id (body keys={}).".format(sorted(payload))
            )
        return str(account_id), status_code == 201

    def upsert_contact(self, email: str, fields: dict) -> typing.Tuple[str, bool]:
        """Upsert a contact by email. Returns ``(id, created)``; ``created`` is True on a 201."""
        body = {
            "unique_identifier": {"emails": email},
            "contact": dict(fields, emails=[{"value": email, "is_primary": True}]),
        }
        status_code, payload = self._request("POST", "contacts/upsert", json_body=body)
        contact = payload.get("contact") or {}
        contact_id = contact.get("id")
        if not contact_id:
            raise exceptions.FreshsalesAPIException(
                "FreshSales upserted a contact but returned no id (body keys={}).".format(sorted(payload))
            )
        return str(contact_id), status_code == 201

    def create_note(self, description: str, contact_id: str) -> str:
        """Attach a note to a contact. Notes are append-only -- there is no upsert, which is why
        the caller must guard on ``freshsales_note_id`` rather than re-posting."""
        body = {
            "note": {
                "description": description,
                "targetable_type": "Contact",
                "targetable_id": contact_id,
            }
        }
        _, payload = self._request("POST", "notes", json_body=body)
        note = payload.get("note") or {}
        note_id = note.get("id")
        if not note_id:
            raise exceptions.FreshsalesAPIException(
                "FreshSales created a note but returned no id (body keys={}).".format(sorted(payload))
            )
        return str(note_id)

    def update_note(self, note_id: str, description: str) -> None:
        """
        Replace a note's body.

        ``PUT /notes/{id}`` works even though ``GET /notes/{id}`` answers 404 -- a single note is
        only readable through ``GET /contacts/{id}/notes``. Verified against the live account.
        Used to backfill the Unibox link into notes written before that link existed, rather than
        posting a second note per contact saying the same thing.
        """
        self._request("PUT", "notes/{}".format(note_id), json_body={"note": {"description": description}})

    def contact_notes(self, contact_id: str) -> typing.List[dict]:
        """Every note on a contact. The only way to read a note back; see :meth:`update_note`."""
        _, payload = self._request("GET", "contacts/{}/notes".format(contact_id))
        return [n for n in (payload.get("notes") or []) if isinstance(n, dict)]

    def create_deal(
        self,
        name: str,
        amount: typing.Union[int, float],
        sales_account_id: str,
        contact_id: typing.Optional[str] = None,
    ) -> str:
        """
        Create a deal. ``name``, ``amount`` and ``sales_account_id`` are all required by the API.

        Pipeline, stage and owner are deliberately omitted so the CRM applies its own defaults --
        today that is the Default Pipeline at stage "New". Passing them would pin this sync to
        numeric ids that change when the pipeline is edited.
        """
        deal = {"name": name, "amount": amount, "sales_account_id": sales_account_id}
        if contact_id:
            deal["contacts_added_list"] = [contact_id]
        _, payload = self._request("POST", "deals", json_body={"deal": deal})
        created = payload.get("deal") or {}
        deal_id = created.get("id")
        if not deal_id:
            raise exceptions.FreshsalesAPIException(
                "FreshSales created a deal but returned no id (body keys={}).".format(sorted(payload))
            )
        return str(deal_id)
