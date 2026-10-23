"""
Callback endpoint for Apollo's asynchronous direct-dial reveals.

Apollo refuses ``reveal_phone_number`` on ``people/match`` and ``people/bulk_match`` unless a
``webhook_url`` is supplied (HTTP 400 ``SEARCH.VALIDATION.WEBHOOK_URL_REQUIRED``) -- phone
resolution runs on their side and the answer is POSTed back minutes later. There is no endpoint
to poll for it, so this view is the only way a direct dial ever reaches us.

Paired with the ``reveal_apollo_phones`` management command, which fires the requests and reads
the rows back. See ``src.models.ApolloPhoneReveal`` for the two traps worth knowing.
"""
import hmac
import json
import logging
import typing

import simplejson
from django import http, views
from django.conf import settings
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

from src import models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[APOLLO-PHONE-WEBHOOK]"


def _json_response(data: typing.Any, status: int = 200) -> http.HttpResponse:
    return http.HttpResponse(
        headers={"Content-Type": "application/json"},
        content=simplejson.dumps(data),
        status=status,
    )


def _extract_phone_numbers(person: dict) -> list:
    """
    Apollo's phone payload shape is undocumented and has moved before, so every known spelling
    is tried and the raw body is stored regardless (see ApolloPhoneReveal.raw_payload).

    Observed shapes:
      - phone_numbers: [{"raw_number": "...", "sanitized_number": "...", "type": "work_direct"}]
      - contact_phone_numbers / personal_phone_numbers: same element shape
      - direct_dial / sanitized_phone: bare strings
    """
    collected = []

    for key in ("phone_numbers", "contact_phone_numbers", "personal_phone_numbers"):
        for entry in person.get(key) or []:
            if isinstance(entry, dict):
                raw = entry.get("raw_number") or entry.get("number") or ""
                sanitized = entry.get("sanitized_number") or ""
                if raw or sanitized:
                    collected.append(
                        {"raw": raw, "sanitized": sanitized, "type": entry.get("type") or key}
                    )
            elif isinstance(entry, str) and entry:
                collected.append({"raw": entry, "sanitized": "", "type": key})

    for key in ("direct_dial", "sanitized_phone", "phone"):
        value = person.get(key)
        if isinstance(value, str) and value:
            collected.append({"raw": value, "sanitized": "", "type": key})

    # Apollo repeats the same number across keys; dedupe on whichever form is populated.
    deduped, seen = [], set()
    for item in collected:
        fingerprint = item["sanitized"] or item["raw"]
        if fingerprint not in seen:
            seen.add(fingerprint)
            deduped.append(item)
    return deduped


def _people_from_payload(payload: typing.Any) -> list:
    """Apollo posts either a single person, a {"people": [...]} envelope, or a bare list."""
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("people", "matches", "contacts"):
        if isinstance(payload.get(key), list):
            return [p for p in payload[key] if isinstance(p, dict)]
    if isinstance(payload.get("person"), dict):
        return [payload["person"]]
    # A bare person object, which is what a single-record reveal actually sends.
    return [payload] if payload.get("id") else []


@method_decorator(csrf_exempt, name="dispatch")
class ApolloPhoneWebhookView(views.View):
    """
    POST /api/apollo/phone-webhook/?secret=<APOLLO_WEBHOOK_SECRET>

    Apollo cannot be configured to send an auth header on its callback, so the shared secret
    rides in the query string -- anything holding this URL can write rows, so treat it as a
    bearer credential. Always answers 200 once authenticated: a non-2xx makes Apollo retry, and
    a malformed payload will not parse any better the second time.
    """

    def post(self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any) -> http.HttpResponse:
        expected = getattr(settings, "APOLLO_WEBHOOK_SECRET", "")
        if not expected:
            logger.error("%s APOLLO_WEBHOOK_SECRET is unset -- rejecting callback", _LOG_PREFIX)
            return _json_response({"message": "Webhook not configured"}, status=503)

        supplied = request.GET.get("secret", "")
        if not hmac.compare_digest(supplied, expected):
            logger.warning("%s Rejected callback with bad secret", _LOG_PREFIX)
            return _json_response({"message": "Forbidden"}, status=403)

        try:
            payload = json.loads(request.body or b"{}")
        except (ValueError, UnicodeDecodeError):
            logger.exception("%s Callback body was not JSON", _LOG_PREFIX)
            return _json_response({"received": True, "stored": 0})

        people = _people_from_payload(payload)
        if not people:
            logger.warning("%s Callback carried no person objects: %s", _LOG_PREFIX, str(payload)[:400])
            return _json_response({"received": True, "stored": 0})

        stored = 0
        for person in people:
            person_id = person.get("id") or ""
            if not person_id:
                continue

            phones = _extract_phone_numbers(person)
            organization = person.get("organization") or {}

            defaults = {
                "status": "received" if phones else "no_phone",
                "phone_numbers": phones,
                "raw_payload": person,
                "received_at": timezone.now(),
            }
            # Only overwrite the snapshot fields when the callback actually carries them --
            # the phone payload is sparser than the enrich payload that created the row.
            if person.get("name"):
                defaults["name"] = person["name"]
            if person.get("title"):
                defaults["title"] = person["title"]
            if person.get("email"):
                defaults["email"] = person["email"]
            if organization.get("primary_domain"):
                defaults["organization_domain"] = organization["primary_domain"]

            models.ApolloPhoneReveal.objects.update_or_create(
                apollo_person_id=person_id, defaults=defaults
            )
            stored += 1

        logger.info("%s Stored %d reveal(s)", _LOG_PREFIX, stored)
        return _json_response({"received": True, "stored": stored})
