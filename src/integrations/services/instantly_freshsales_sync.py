"""
Turns Instantly replies into FreshSales contacts, and Instantly's own positive labels into deals.

Run by ``manage.py sync_instantly_replies`` on a 15-minute cron. Three passes, each independently
re-runnable, because the useful failure mode is "stopped halfway and resumed", not "rolled back":

1. :func:`ingest_replies`  -- read inbound emails from Instantly into ``InstantlyReply``. Writes
   nothing to the CRM, so a FreshSales outage cannot lose a reply.
2. :func:`push_contacts`   -- sales account, contact, note for every human reply not yet pushed.
3. :func:`refresh_interest` -- re-read the interest label and keep the CRM contact status honest.

:func:`create_deals` exists and is tested but is **not called unless ``FRESHSALES_CREATE_DEALS``
is on**, which it is not by default: deals are opened by hand. A positive reply still lands as a
contact whose FreshSales status reads "Interested", which is the queue to work from.

**When it is on**, create_deals follows the rescue-process spec exactly (2026-10-19): check
FreshSales directly for a deal already open on the contact (not just our own
``freshsales_deal_id``) before creating a second one, pin the new deal to a specific
pipeline/stage with the custom fields the rescue process reads from
(settings.FRESHSALES_DEAL_*), remove the lead from its Instantly campaign so the cold sequence
stops, and add the full reply as a note on the deal. The campaign-removal step
(:meth:`InstantlyApiClient.remove_lead_from_campaign`) and three of the new FreshSales client
methods it needs (:meth:`find_contact_by_email`, :meth:`get_contact_deals`,
:meth:`deal_stage_types_by_id`) are **unverified against the live account** -- the FreshSales API
key has been rejected with 401 since 2026-10-17 (see that incident), so none of this could be
tested against real data; confirm the response shapes once access is restored, per each method's
own docstring.

**Why pass 3 exists.** Instantly's label is not always set when the reply arrives: their AI applies
it shortly after, and a human relabelling in Unibox can change it much later. In the live account 4
of 22 replies are unlabelled, two of them a month old. Reading ``i_status`` once at ingest and never
again would permanently miss any of those that later turn positive, so the label is re-read for a
window after the reply (``INSTANTLY_INTEREST_RECHECK_DAYS``) and the contact's status in the CRM is
updated along with it.

**What we trust.** Positive is Instantly's own label, not a judgement of our own -- verified sound:
their ``i_status`` agreed with the lead's ``lt_interest_status`` on every address in the account,
and 18 of 22 replies are labelled. No LLM is involved in this path.

**Where the shop context comes from.** ``scripts/export_instantly_list.py`` uploads City / State /
Zip / Tier / Typology / Locations / company / website / phone as Instantly custom variables, and
Instantly hands them back on the lead. So a CRM contact can be built rich without matching back to
``Lead`` / ``RealTruckLead`` at all -- see docs/INSTANTLY_FRESHSALES_SYNC_PLAN.md §5 for why that
matching was dropped.
"""
import datetime
import logging
import typing

from django.db import models as django_db_models
from django.utils import timezone

from common import utils as common_utils
from src import models as src_models
from src.integrations.clients.freshsales import client as freshsales_client
from src.integrations.clients.freshsales import exceptions as freshsales_exceptions
from src.integrations.clients.instantly import client as instantly_client
from src.integrations.clients.instantly import exceptions as instantly_exceptions
from src.integrations.services import platform_crm_sync

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[INSTANTLY-FRESHSALES]"

TASK_NAME = "sync_instantly_replies"

# Instantly's label -> the FreshSales contact status that says the same thing. Resolved to numeric
# ids by name at runtime (see FreshsalesApiClient.resolve_contact_status_id), never configured.
# All three live in the "Lead" lifecycle stage, so nothing here promotes a contact past where it
# belongs.
CONTACT_STATUS_INTERESTED = "Interested"
CONTACT_STATUS_UNQUALIFIED = "Unqualified"
CONTACT_STATUS_CONTACTED = "Contacted"

# A display name containing one of these is a business, not a person. Instantly's From header
# carries both kinds -- "Miguel Bautista" and "Sundowner Truck Accessories" are both real examples
# from the account -- and a business name split into first/last puts "Sundowner Truck" in front of
# a human reading the CRM.
_BUSINESS_NAME_TOKENS = frozenset(
    {
        "4x4",
        "4wd",
        "accessories",
        "auto",
        "automotive",
        "autosports",
        "center",
        "centre",
        "co",
        "company",
        "corp",
        "customs",
        "dealer",
        "diesel",
        "enterprises",
        "fab",
        "fabrication",
        "garage",
        "gear",
        "group",
        "inc",
        "industries",
        "jeep",
        "llc",
        "ltd",
        "motors",
        "motorsports",
        "offroad",
        "off-road",
        "outfitters",
        "performance",
        "parts",
        "rv",
        "sale",
        "sales",
        "service",
        "services",
        "shop",
        "supply",
        "tire",
        "tires",
        "truck",
        "trucks",
        "wheel",
        "wheels",
        "works",
    }
)

# A token with 4x4 or 4wd anywhere inside it is a shop name, not a surname -- the live account has
# "Clarksville MC4x4 Sales", where neither word is in the list above but "MC4x4" gives it away.
_BUSINESS_TOKEN_SUBSTRINGS = ("4x4", "4wd")

# Free-mailbox domains are never a usable company name: a dozen unrelated shops reply from gmail,
# and an account called "gmail.com" would collect them all.
_GENERIC_EMAIL_DOMAINS = frozenset(
    {
        "gmail.com",
        "googlemail.com",
        "yahoo.com",
        "ymail.com",
        "hotmail.com",
        "outlook.com",
        "live.com",
        "msn.com",
        "aol.com",
        "icloud.com",
        "me.com",
        "comcast.net",
        "att.net",
        "verizon.net",
        "sbcglobal.net",
        "bellsouth.net",
        "cox.net",
        "charter.net",
    }
)


# ---------------------------------------------------------------------------------------------
# Pure mapping helpers. No DB, no HTTP -- all of the decisions this sync makes live here so they
# can be tested directly, which matters because the orchestration below cannot be tested against
# the production database this repo's .env points at.
# ---------------------------------------------------------------------------------------------


def parse_timestamp(value: typing.Optional[str]) -> typing.Optional[datetime.datetime]:
    """Instantly's ISO-8601 with a trailing Z, as an aware UTC datetime. None on anything unparseable."""
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        logger.warning("{} Unparseable timestamp {!r}.".format(_LOG_PREFIX, value))
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def is_positive_interest(interest_status: typing.Optional[int]) -> bool:
    """Interested / Meeting Booked / Meeting Completed / Closed. Anything else, including an
    unlabelled thread, is not positive -- and unlabelled is the common case early on."""
    return interest_status in src_models.InstantlyReply.POSITIVE_INTEREST


def derive_is_auto_reply(interest_status: typing.Optional[int], subject: typing.Optional[str]) -> bool:
    """
    Whether this reply is an auto-responder rather than a person.

    Instantly documents an ``is_auto_reply`` field on the email object but never actually returns
    it, so it cannot be read. What it does do is label out-of-office replies ``i_status = 0``, and
    those also arrive with an "Out of office Re: ..." subject. Either signal is enough; the subject
    check catches an auto-reply that has not been labelled yet.
    """
    if interest_status == src_models.InstantlyReply.Interest.OUT_OF_OFFICE:
        return True
    text = (subject or "").strip().lower()
    return text.startswith(("out of office", "automatic reply", "auto-reply", "autoreply", "away from"))


def contact_status_name(interest_status: typing.Optional[int]) -> str:
    """The FreshSales contact status that reports Instantly's verdict."""
    if is_positive_interest(interest_status):
        return CONTACT_STATUS_INTERESTED
    if interest_status is not None and interest_status < 0:
        return CONTACT_STATUS_UNQUALIFIED
    # Out of office, or not labelled yet. Both mean "they received it and we know nothing more".
    return CONTACT_STATUS_CONTACTED


def domain_of(email: str) -> str:
    return (email or "").strip().lower().rpartition("@")[2]


def domain_of_url(url: str) -> str:
    """The bare host of a website custom variable ("https://www.rhinoutah.com/shop" -> "rhinoutah.com"),
    for company_name_for's website-domain fallback. Not domain_of -- that splits on "@" and would
    return a URL unchanged since it has none."""
    text = (url or "").strip().lower()
    if not text:
        return ""
    text = text.split("://", 1)[-1]  # drop a scheme if present
    text = text.split("/", 1)[0]  # drop any path
    text = text.split(":", 1)[0]  # drop a port
    if text.startswith("www."):
        text = text[4:]
    return text


def display_name_of(email_payload: dict) -> str:
    """The sender's own From-header display name, which is what they typed, not a guess."""
    addresses = email_payload.get("from_address_json") or []
    for entry in addresses:
        if isinstance(entry, dict) and (entry.get("name") or "").strip():
            return entry["name"].strip()
    return ""


def looks_like_business_name(name: str) -> bool:
    tokens = [t.strip(",.").lower() for t in (name or "").split()]
    if not tokens:
        return False
    if any(t in _BUSINESS_NAME_TOKENS for t in tokens):
        return True
    if any(fragment in t for t in tokens for fragment in _BUSINESS_TOKEN_SUBSTRINGS):
        return True
    # "Smith Brothers Jeep Outfitters LLC" style: a person's name is one or two words, three at a
    # push. Four or more is a business even without a recognised token.
    return len(tokens) >= 4


def split_display_name(display_name: str, company_name: str = "") -> typing.Tuple[str, str]:
    """
    ``(first_name, last_name)``, or ``("", "")`` when the display name is a business.

    Blank is the safe failure. This repo already refuses to guess a person's name onto outbound
    mail -- scripts/export_instantly_list.py leaves First/Last empty on purpose -- and a wrong name
    on a CRM record a human will read is worse than no name at all.
    """
    name = (display_name or "").strip()
    if not name:
        return "", ""
    if company_name and name.strip().lower() == company_name.strip().lower():
        return "", ""
    if looks_like_business_name(name):
        return "", ""
    parts = name.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def company_name_for(
    lead_email: str, lead_payload: typing.Optional[dict] = None, person_name: str = ""
) -> str:
    """
    The shop's name, in order: Instantly's ``companyName`` custom variable; else the website's own
    domain; else the reply's email domain (skipped for a free mailbox -- those fall through
    instead, so fourteen unrelated gmail repliers do not all land under one "gmail.com" account);
    else the person's full name; else the email address itself, as the last resort of last
    resorts.

    ``person_name`` is optional so every existing caller that has not been updated to pass one
    (and every old test) keeps its previous behaviour -- it only changes the answer for an address
    that reaches step 4 with no name supplied.
    """
    payload = lead_payload or {}
    for key in ("companyName", "company_name"):
        value = (payload.get(key) or "").strip()
        if value:
            return value
    website_domain = domain_of_url(payload.get("website") or "")
    if website_domain and website_domain not in _GENERIC_EMAIL_DOMAINS:
        return website_domain
    domain = domain_of(lead_email)
    if domain and domain not in _GENERIC_EMAIL_DOMAINS:
        return domain
    name = (person_name or "").strip()
    if name:
        return name
    return lead_email or domain or "Unknown"


def row_fields_from_email(
    email_payload: dict,
    campaign_names: typing.Optional[typing.Dict[str, str]] = None,
) -> dict:
    """Map one Instantly email onto ``InstantlyReply`` field values."""
    interest_status = email_payload.get("i_status")
    subject = email_payload.get("subject")
    campaign_id = email_payload.get("campaign_id")
    body = email_payload.get("body") or {}

    return {
        "instantly_email_id": str(email_payload.get("id") or ""),
        "thread_id": email_payload.get("thread_id"),
        "campaign_id": campaign_id,
        "campaign_name": (campaign_names or {}).get(str(campaign_id or "")) or None,
        "lead_email": (email_payload.get("lead") or email_payload.get("from_address_email") or "").strip(),
        "from_name": display_name_of(email_payload) or None,
        "eaccount": email_payload.get("eaccount"),
        "subject": subject,
        "body_text": body.get("text") or email_payload.get("content_preview"),
        "email_timestamp": parse_timestamp(email_payload.get("timestamp_email")),
        "instantly_created_at": parse_timestamp(email_payload.get("timestamp_created")),
        "interest_status": interest_status,
        "is_positive": is_positive_interest(interest_status),
        "is_auto_reply": derive_is_auto_reply(interest_status, subject),
    }


def sales_account_fields(reply: "src_models.InstantlyReply") -> dict:
    """Address and web details for the shop, from Instantly's custom variables."""
    payload = reply.lead_payload or {}
    fields = {
        "website": payload.get("website") or None,
        "phone": payload.get("phoneNumber") or None,
        "city": payload.get("City") or None,
        "state": payload.get("State") or None,
        "zipcode": payload.get("Zip") or None,
    }
    return {k: v for k, v in fields.items() if v}


def contact_fields(reply: "src_models.InstantlyReply", contact_status_id: int) -> dict:
    """
    The contact body. Names come from the reply's From header (§split_display_name); everything
    else from Instantly's custom variables, which are our own export's data coming home.
    """
    payload = reply.lead_payload or {}
    company = company_name_for(reply.lead_email, payload, person_name=reply.from_name or "")
    first_name, last_name = split_display_name(reply.from_name or "", company)

    fields = {
        "first_name": first_name,
        "last_name": last_name,
        "contact_status_id": contact_status_id,
        "work_number": payload.get("phoneNumber") or None,
        "job_title": payload.get("jobTitle") or None,
        "city": payload.get("City") or None,
        "state": payload.get("State") or None,
        "zipcode": payload.get("Zip") or None,
        "country": payload.get("Country") or None,
    }
    return {k: v for k, v in fields.items() if v not in (None, "")}


def unibox_url(reply: "src_models.InstantlyReply") -> typing.Optional[str]:
    """
    A link straight to this thread in Instantly's Unibox, or None.

    **The format is configured, not discovered.** Instantly only hands out a real ``unibox_url`` on
    *webhook* payloads, and this sync polls ``GET /emails``, which returns no URL field at all --
    checked against every field the live API returns. So the link is built from
    ``INSTANTLY_UNIBOX_URL_TEMPLATE`` and the thread id.

    Because the template is a best guess until somebody pastes a real Unibox URL, the note also
    carries the raw thread id and the lead's address (see :func:`note_description`): those are both
    searchable in Unibox, so the thread stays findable even if this link is wrong. Set the template
    to "" to drop the link entirely and rely on the search hints.
    """
    from django.conf import settings as django_settings

    template = getattr(django_settings, "INSTANTLY_UNIBOX_URL_TEMPLATE", "") or ""
    if not template or not reply.thread_id:
        return None
    try:
        return template.format(thread_id=reply.thread_id, lead_email=reply.lead_email or "")
    except (KeyError, IndexError):
        # A malformed template must not take a whole sync pass down over a convenience link.
        logger.warning("{} INSTANTLY_UNIBOX_URL_TEMPLATE is malformed: {!r}".format(_LOG_PREFIX, template))
        return None


def note_description(reply: "src_models.InstantlyReply") -> str:
    """
    The reply, with the context needed to read it a month later.

    The body is included in full rather than truncated: it is the reason the contact exists, and a
    note is the only place in the CRM it will ever appear.
    """
    payload = reply.lead_payload or {}
    label = reply.get_interest_status_display() if reply.interest_status is not None else "unlabelled"
    received = reply.email_timestamp.isoformat() if reply.email_timestamp else "unknown"

    lines = [
        "Reply received in Instantly",
        "",
        "From:      {} <{}>".format(reply.from_name or "", reply.lead_email).strip(),
        "Received:  {}".format(received),
        "Campaign:  {}".format(reply.campaign_name or reply.campaign_id or "unknown"),
        "Mailbox:   {}".format(reply.eaccount or "unknown"),
        "Instantly label: {}".format(label),
    ]

    link = unibox_url(reply)
    if link:
        lines += ["", "Open the thread: {}".format(link)]
    # Always present, and independent of whether the link format is right: both of these paste
    # straight into Unibox's own search box.
    lines += [
        "",
        "Find it in Unibox by searching for:  {}".format(reply.lead_email or "(no address)"),
        "            or by thread:            thread:{}".format(reply.thread_id or "(unknown)"),
    ]

    shop_bits = [
        "{}: {}".format(key, payload.get(source))
        for key, source in (
            ("Location", "City"),
            ("State", "State"),
            ("Locations", "Locations"),
            ("Tier", "Tier"),
            ("Typology", "Typology"),
            ("Website", "website"),
        )
        if payload.get(source)
    ]
    if shop_bits:
        lines += ["", "Shop: " + " | ".join(shop_bits)]

    lines += ["", "Subject: {}".format(reply.subject or "(none)"), "", (reply.body_text or "").strip()]
    return "\n".join(lines)


def deal_name(reply: "src_models.InstantlyReply") -> str:
    """Just the company name -- no "Deal" suffix, no campaign name appended. Exact spec."""
    return company_name_for(reply.lead_email, reply.lead_payload, person_name=reply.from_name or "")


def watermark(overlap_minutes: int, initial_days: int) -> typing.Optional[datetime.datetime]:
    """
    Where the next ingest starts: the newest reply already stored, less an overlap.

    Derived rather than stored. A stored cursor can end up ahead of the rows it claims to describe
    when a run half-commits; this cannot, and the unique constraint on ``instantly_email_id``
    absorbs whatever the overlap re-reads. With no rows at all, falls back to ``initial_days`` so a
    first run does not silently pull an entire account's history.
    """
    latest = (
        src_models.InstantlyReply.objects.order_by("-instantly_created_at")
        .values_list("instantly_created_at", flat=True)
        .first()
    )
    if latest:
        return latest - datetime.timedelta(minutes=overlap_minutes)
    if initial_days:
        return timezone.now() - datetime.timedelta(days=initial_days)
    return None


def _iso(value: typing.Optional[datetime.datetime]) -> typing.Optional[str]:
    return value.isoformat() if value else None


def _as_id(value: typing.Optional[str]) -> typing.Union[int, str, None]:
    """A stored FreshSales id back as the integer the API issued, or unchanged if it is not one."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


# ---------------------------------------------------------------------------------------------
# Pass 1 -- ingest
# ---------------------------------------------------------------------------------------------


def ingest_replies(
    client: "instantly_client.InstantlyApiClient",
    since: typing.Optional[datetime.datetime] = None,
    campaign_id: typing.Optional[str] = None,
    campaign_names: typing.Optional[typing.Dict[str, str]] = None,
) -> typing.Dict[str, int]:
    """
    Store every inbound email at or after ``since``. Returns counts.

    Existing rows are updated only where Instantly is authoritative and we are not: the interest
    label and the campaign name. The CRM ids and sync timestamps on an existing row are never
    touched here, so re-reading the overlap window cannot undo work pass 2 already did.
    """
    counts = {"seen": 0, "created": 0, "relabelled": 0}

    for email_payload in client.iter_received_emails(min_timestamp_created=_iso(since), campaign_id=campaign_id):
        counts["seen"] += 1
        fields = row_fields_from_email(email_payload, campaign_names)
        if not fields["instantly_email_id"]:
            logger.warning("{} Skipping an email with no id.".format(_LOG_PREFIX))
            continue
        if not fields["lead_email"]:
            logger.warning(
                "{} Reply {} has no lead address -- stored, but it cannot become a contact.".format(
                    _LOG_PREFIX, fields["instantly_email_id"]
                )
            )

        reply, created = src_models.InstantlyReply.objects.get_or_create(
            instantly_email_id=fields["instantly_email_id"],
            defaults=dict(fields, interest_checked_at=timezone.now()),
        )
        if created:
            counts["created"] += 1
            continue

        # Already stored. Take a changed label (someone relabelled in Unibox between runs) and a
        # campaign name we did not have, and nothing else.
        updates = {}
        if reply.interest_status != fields["interest_status"] and fields["interest_status"] is not None:
            updates.update(
                interest_status=fields["interest_status"],
                is_positive=fields["is_positive"],
                is_auto_reply=fields["is_auto_reply"],
                interest_checked_at=timezone.now(),
            )
        if fields["campaign_name"] and not reply.campaign_name:
            updates["campaign_name"] = fields["campaign_name"]
        if updates:
            for key, value in updates.items():
                setattr(reply, key, value)
            reply.save(update_fields=list(updates) + ["updated_at"])
            counts["relabelled"] += 1

    return counts


# ---------------------------------------------------------------------------------------------
# Pass 2 -- contacts
# ---------------------------------------------------------------------------------------------


def push_contacts(
    crm: "freshsales_client.FreshsalesApiClient",
    limit: typing.Optional[int] = None,
    max_calls: typing.Optional[int] = None,
    max_attempts: int = 5,
    status_ids: typing.Optional[typing.Dict[str, int]] = None,
) -> typing.Dict[str, int]:
    """
    Sales account, contact and note for every human reply not yet pushed.

    Each id is saved the moment the CRM returns it, so a crash between the contact and the note
    resumes rather than creating the contact twice. One reply failing is recorded on its row and
    the pass continues -- a single malformed reply must not stall the queue behind it.

    Also -- regardless of FRESHSALES_CREATE_DEALS -- posts the reply as a note on the contact's
    deal when one is already open (see :func:`_push_one_contact`), so an ongoing conversation
    stays visible on whichever deal a human (or :func:`create_deals`) is working, not just on the
    contact. ``stage_types`` is resolved lazily, once, only the first time a contact actually needs
    the lookup, same reasoning as :func:`create_deals`.
    """
    counts = {
        "contacts_created": 0,
        "contacts_updated": 0,
        "notes_created": 0,
        "deal_notes_created": 0,
        "skipped_no_email": 0,
        "failed": 0,
        "budget_stopped": 0,
    }

    # Reuse the caller's resolution when there is one: an idle tick would otherwise spend two
    # FreshSales calls on the same selector just to find there is nothing to push.
    if status_ids is None:
        status_ids = crm.contact_status_ids_by_name()
    stage_types: typing.Optional[typing.Dict[int, str]] = None

    queryset = src_models.InstantlyReply.objects.filter(
        contact_synced_at__isnull=True,
        is_auto_reply=False,
        sync_attempts__lt=max_attempts,
    ).order_by("email_timestamp")
    if limit:
        queryset = queryset[:limit]

    for reply in queryset:
        if max_calls is not None and crm.calls_made >= max_calls:
            counts["budget_stopped"] += 1
            logger.warning(
                "{} Stopping pass 2: {} FreshSales calls used, budget is {}. The rest resumes next "
                "run.".format(_LOG_PREFIX, crm.calls_made, max_calls)
            )
            break

        if not reply.lead_email:
            counts["skipped_no_email"] += 1
            continue

        try:
            if stage_types is None:
                stage_types = crm.deal_stage_types_by_id()
            _push_one_contact(crm, reply, status_ids, stage_types, counts)
        except (
            freshsales_exceptions.FreshsalesRateLimited,
            freshsales_exceptions.FreshsalesAuthError,
            freshsales_exceptions.FreshsalesConfigError,
        ):
            # Account-wide, not row-specific: every remaining row would fail the same way, and
            # burning sync_attempts on all of them would quietly retire the queue.
            raise
        except Exception as e:
            counts["failed"] += 1
            reply.sync_attempts += 1
            reply.last_error = common_utils.get_exception_message(exception=e)
            reply.save(update_fields=["sync_attempts", "last_error", "updated_at"])
            logger.exception(
                "{} Reply {} ({}) failed to sync (attempt {}).".format(
                    _LOG_PREFIX, reply.instantly_email_id, reply.lead_email, reply.sync_attempts
                )
            )

    return counts


def _push_one_contact(
    crm: "freshsales_client.FreshsalesApiClient",
    reply: "src_models.InstantlyReply",
    status_ids: typing.Dict[str, int],
    stage_types: typing.Dict[int, str],
    counts: typing.Dict[str, int],
) -> None:
    company = company_name_for(reply.lead_email, reply.lead_payload, person_name=reply.from_name or "")

    if not reply.freshsales_account_id:
        account_id, _ = crm.upsert_sales_account(name=company, fields=sales_account_fields(reply))
        reply.freshsales_account_id = account_id
        reply.save(update_fields=["freshsales_account_id", "updated_at"])

    if not reply.freshsales_contact_id:
        status_id = crm.resolve_contact_status_id(contact_status_name(reply.interest_status), status_ids)
        fields = contact_fields(reply, status_id)
        if platform_crm_sync.is_platform_contact(reply.lead_email):
            # They have signed up, which puts them at "Qualified" in the Sales Qualified Lead
            # stage. A cold-reply label is a weaker signal, so it must not drag them back to a
            # Lead-stage status; everything else about the contact is still written.
            fields.pop("contact_status_id", None)
            logger.info(
                "{} {} is a platform signup -- leaving its CRM status alone.".format(_LOG_PREFIX, reply.lead_email)
            )
        # FreshSales ids are integers; we store them as text (they are opaque to us), so the
        # association is cast back rather than sent as a string the API may not match.
        fields["sales_accounts"] = [{"id": _as_id(reply.freshsales_account_id), "is_primary": True}]
        contact_id, created = crm.upsert_contact(email=reply.lead_email, fields=fields)
        reply.freshsales_contact_id = contact_id
        reply.save(update_fields=["freshsales_contact_id", "updated_at"])
        counts["contacts_created" if created else "contacts_updated"] += 1

    if not reply.freshsales_note_id:
        note_id = crm.create_note(description=note_description(reply), targetable_id=reply.freshsales_contact_id)
        reply.freshsales_note_id = note_id
        reply.save(update_fields=["freshsales_note_id", "updated_at"])
        counts["notes_created"] += 1

    # Keep an already-open deal's conversation current -- every reply, not just positive ones, so
    # a routine back-and-forth after "Interested" still shows up where a rep is actually looking.
    # Independent of FRESHSALES_CREATE_DEALS: this never creates a deal, only notes an existing
    # one (ours or one a human opened by hand). freshsales_deal_note_id is the single idempotency
    # marker shared with create_deals' own open-deal path, so the same reply is never posted twice
    # across the two passes -- see that function's docstring.
    if not reply.freshsales_deal_note_id and reply.freshsales_contact_id:
        open_deal_id = find_open_deal_for_contact(crm, reply.freshsales_contact_id, stage_types)
        if open_deal_id:
            note_id = crm.create_note(
                description=note_description(reply), targetable_id=open_deal_id, targetable_type="Deal"
            )
            reply.freshsales_deal_id = open_deal_id
            reply.freshsales_deal_note_id = note_id
            reply.deal_created_at = timezone.now()
            reply.save(
                update_fields=["freshsales_deal_id", "freshsales_deal_note_id", "deal_created_at", "updated_at"]
            )
            counts["deal_notes_created"] += 1

    reply.contact_synced_at = timezone.now()
    reply.last_error = None
    reply.save(update_fields=["contact_synced_at", "last_error", "updated_at"])


# ---------------------------------------------------------------------------------------------
# Pass 3 -- refresh the label, then deals
# ---------------------------------------------------------------------------------------------


def refresh_interest(
    client: "instantly_client.InstantlyApiClient",
    crm: typing.Optional["freshsales_client.FreshsalesApiClient"] = None,
    recheck_days: int = 60,
    recheck_hours: int = 6,
    status_ids: typing.Optional[typing.Dict[str, int]] = None,
) -> typing.Dict[str, int]:
    """
    Read the lead behind each reply: its interest label, and its custom-variable payload.

    Two reasons a reply needs this, and it must be either, not both:

    * **It has no payload yet.** ``GET /emails`` carries no shop details at all -- company, city,
      phone and the rest live on the *lead*. Without this the contact is built from the address
      alone, and a shop that replied from gmail becomes a sales account named
      ``freetuck@gmail.com``. This applies whatever the label says, which is why a positive reply
      is not excluded here.
    * **Its label may have moved.** Instantly's AI labels most threads within minutes but not all
      -- 4 of 22 in the live account are still unlabelled, two of them a month old -- and a human
      relabelling in Unibox can change one at any time.

    One call per address, because the documented server-side interest filter is ignored by the live
    API. ``interest_checked_at`` throttles label re-reads to once per ``recheck_hours`` so a
    15-minute cron does not re-ask constantly, and ``recheck_days`` stops the walk going back
    forever. A reply that is already positive and already has its payload is left alone: its deal
    exists, and every positive value maps to the same CRM status, so a 1 -> 2 move changes nothing.

    When a label changes and ``crm`` is given, the already-synced contact's status is corrected too
    -- otherwise a thread relabelled in Unibox would read as "Contacted" in the CRM forever.
    """
    counts = {"checked": 0, "changed": 0, "became_positive": 0, "status_updated": 0, "failed": 0}
    now = timezone.now()

    stale_label = django_db_models.Q(is_positive=False) & (
        django_db_models.Q(interest_checked_at__isnull=True)
        | django_db_models.Q(interest_checked_at__lt=now - datetime.timedelta(hours=recheck_hours))
    )
    missing_payload = django_db_models.Q(lead_payload={}) | django_db_models.Q(lead_payload__isnull=True)

    queryset = (
        src_models.InstantlyReply.objects.filter(
            email_timestamp__gte=now - datetime.timedelta(days=recheck_days),
        )
        .exclude(lead_email="")
        .filter(stale_label | missing_payload)
        .order_by("email_timestamp")
    )

    # One lookup per address, not per reply: 22 replies came from 14 addresses in the live account,
    # and the label belongs to the lead.
    by_email: typing.Dict[str, typing.List[src_models.InstantlyReply]] = {}
    for reply in queryset:
        by_email.setdefault(reply.lead_email, []).append(reply)

    for lead_email, replies in by_email.items():
        try:
            lead = client.find_lead(lead_email)
        except instantly_exceptions.InstantlyRateLimited:
            raise
        except Exception as e:
            counts["failed"] += 1
            logger.warning(
                "{} Could not re-read the interest label for {}: {}".format(
                    _LOG_PREFIX, lead_email, common_utils.get_exception_message(exception=e)
                )
            )
            continue

        counts["checked"] += 1
        if lead is None:
            # Deleted from Instantly, or never a campaign lead. Stamp the check so it is not
            # re-asked every run.
            src_models.InstantlyReply.objects.filter(id__in=[r.id for r in replies]).update(
                interest_checked_at=now, updated_at=now
            )
            continue

        new_status = lead.get("lt_interest_status")
        payload = lead.get("payload") or {}

        for reply in replies:
            changed = new_status is not None and new_status != reply.interest_status
            reply.interest_checked_at = now
            if payload:
                reply.lead_payload = payload
            if changed:
                reply.interest_status = new_status
                reply.is_positive = is_positive_interest(new_status)
                reply.is_auto_reply = derive_is_auto_reply(new_status, reply.subject)
                counts["changed"] += 1
                if reply.is_positive:
                    counts["became_positive"] += 1
            reply.save(
                update_fields=[
                    "interest_checked_at",
                    "lead_payload",
                    "interest_status",
                    "is_positive",
                    "is_auto_reply",
                    "updated_at",
                ]
            )

            if (
                changed
                and crm is not None
                and reply.freshsales_contact_id
                and not platform_crm_sync.is_platform_contact(reply.lead_email)
            ):
                try:
                    ids = status_ids if status_ids is not None else crm.contact_status_ids_by_name()
                    status_id = crm.resolve_contact_status_id(contact_status_name(new_status), ids)
                    crm.upsert_contact(email=reply.lead_email, fields={"contact_status_id": status_id})
                    counts["status_updated"] += 1
                except Exception as e:
                    logger.warning(
                        "{} Relabelled {} but could not update its CRM status: {}".format(
                            _LOG_PREFIX, reply.lead_email, common_utils.get_exception_message(exception=e)
                        )
                    )

    return counts


def find_open_deal_for_contact(
    crm: "freshsales_client.FreshsalesApiClient",
    contact_id: str,
    stage_types: typing.Dict[int, int],
) -> typing.Optional[str]:
    """
    Step 1 of the cold-reply deal flow: does this contact already have a deal whose stage's
    ``forecast_type`` is not "Closed Won" or "Closed Lost"? If so, its id is returned and the
    caller must not create a second deal -- only add the reply as a note on this one instead (see
    :func:`create_deals`).

    A stage this account's selector does not recognise (``stage_types`` has no entry for it) is
    treated as open rather than skipped -- the safe failure here is "add a note to a deal that may
    already be closed", not "silently open a duplicate deal". Confirmed live 2026-10-19 against a
    real open deal (forecast_type "Open" on its stage) -- this must not create a second deal for
    that contact's next reply.
    """
    for deal in crm.get_contact_deals(contact_id):
        deal_id = deal.get("id")
        if not deal_id:
            continue
        stage_type = stage_types.get(deal.get("deal_stage_id"), "")
        if stage_type not in ("Closed Won", "Closed Lost"):
            return str(deal_id)
    return None


def _deal_custom_fields(reply: "src_models.InstantlyReply", conf: typing.Any) -> dict:
    """
    The dropdown-backed fields the rescue process reads from. Values for cf_inbound_source,
    cf_rescue_step and cf_assigned_to must match an existing FreshSales dropdown option exactly
    (configured in settings, not typed inline here, so a renamed option is one settings change,
    not a code change) -- see settings.FRESHSALES_DEAL_* and FreshsalesApiClient.create_deal's
    custom_field note.
    """
    return {
        "cf_inbound_source": conf.FRESHSALES_DEAL_SOURCE,
        "cf_source_campaign": reply.campaign_name or "",
        "cf_first_reply": (reply.body_text or "")[:250],
        "cf_unibox_link": unibox_url(reply) or "",
        "cf_rescue_step": conf.FRESHSALES_DEAL_RESCUE_STEP,
        "cf_next_step": conf.FRESHSALES_DEAL_NEXT_STEP,
        "cf_next_step_date": timezone.now().date().isoformat(),
        "cf_assigned_to": conf.FRESHSALES_DEAL_ASSIGNED_TO,
    }


def provision_platform_account_for_reply(
    reply: "src_models.InstantlyReply", crm: "freshsales_client.FreshsalesApiClient", deal_id: str
) -> typing.Optional[dict]:
    """
    "Day 0: create their account" (cf_next_step's own text) -- automated rather than done by
    hand, the moment this reply's deal is created.

    Checked first against :func:`platform_crm_sync.is_platform_contact` so this can never create a
    duplicate ``Company`` for someone who already has a real platform account (a genuine signup
    that beat the rescue process to it). Everything after that is best-effort and never raises: a
    failure provisioning the account, or recording it on the deal, must not undo the deal itself.

    Records the result on the deal exactly where the rescue process already looks for it --
    ``cf_platform_account_email`` / ``cf_account_created_date``, both real custom fields observed
    live on existing deals, not invented here -- plus a note carrying the generated password
    once. The password is never stored anywhere else (see InstantlyReply.platform_company_id's own
    docstring); a lost password is retrieved via the admin-panel reset-password endpoint, not by
    re-reading this note.

    Returns the created account dict (see admin_services.create_company_for_admin), or None when
    nothing was created.
    """
    from src.api.services import admin as admin_services

    if platform_crm_sync.is_platform_contact(reply.lead_email):
        logger.info(
            "{} {} already has a platform account -- skipping auto-provision.".format(
                _LOG_PREFIX, reply.lead_email
            )
        )
        return None

    company = company_name_for(reply.lead_email, reply.lead_payload, person_name=reply.from_name or "")
    first_name, last_name = split_display_name(reply.from_name or "", company)

    try:
        account = admin_services.create_company_for_admin(
            company_name=company,
            email=reply.lead_email,
            first_name=first_name,
            last_name=last_name,
        )
    except ValueError as e:
        # Most likely "a user with this email already exists" (a race with the
        # is_platform_contact check above, or an account created some other way) -- not worth
        # retrying with the same inputs.
        logger.warning(
            "{} Could not auto-provision a platform account for {}: {}".format(_LOG_PREFIX, reply.lead_email, e)
        )
        return None
    except Exception:
        logger.exception(
            "{} Unexpected error auto-provisioning a platform account for {}.".format(
                _LOG_PREFIX, reply.lead_email
            )
        )
        return None

    today = timezone.now().date().isoformat()
    try:
        crm.update_deal(
            deal_id, custom_field={"cf_platform_account_email": account["email"], "cf_account_created_date": today}
        )
    except Exception as e:
        logger.warning(
            "{} Platform account created for {} but updating deal {} failed: {}".format(
                _LOG_PREFIX, reply.lead_email, deal_id, common_utils.get_exception_message(exception=e)
            )
        )

    try:
        crm.create_note(
            description=(
                "AftermarketScout account auto-created.\n"
                "Login: {}\n"
                "Temporary password: {}\n"
                "Relay this to the customer (or they can use \"forgot password\") -- this is the "
                "only place this password is recorded.".format(account["email"], account["password"])
            ),
            targetable_id=deal_id,
            targetable_type="Deal",
        )
    except Exception as e:
        logger.warning(
            "{} Platform account created for {} but the credentials note on deal {} failed: {} -- "
            "use the admin-panel reset-password endpoint to get the customer a fresh password "
            "rather than retrying this.".format(
                _LOG_PREFIX, reply.lead_email, deal_id, common_utils.get_exception_message(exception=e)
            )
        )

    reply.platform_company_id = account["company_id"]
    reply.platform_user_id = account["user_id"]
    reply.platform_account_created_at = timezone.now()
    reply.save(
        update_fields=["platform_company_id", "platform_user_id", "platform_account_created_at", "updated_at"]
    )

    return account


def create_deals(
    crm: "freshsales_client.FreshsalesApiClient",
    instantly: "instantly_client.InstantlyApiClient",
    default_amount: typing.Union[int, float] = 0,
    limit: typing.Optional[int] = None,
    max_calls: typing.Optional[int] = None,
    max_attempts: int = 5,
    settings_module: typing.Any = None,
    provision_platform_accounts: bool = False,
) -> typing.Dict[str, int]:
    """
    One deal per positive reply whose contact has no open deal yet. Six steps, matching the
    rescue-process spec exactly:

      1. Check FreshSales directly (not just our own ``freshsales_deal_id``) for a deal already on
         this contact that is not Won/Lost -- someone who replies twice, or who already has an
         open deal from some other route into the CRM, is one opportunity, not two. If one exists,
         only add the reply as a note on it and stop.
      2+3. The sales account and contact already exist by this point -- pass 2
         (:func:`push_contacts`) created both before a reply can ever reach this function.
      4. Create the deal, pinned to the cold-reply pipeline/stage
         (settings.FRESHSALES_DEAL_PIPELINE_ID/_STAGE_ID) with the custom fields above. Deal value
         is left at ``default_amount`` (0 unless configured) -- it is set later, by hand, once they
         pick a plan.
      5. Remove the lead from its Instantly campaign so the cold sequence stops sending. Best
         effort: a failure here is logged and counted but does not undo the deal just created, and
         does not stop the pass -- a human can remove it from Instantly by hand.
      6. Add the full reply as a note on the deal (distinct from pass 2's note on the contact,
         which still happens separately and is not duplicated here).
      7. When ``provision_platform_accounts`` is on (its own flag, independent of
         ``FRESHSALES_CREATE_DEALS`` -- deals can be enabled while this stays off): auto-create an
         AftermarketScout account for the company ("Day 0: create their account" from
         cf_next_step, automated rather than done by hand) and record it on the deal. See
         :func:`provision_platform_account_for_reply`.
    """
    from django.conf import settings as django_settings

    conf = settings_module or django_settings

    counts = {
        "deals_created": 0,
        "skipped_existing_deal": 0,
        "skipped_no_contact": 0,
        "removed_from_campaign": 0,
        "campaign_removal_failed": 0,
        "platform_accounts_created": 0,
        "failed": 0,
    }

    # Resolved once per run, lazily -- an idle tick with nothing positive to do should not spend a
    # FreshSales call finding out that out.
    stage_types: typing.Optional[typing.Dict[int, str]] = None

    queryset = src_models.InstantlyReply.objects.filter(
        is_positive=True,
        deal_created_at__isnull=True,
        is_auto_reply=False,
        sync_attempts__lt=max_attempts,
    ).order_by("email_timestamp")
    if limit:
        queryset = queryset[:limit]

    for reply in queryset:
        if max_calls is not None and crm.calls_made >= max_calls:
            logger.warning(
                "{} Stopping deal creation: {} FreshSales calls used, budget is {}.".format(
                    _LOG_PREFIX, crm.calls_made, max_calls
                )
            )
            break

        if not reply.freshsales_contact_id or not reply.freshsales_account_id:
            # Pass 2 has not reached it yet (or it failed there). Next run.
            counts["skipped_no_contact"] += 1
            continue

        try:
            if stage_types is None:
                stage_types = crm.deal_stage_types_by_id()

            open_deal_id = find_open_deal_for_contact(crm, reply.freshsales_contact_id, stage_types)
            if open_deal_id:
                # push_contacts (pass 2, every reply) already posts this same reply onto an
                # already-open deal and sets freshsales_deal_note_id -- the deal_created_at__isnull
                # filter above normally keeps this queryset from ever reaching that case, but the
                # note itself is still guarded here rather than assumed, in case that ever drifts.
                if not reply.freshsales_deal_note_id:
                    note_id = crm.create_note(
                        description=note_description(reply), targetable_id=open_deal_id, targetable_type="Deal"
                    )
                    reply.freshsales_deal_note_id = note_id
                reply.freshsales_deal_id = open_deal_id
                reply.deal_created_at = timezone.now()
                reply.last_error = None
                reply.save(
                    update_fields=[
                        "freshsales_deal_id",
                        "freshsales_deal_note_id",
                        "deal_created_at",
                        "last_error",
                        "updated_at",
                    ]
                )
                counts["skipped_existing_deal"] += 1
                continue

            deal_id = crm.create_deal(
                name=deal_name(reply),
                amount=default_amount,
                sales_account_id=reply.freshsales_account_id,
                contact_id=reply.freshsales_contact_id,
                pipeline_id=conf.FRESHSALES_DEAL_PIPELINE_ID,
                stage_id=conf.FRESHSALES_DEAL_STAGE_ID,
                custom_field=_deal_custom_fields(reply, conf),
            )
        except (
            freshsales_exceptions.FreshsalesRateLimited,
            freshsales_exceptions.FreshsalesAuthError,
        ):
            raise
        except Exception as e:
            counts["failed"] += 1
            reply.sync_attempts += 1
            reply.last_error = common_utils.get_exception_message(exception=e)
            reply.save(update_fields=["sync_attempts", "last_error", "updated_at"])
            logger.exception("{} Could not create a deal for {}.".format(_LOG_PREFIX, reply.lead_email))
            continue

        reply.freshsales_deal_id = deal_id
        reply.deal_created_at = timezone.now()
        reply.last_error = None
        reply.save(update_fields=["freshsales_deal_id", "deal_created_at", "last_error", "updated_at"])
        counts["deals_created"] += 1

        # Step 5 -- stop the cold sequence. Best-effort: logged and counted, never raised, so a
        # campaign-removal failure cannot undo (or retry-loop) the deal just created.
        if reply.campaign_id:
            try:
                lead = instantly.find_lead(reply.lead_email)
                if lead and lead.get("id"):
                    instantly.remove_lead_from_campaign(str(lead["id"]), reply.campaign_id)
                    counts["removed_from_campaign"] += 1
                else:
                    counts["campaign_removal_failed"] += 1
                    logger.warning(
                        "{} Deal created for {} but its Instantly lead id could not be found -- "
                        "remove it from the campaign by hand.".format(_LOG_PREFIX, reply.lead_email)
                    )
            except Exception as e:
                counts["campaign_removal_failed"] += 1
                logger.warning(
                    "{} Deal created for {} but removing it from campaign {} failed: {} -- remove "
                    "it by hand in Instantly.".format(
                        _LOG_PREFIX,
                        reply.lead_email,
                        reply.campaign_id,
                        common_utils.get_exception_message(exception=e),
                    )
                )

        # Step 6 -- the full reply as a note on the new deal.
        try:
            note_id = crm.create_note(
                description=note_description(reply), targetable_id=deal_id, targetable_type="Deal"
            )
            reply.freshsales_deal_note_id = note_id
            reply.save(update_fields=["freshsales_deal_note_id", "updated_at"])
        except Exception as e:
            logger.warning(
                "{} Deal {} created for {} but its note failed: {}".format(
                    _LOG_PREFIX, deal_id, reply.lead_email, common_utils.get_exception_message(exception=e)
                )
            )

        # Step 7 -- "Day 0: create their account" (cf_next_step), automated. Never raises; see
        # provision_platform_account_for_reply's own docstring for why a failure here must not
        # undo the deal or stop the pass.
        if provision_platform_accounts and provision_platform_account_for_reply(reply, crm, deal_id):
            counts["platform_accounts_created"] += 1

    return counts


# ---------------------------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------------------------


def preview(
    client: "instantly_client.InstantlyApiClient",
    since: typing.Optional[datetime.datetime] = None,
    campaign_id: typing.Optional[str] = None,
    campaign_names: typing.Optional[typing.Dict[str, str]] = None,
) -> typing.Tuple[typing.List[dict], typing.Dict[str, int]]:
    """
    What a real run would do, without writing anything -- not to the CRM and not to our own tables.

    Reads Instantly and the existing ``InstantlyReply`` rows, then reports per reply what would
    happen. A true preview matters here because the first live run writes into a CRM that someone
    then has to look at.
    """
    rows = []
    counts = {
        "replies": 0,
        "new": 0,
        "already_stored": 0,
        "auto_replies": 0,
        "would_create_contacts": 0,
        "would_create_deals": 0,
        "no_email": 0,
    }
    counts["already_synced"] = 0
    # A preview that promises deals while the sync is configured not to create them is the same
    # class of lie as one that ignores the watermark.
    from django.conf import settings as django_settings

    create_deals_enabled = getattr(django_settings, "FRESHSALES_CREATE_DEALS", False)
    known = set(src_models.InstantlyReply.objects.values_list("instantly_email_id", flat=True))

    # What has already reached the CRM. A preview that ignores this reports work a real run would
    # not do -- after the backfill, "would create 1 contact" for a reply that was pushed last week.
    # The whole point of the flag is to answer "what happens if I run this", so it has to subtract
    # what is already done.
    synced_email_ids = set(
        src_models.InstantlyReply.objects.filter(contact_synced_at__isnull=False).values_list(
            "instantly_email_id", flat=True
        )
    )
    # Both counted per address, not per reply: contacts are upserted on the address and a shop that
    # replies three times is one contact and one opportunity. Counting rows here would promise 20
    # contacts for 13 people, which is exactly the number someone checks the CRM against. Seeded
    # with the addresses already in the CRM so a new reply from a known shop reads as a note.
    contact_addresses = set(
        src_models.InstantlyReply.objects.filter(freshsales_contact_id__isnull=False)
        .values_list("lead_email", flat=True)
        .distinct()
    )
    deal_addresses = set(
        src_models.InstantlyReply.objects.filter(freshsales_deal_id__isnull=False)
        .values_list("lead_email", flat=True)
        .distinct()
    )

    for email_payload in client.iter_received_emails(min_timestamp_created=_iso(since), campaign_id=campaign_id):
        fields = row_fields_from_email(email_payload, campaign_names)
        counts["replies"] += 1
        is_new = fields["instantly_email_id"] not in known
        counts["new" if is_new else "already_stored"] += 1

        action = []
        if fields["instantly_email_id"] in synced_email_ids:
            counts["already_synced"] += 1
            action.append("already synced")
        elif not fields["lead_email"]:
            counts["no_email"] += 1
            action.append("skip (no address)")
        elif fields["is_auto_reply"]:
            counts["auto_replies"] += 1
            action.append("skip (auto-reply)")
        elif fields["lead_email"] in contact_addresses:
            action.append("note on existing contact")
        else:
            action.append("contact")
            contact_addresses.add(fields["lead_email"])
            counts["would_create_contacts"] += 1

        if (
            create_deals_enabled
            and fields["is_positive"]
            and not fields["is_auto_reply"]
            and fields["lead_email"]
            and fields["instantly_email_id"] not in synced_email_ids
            and fields["lead_email"] not in deal_addresses
        ):
            action.append("deal")
            deal_addresses.add(fields["lead_email"])
            counts["would_create_deals"] += 1

        rows.append(
            {
                "lead_email": fields["lead_email"],
                "from_name": fields["from_name"] or "",
                "subject": (fields["subject"] or "")[:60],
                "interest": fields["interest_status"],
                "status": contact_status_name(fields["interest_status"]),
                "new": is_new,
                "action": " + ".join(action),
            }
        )

    return rows, counts


# ---------------------------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------------------------


def run(
    since: typing.Optional[datetime.datetime] = None,
    campaign_id: typing.Optional[str] = None,
    limit: typing.Optional[int] = None,
    recheck_days: typing.Optional[int] = None,
    max_attempts: int = 5,
    settings_module: typing.Any = None,
) -> typing.Dict[str, typing.Any]:
    """All three passes. Returns a flat summary, which is what the audit row's message records."""
    from django.conf import settings as django_settings

    conf = settings_module or django_settings

    instantly = instantly_client.InstantlyApiClient()
    crm = freshsales_client.FreshsalesApiClient()

    campaign_names = {}
    try:
        campaign_names = instantly.list_campaigns()
    except Exception as e:
        # Only used to name deals; a deal falls back to "<Company> — Instantly reply".
        logger.warning(
            "{} Could not list campaigns: {}".format(_LOG_PREFIX, common_utils.get_exception_message(exception=e))
        )

    start = (
        since
        if since is not None
        else watermark(
            overlap_minutes=conf.INSTANTLY_SYNC_OVERLAP_MINUTES,
            initial_days=conf.INSTANTLY_SYNC_INITIAL_DAYS,
        )
    )
    logger.info("{} Ingesting replies created at or after {}.".format(_LOG_PREFIX, _iso(start)))

    summary: typing.Dict[str, typing.Any] = {"since": _iso(start)}
    summary.update(
        ingest_replies(client=instantly, since=start, campaign_id=campaign_id, campaign_names=campaign_names)
    )

    status_ids = crm.contact_status_ids_by_name()
    summary.update(
        refresh_interest(
            client=instantly,
            crm=crm,
            recheck_days=recheck_days if recheck_days is not None else conf.INSTANTLY_INTEREST_RECHECK_DAYS,
            recheck_hours=conf.INSTANTLY_INTEREST_RECHECK_HOURS,
            status_ids=status_ids,
        )
    )
    summary.update(
        push_contacts(
            crm=crm,
            limit=limit,
            max_calls=conf.FRESHSALES_MAX_CALLS_PER_RUN,
            max_attempts=max_attempts,
            status_ids=status_ids,
        )
    )
    # Deals are created by hand. The contact still carries Instantly's verdict as its FreshSales
    # status, so "Interested" is the queue to work from; the sync does not open the opportunity.
    if getattr(conf, "FRESHSALES_CREATE_DEALS", False):
        summary.update(
            create_deals(
                crm=crm,
                instantly=instantly,
                default_amount=conf.FRESHSALES_DEFAULT_DEAL_AMOUNT,
                limit=limit,
                max_calls=conf.FRESHSALES_MAX_CALLS_PER_RUN,
                max_attempts=max_attempts,
                settings_module=conf,
                provision_platform_accounts=getattr(conf, "FRESHSALES_AUTO_PROVISION_PLATFORM_ACCOUNT", False),
            )
        )
    else:
        summary["deals_created"] = "disabled"
        summary["awaiting_manual_deal"] = (
            src_models.InstantlyReply.objects.filter(
                is_positive=True, is_auto_reply=False, freshsales_deal_id__isnull=True
            )
            .values("lead_email")
            .distinct()
            .count()
        )

    summary["freshsales_calls"] = crm.calls_made
    stuck = src_models.InstantlyReply.objects.filter(
        sync_attempts__gte=max_attempts, contact_synced_at__isnull=True
    ).count()
    if stuck:
        # Named in the summary so the audit row shows it without anyone reading logs.
        summary["stuck_over_max_attempts"] = stuck
    return summary


def rewrite_notes(
    crm: "freshsales_client.FreshsalesApiClient",
    limit: typing.Optional[int] = None,
    max_calls: typing.Optional[int] = None,
) -> typing.Dict[str, int]:
    """
    Rebuild the note body for replies already pushed, so older notes gain whatever the note now
    carries -- in practice the Unibox thread link, added after the first 20 notes were written.

    An update rather than a second note: the alternative is every contact carrying two notes that
    say the same thing, one of which has a link. ``PUT /notes/{id}`` is supported (see
    ``FreshsalesApiClient.update_note``), so the existing note is simply rewritten from the reply
    row we already store. Idempotent -- the description is regenerated from stored data, so running
    it twice writes the same bytes.
    """
    counts = {"rewritten": 0, "failed": 0}

    queryset = src_models.InstantlyReply.objects.exclude(freshsales_note_id=None).order_by("email_timestamp")
    if limit:
        queryset = queryset[:limit]

    for reply in queryset:
        if max_calls is not None and crm.calls_made >= max_calls:
            logger.warning("{} Stopping note rewrite: FreshSales call budget reached.".format(_LOG_PREFIX))
            break
        try:
            crm.update_note(note_id=reply.freshsales_note_id, description=note_description(reply))
        except (
            freshsales_exceptions.FreshsalesRateLimited,
            freshsales_exceptions.FreshsalesAuthError,
        ):
            raise
        except Exception as e:
            counts["failed"] += 1
            logger.warning(
                "{} Could not rewrite note {} for {}: {}".format(
                    _LOG_PREFIX,
                    reply.freshsales_note_id,
                    reply.lead_email,
                    common_utils.get_exception_message(exception=e),
                )
            )
            continue
        counts["rewritten"] += 1

    return counts
