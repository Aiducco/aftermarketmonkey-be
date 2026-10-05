"""
Turns Instantly replies into FreshSales contacts, and Instantly's own positive labels into deals.

Run by ``manage.py sync_instantly_replies`` on a 15-minute cron. Three passes, each independently
re-runnable, because the useful failure mode is "stopped halfway and resumed", not "rolled back":

1. :func:`ingest_replies`  -- read inbound emails from Instantly into ``InstantlyReply``. Writes
   nothing to the CRM, so a FreshSales outage cannot lose a reply.
2. :func:`push_contacts`   -- sales account, contact, note for every human reply not yet pushed.
3. :func:`refresh_interest` then :func:`create_deals` -- re-read the interest label, then create a
   deal for whatever is positive and has no deal yet.

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


def company_name_for(lead_email: str, lead_payload: typing.Optional[dict] = None) -> str:
    """
    The shop's name: Instantly's ``companyName`` custom variable, else the reply's domain.

    The domain fallback never fires for a free mailbox -- those fall back to the address itself, so
    fourteen unrelated gmail repliers do not all land under one "gmail.com" account.
    """
    payload = lead_payload or {}
    for key in ("companyName", "company_name"):
        value = (payload.get(key) or "").strip()
        if value:
            return value
    domain = domain_of(lead_email)
    if domain and domain not in _GENERIC_EMAIL_DOMAINS:
        return domain
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
    company = company_name_for(reply.lead_email, payload)
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
    company = company_name_for(reply.lead_email, reply.lead_payload)
    if reply.campaign_name:
        return "{} — {}".format(company, reply.campaign_name)
    return "{} — Instantly reply".format(company)


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
    """
    counts = {
        "contacts_created": 0,
        "contacts_updated": 0,
        "notes_created": 0,
        "skipped_no_email": 0,
        "failed": 0,
        "budget_stopped": 0,
    }

    # Reuse the caller's resolution when there is one: an idle tick would otherwise spend two
    # FreshSales calls on the same selector just to find there is nothing to push.
    if status_ids is None:
        status_ids = crm.contact_status_ids_by_name()

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
            _push_one_contact(crm, reply, status_ids, counts)
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
    counts: typing.Dict[str, int],
) -> None:
    company = company_name_for(reply.lead_email, reply.lead_payload)

    if not reply.freshsales_account_id:
        account_id, _ = crm.upsert_sales_account(name=company, fields=sales_account_fields(reply))
        reply.freshsales_account_id = account_id
        reply.save(update_fields=["freshsales_account_id", "updated_at"])

    if not reply.freshsales_contact_id:
        status_id = crm.resolve_contact_status_id(contact_status_name(reply.interest_status), status_ids)
        fields = contact_fields(reply, status_id)
        # FreshSales ids are integers; we store them as text (they are opaque to us), so the
        # association is cast back rather than sent as a string the API may not match.
        fields["sales_accounts"] = [{"id": _as_id(reply.freshsales_account_id), "is_primary": True}]
        contact_id, created = crm.upsert_contact(email=reply.lead_email, fields=fields)
        reply.freshsales_contact_id = contact_id
        reply.save(update_fields=["freshsales_contact_id", "updated_at"])
        counts["contacts_created" if created else "contacts_updated"] += 1

    if not reply.freshsales_note_id:
        note_id = crm.create_note(description=note_description(reply), contact_id=reply.freshsales_contact_id)
        reply.freshsales_note_id = note_id
        reply.save(update_fields=["freshsales_note_id", "updated_at"])
        counts["notes_created"] += 1

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

            if changed and crm is not None and reply.freshsales_contact_id:
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


def create_deals(
    crm: "freshsales_client.FreshsalesApiClient",
    default_amount: typing.Union[int, float] = 0,
    limit: typing.Optional[int] = None,
    max_calls: typing.Optional[int] = None,
    max_attempts: int = 5,
) -> typing.Dict[str, int]:
    """
    One deal per positive reply that has no deal yet, skipping addresses that already have one.

    **One deal per address, not per reply.** Someone who replies twice is one opportunity, and the
    live account has 22 replies from 14 addresses, so without this the pipeline would double-count.
    The guard is a query rather than a constraint because the first deal may have been created in
    an earlier run.
    """
    counts = {"deals_created": 0, "skipped_existing_deal": 0, "skipped_no_contact": 0, "failed": 0}

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

        existing = (
            src_models.InstantlyReply.objects.filter(lead_email=reply.lead_email, freshsales_deal_id__isnull=False)
            .exclude(id=reply.id)
            .values_list("freshsales_deal_id", flat=True)
            .first()
        )
        if existing:
            reply.freshsales_deal_id = existing
            reply.deal_created_at = timezone.now()
            reply.save(update_fields=["freshsales_deal_id", "deal_created_at", "updated_at"])
            counts["skipped_existing_deal"] += 1
            continue

        try:
            deal_id = crm.create_deal(
                name=deal_name(reply),
                amount=default_amount,
                sales_account_id=reply.freshsales_account_id,
                contact_id=reply.freshsales_contact_id,
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
            fields["is_positive"]
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
    summary.update(
        create_deals(
            crm=crm,
            default_amount=conf.FRESHSALES_DEFAULT_DEAL_AMOUNT,
            limit=limit,
            max_calls=conf.FRESHSALES_MAX_CALLS_PER_RUN,
            max_attempts=max_attempts,
        )
    )

    summary["freshsales_calls"] = crm.calls_made
    stuck = src_models.InstantlyReply.objects.filter(
        sync_attempts__gte=max_attempts, contact_synced_at__isnull=True
    ).count()
    if stuck:
        # Named in the summary so the audit row shows it without anyone reading logs.
        summary["stuck_over_max_attempts"] = stuck
    return summary
