"""
Mirrors platform signups into FreshSales: a ``Company`` becomes a sales account, each of its users
a contact. Run by ``manage.py sync_platform_signups`` on a cron.

The point is one CRM that holds everybody, however they arrived. ``instantly_freshsales_sync``
covers people who answered a cold email; this covers people who signed up through any other
channel. They overlap in practice -- ``frontlineoutfitter@gmail.com`` is both an Instantly reply
labelled Interested *and* a completed signup as "Frontline Off-Road" -- and §Collisions below is
how that is kept to one account and one contact rather than two of each.

**A mirror, not a dependency.** Nothing in the product reads the ``freshsales_*`` columns, signup
never waits on this, and a CRM outage is invisible to users. Which is also why it is a cron rather
than a ``post_save`` signal: a signal would put a third-party HTTP call in the signup path, where
its failure is either silent or user-facing, and neither is acceptable for a CRM nicety.

**Who is excluded.** Our own accounts. Two mechanisms, because one is not enough:

* ``Company.is_internal`` -- set by hand, and by migration 0206 for the ten companies that existed
  then. Needed for cases no rule can see: "Trident Motorsports" is our own entity but one of its
  three users signed up with a gmail address.
* ``FRESHSALES_INTERNAL_EMAIL_DOMAINS`` -- a company whose *every* user sits on one of our domains
  is skipped, so a new staff or pentest signup is excluded without anyone remembering the flag.
  Applied per user too: a support address added to a real customer's company is not a contact.

**Status.** A signup lands as ``Qualified`` (Sales Qualified Lead), one stage above the Lead-stage
statuses the Instantly sync writes -- signing up is a stronger signal than replying. So where both
apply, platform status wins: see :func:`is_platform_contact`, which the Instantly sync consults
before it would otherwise demote a contact back to "Interested" on a relabel.
"""
import logging
import typing

from django.conf import settings
from django.db import models as django_db_models
from django.utils import timezone

from common import utils as common_utils
from src import models as src_models
from src.integrations.clients.freshsales import client as freshsales_client
from src.integrations.clients.freshsales import exceptions as freshsales_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[PLATFORM-CRM]"

TASK_NAME = "sync_platform_signups"


# ---------------------------------------------------------------------------------------------
# Pure helpers. No DB, no HTTP.
# ---------------------------------------------------------------------------------------------


def internal_domains() -> typing.FrozenSet[str]:
    return frozenset(getattr(settings, "FRESHSALES_INTERNAL_EMAIL_DOMAINS", frozenset()))


def is_internal_email(email: str, domains: typing.Optional[typing.Iterable[str]] = None) -> bool:
    """Whether this address is one of ours rather than a customer's."""
    known = frozenset(domains) if domains is not None else internal_domains()
    return (email or "").strip().lower().rpartition("@")[2] in known


def account_fields(company: "src_models.Company") -> dict:
    """
    Sales-account body for a company. Onboarding's own address fields, nothing inferred.

    ``business_type`` is a list of slugs (e.g. ``["retail_store", "dealership"]``); it is flattened
    into a readable string rather than sent raw, since nothing in the CRM knows our slugs.
    """
    fields = {
        "name": company.name,
        "city": company.city or None,
        "state": company.state_province or None,
        "zipcode": company.postal_code or None,
        "country": company.country or None,
    }
    return {key: value for key, value in fields.items() if value}


def contact_fields(
    profile: "src_models.UserProfile",
    contact_status_id: int,
) -> dict:
    """
    Contact body for a platform user.

    Names come from the ``User`` record the person filled in themselves, so unlike the Instantly
    path there is nothing to guess at -- and an empty name is left out rather than sent as "",
    which would overwrite whatever a human has since typed into the CRM.
    """
    user = profile.user
    company = profile.company
    fields = {
        "first_name": (user.first_name or "").strip(),
        "last_name": (user.last_name or "").strip(),
        "contact_status_id": contact_status_id,
        "job_title": (profile.role or "").replace("_", " ").title() or None,
        "city": (company.city or None) if company else None,
        "state": (company.state_province or None) if company else None,
        "zipcode": (company.postal_code or None) if company else None,
        "country": (company.country or None) if company else None,
    }
    return {key: value for key, value in fields.items() if value not in (None, "")}


def note_description(profile: "src_models.UserProfile") -> str:
    """Why this contact exists, and where to find them in the product."""
    company = profile.company
    user = profile.user
    lines = [
        "Signed up on the AfterMarketScout platform",
        "",
        "Company:    {}".format(company.name if company else "(none)"),
        "User:       {} <{}>".format((user.get_full_name() or "").strip(), user.email).strip(),
        "Role:       {}".format(profile.role or "not set"),
        "Admin:      {}".format("yes" if profile.is_company_admin else "no"),
        "Signed up:  {}".format(user.date_joined.isoformat() if user.date_joined else "unknown"),
    ]
    if company:
        lines += [
            "Onboarding: step {} of 4".format(company.onboarding_step),
            "Plan:       {}".format(company.subscription_plan or "none"),
        ]
        if company.subscription_status:
            lines += ["Billing:    {}".format(company.subscription_status)]
        if company.business_type:
            lines += ["Business:   {}".format(", ".join(str(b).replace("_", " ") for b in company.business_type))]
        lines += ["", "Company id: {} (slug {})".format(company.id, company.slug)]
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------------------------


def syncable_companies() -> "django_db_models.QuerySet":
    """
    Companies worth mirroring: not ours, and far enough through onboarding to be real.

    The ``is_internal`` and onboarding filters are SQL; the all-users-on-an-internal-domain rule
    is not, because it depends on the company's users -- :func:`syncable_profiles_by_company`
    applies it while grouping, which is the same pass that has the emails to hand.
    """
    return src_models.Company.objects.filter(
        is_internal=False,
        onboarding_step__gte=settings.FRESHSALES_SIGNUP_MIN_ONBOARDING_STEP,
    ).order_by("created_at")


def syncable_profiles_by_company() -> typing.Dict[int, typing.List["src_models.UserProfile"]]:
    """
    ``{company_id: [profile, ...]}`` for every company in scope, keeping only contactable users.

    A company with no contactable users is dropped entirely rather than becoming an empty sales
    account: an account nobody is attached to is not a lead, it is clutter. That also handles the
    all-users-internal case without a separate query -- if every user is filtered out, so is the
    company.
    """
    domains = internal_domains()
    company_ids = set(syncable_companies().values_list("id", flat=True))

    profiles = (
        src_models.UserProfile.objects.filter(company_id__in=company_ids)
        .exclude(user__email="")
        .select_related("user", "company")
        .order_by("-is_company_admin", "user_id")
    )

    grouped: typing.Dict[int, typing.List[src_models.UserProfile]] = {}
    for profile in profiles:
        if is_internal_email(profile.user.email, domains):
            continue
        grouped.setdefault(profile.company_id, []).append(profile)
    return grouped


def is_platform_contact(email: str) -> bool:
    """
    Whether this address belongs to a synced platform signup.

    Consulted by ``instantly_freshsales_sync`` before it writes a contact status: a signup sits at
    ``Qualified`` in the Sales Qualified Lead stage, and a later Instantly relabel must not drag it
    back down to a Lead-stage status. Platform state is the stronger signal, so it wins.
    """
    if not (email or "").strip():
        return False
    return src_models.UserProfile.objects.filter(
        user__email__iexact=email.strip(),
        freshsales_synced_at__isnull=False,
    ).exists()


def _reusable_account_id(emails: typing.Sequence[str]) -> typing.Optional[str]:
    """
    A sales account the Instantly sync already created for one of these people, if any.

    Reused rather than creating a second account for the same business under a different name --
    Instantly knew one shop as "Frontline Outfitters" while the platform knows it as "Frontline
    Off-Road". One business, one account. The existing account keeps its name: renaming it would
    silently rewrite a record a human may have since edited, and the platform company name is
    recorded in the contact's note either way.
    """
    if not emails:
        return None
    existing = (
        src_models.InstantlyReply.objects.filter(
            lead_email__in=[e.strip() for e in emails if (e or "").strip()],
            freshsales_account_id__isnull=False,
        )
        .values_list("freshsales_account_id", flat=True)
        .first()
    )
    return existing or None


# ---------------------------------------------------------------------------------------------
# The sync
# ---------------------------------------------------------------------------------------------


def run(
    limit: typing.Optional[int] = None,
    company_id: typing.Optional[int] = None,
    max_calls: typing.Optional[int] = None,
) -> typing.Dict[str, typing.Any]:
    """
    Push every in-scope company and its users. Returns a flat summary for the audit row.

    Per company: upsert the sales account, then each user as a contact with a note. Every id is
    saved the moment the CRM returns it, so a crash resumes rather than repeating, and a company
    that fails is recorded on its own row and the run continues to the next.
    """
    crm = freshsales_client.FreshsalesApiClient()
    status_ids = crm.contact_status_ids_by_name()
    signup_status_id = crm.resolve_contact_status_id(settings.FRESHSALES_SIGNUP_CONTACT_STATUS, status_ids)

    grouped = syncable_profiles_by_company()
    companies = {c.id: c for c in syncable_companies()}
    if company_id:
        companies = {cid: c for cid, c in companies.items() if cid == company_id}

    summary = {
        "companies_in_scope": len(companies),
        "accounts_created": 0,
        "accounts_reused": 0,
        "accounts_existing": 0,
        "contacts_created": 0,
        "contacts_updated": 0,
        "notes_created": 0,
        "companies_skipped_no_users": 0,
        "failed": 0,
    }

    pushed = 0
    for cid, company in companies.items():
        profiles = grouped.get(cid) or []
        if not profiles:
            summary["companies_skipped_no_users"] += 1
            continue
        if limit is not None and pushed >= limit:
            break
        if max_calls is not None and crm.calls_made >= max_calls:
            logger.warning(
                "{} Stopping: {} FreshSales calls used, budget {}. Resumes next run.".format(
                    _LOG_PREFIX, crm.calls_made, max_calls
                )
            )
            break

        try:
            _push_company(crm, company, profiles, signup_status_id, summary)
            pushed += 1
        except (
            freshsales_exceptions.FreshsalesRateLimited,
            freshsales_exceptions.FreshsalesAuthError,
            freshsales_exceptions.FreshsalesConfigError,
        ):
            # Account-wide: every remaining company would fail the same way.
            raise
        except Exception as e:
            summary["failed"] += 1
            company.freshsales_last_error = common_utils.get_exception_message(exception=e)
            company.save(update_fields=["freshsales_last_error", "updated_at"])
            logger.exception("{} Company {} ({}) failed to sync.".format(_LOG_PREFIX, company.id, company.name))

    summary["freshsales_calls"] = crm.calls_made
    return summary


def _push_company(
    crm: "freshsales_client.FreshsalesApiClient",
    company: "src_models.Company",
    profiles: typing.Sequence["src_models.UserProfile"],
    signup_status_id: int,
    summary: typing.Dict[str, typing.Any],
) -> None:
    emails = [p.user.email for p in profiles]

    if not company.freshsales_account_id:
        reused = _reusable_account_id(emails)
        if reused:
            company.freshsales_account_id = reused
            summary["accounts_reused"] += 1
            logger.info(
                "{} Company {!r} reuses sales account {} created from an Instantly reply.".format(
                    _LOG_PREFIX, company.name, reused
                )
            )
        else:
            account_id, created = crm.upsert_sales_account(name=company.name, fields=account_fields(company))
            company.freshsales_account_id = account_id
            summary["accounts_created" if created else "accounts_existing"] += 1
        company.save(update_fields=["freshsales_account_id", "updated_at"])

    for profile in profiles:
        if profile.freshsales_contact_id and profile.freshsales_synced_at:
            continue
        try:
            _push_profile(crm, profile, company, signup_status_id, summary)
        except (
            freshsales_exceptions.FreshsalesRateLimited,
            freshsales_exceptions.FreshsalesAuthError,
        ):
            raise
        except Exception as e:
            summary["failed"] += 1
            profile.freshsales_last_error = common_utils.get_exception_message(exception=e)
            profile.save(update_fields=["freshsales_last_error", "updated_at"])
            logger.exception("{} User {} failed to sync.".format(_LOG_PREFIX, profile.user.email))

    company.freshsales_synced_at = timezone.now()
    company.freshsales_last_error = None
    company.save(update_fields=["freshsales_synced_at", "freshsales_last_error", "updated_at"])


def _push_profile(
    crm: "freshsales_client.FreshsalesApiClient",
    profile: "src_models.UserProfile",
    company: "src_models.Company",
    signup_status_id: int,
    summary: typing.Dict[str, typing.Any],
) -> None:
    fields = contact_fields(profile, signup_status_id)
    fields["sales_accounts"] = [{"id": _as_id(company.freshsales_account_id), "is_primary": True}]

    if not profile.freshsales_contact_id:
        contact_id, created = crm.upsert_contact(email=profile.user.email, fields=fields)
        profile.freshsales_contact_id = contact_id
        profile.save(update_fields=["freshsales_contact_id", "updated_at"])
        summary["contacts_created" if created else "contacts_updated"] += 1
        # A note only on first sight: it records the signup, which does not happen twice.
        crm.create_note(description=note_description(profile), contact_id=contact_id)
        summary["notes_created"] += 1
    else:
        # Contact exists but was never marked synced -- a previous run died between the two writes.
        crm.upsert_contact(email=profile.user.email, fields=fields)
        summary["contacts_updated"] += 1

    profile.freshsales_synced_at = timezone.now()
    profile.freshsales_last_error = None
    profile.save(update_fields=["freshsales_synced_at", "freshsales_last_error", "updated_at"])


def _as_id(value: typing.Optional[str]) -> typing.Union[int, str, None]:
    """A stored FreshSales id back as the integer the API issued, or unchanged if it is not one."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def preview() -> typing.Tuple[typing.List[dict], typing.Dict[str, int]]:
    """What a real run would do, reading only. Writes nothing to the CRM or to our own tables."""
    grouped = syncable_profiles_by_company()
    rows = []
    counts = {
        "companies": 0,
        "accounts_to_create": 0,
        "accounts_reused_from_instantly": 0,
        "accounts_already_linked": 0,
        "contacts_to_create": 0,
        "contacts_already_synced": 0,
        "companies_skipped_no_users": 0,
    }

    for company in syncable_companies():
        profiles = grouped.get(company.id) or []
        if not profiles:
            counts["companies_skipped_no_users"] += 1
            continue
        counts["companies"] += 1

        if company.freshsales_account_id:
            account = "linked ({})".format(company.freshsales_account_id)
            counts["accounts_already_linked"] += 1
        elif _reusable_account_id([p.user.email for p in profiles]):
            account = "reuse from Instantly"
            counts["accounts_reused_from_instantly"] += 1
        else:
            account = "create"
            counts["accounts_to_create"] += 1

        for profile in profiles:
            synced = bool(profile.freshsales_synced_at)
            counts["contacts_already_synced" if synced else "contacts_to_create"] += 1
            rows.append(
                {
                    "company": company.name,
                    "email": profile.user.email,
                    "name": (profile.user.get_full_name() or "").strip(),
                    "role": profile.role or "",
                    "account": account,
                    "contact": "already synced" if synced else "create",
                }
            )
    return rows, counts
