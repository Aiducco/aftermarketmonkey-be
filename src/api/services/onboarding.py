"""
Onboarding flow services for B2B sign-up.

Steps 1+2 are atomic: User is never created without complete company details.
"""
import logging
import re
import uuid

from django.contrib.auth import models as auth_models
from django.db import transaction

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations.services import integration_pricing_sync_jobs

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ONBOARDING]"

# Business type display names
BUSINESS_TYPES = [
    {"value": "retail_store", "label": "Retail Store"},
    {"value": "installation_repair_shop", "label": "Installation/Repair Shop"},
    {"value": "ecommerce", "label": "E-commerce solely"},
    {"value": "dealership", "label": "Dealership"},
    {"value": "fleet_manager", "label": "Fleet Manager"},
]

# Top categories for personalization
TOP_CATEGORIES = [
    "Suspension/Lift Kits",
    "Tonneau Covers",
    "Lighting",
    "Exterior Armor",
    "Performance Tuning",
    "Wheels & Tires",
    "Interior Accessories",
    "Bed Accessories",
]


def _generate_slug(company_name: str) -> str:
    """Generate unique slug from company name."""
    base = re.sub(r"[^a-z0-9]+", "-", company_name.lower()).strip("-") or "company"
    return f"{base}-{uuid.uuid4().hex[:8]}"


@transaction.atomic
def register_user(
    first_name: str,
    last_name: str,
    email: str,
    password: str,
    company_name: str,
    business_type: str | None = None,
    country: str | None = None,
    state_province: str | None = None,
    tax_id: str | None = None,
) -> dict:
    """
    Step 1+2 (atomic): Create User, Company (with full details), and UserProfile.
    All or nothing - no partial state on failure.
    Returns login response with JWT.
    """
    email = email.strip().lower()
    company_name = company_name.strip()
    if not company_name:
        raise ValueError("Company name is required.")

    if auth_models.User.objects.filter(email=email).exists():
        raise ValueError("A user with this email already exists.")

    # Create company with full details (no placeholder)
    def _opt(val):
        return (val or "").strip() or None

    company = src_models.Company.objects.create(
        name=company_name,
        slug=_generate_slug(company_name),
        status=src_enums.CompanyStatus.ACTIVE.value,
        status_name=src_enums.CompanyStatus.ACTIVE.name,
        business_type=_opt(business_type),
        country=_opt(country),
        state_province=_opt(state_province),
        tax_id=_opt(tax_id),
        onboarding_step=2,  # Step 1+2 done; next is step 3 (personalization)
    )

    # Create user
    user = auth_models.User(
        username=email,
        email=email,
        first_name=first_name.strip(),
        last_name=last_name.strip(),
    )
    user.set_password(password)
    user.save()

    # Create UserProfile (signal may not be loaded; create explicitly and set as admin)
    profile, created = src_models.UserProfile.objects.get_or_create(
        user=user,
        defaults={"company": company, "is_company_admin": True},
    )
    if not created:
        profile.company = company
        profile.is_company_admin = True
        profile.save()

    # Create JWT (same as login)
    from src.authentication.services import create_jwt_token

    return {
        "user_id": user.id,
        "access_token": create_jwt_token(user=user),
        "company_id": company.id,
        "onboarding_step": 2,
        "is_company_admin": True,  # First user is always admin
    }


def update_company_details(
    company_id: int,
    company_name: str,
    business_type: str | None = None,
    country: str | None = None,
    state_province: str | None = None,
    tax_id: str | None = None,
) -> dict:
    """
    Step 2: Update company details.
    """
    company = src_models.Company.objects.get(id=company_id)
    company.name = company_name.strip()
    # Update slug to reflect company name (unique suffix ensures no collision)
    company.slug = _generate_slug(company_name)
    if business_type is not None:
        company.business_type = business_type.strip() or None
    if country is not None:
        company.country = country.strip() or None
    if state_province is not None:
        company.state_province = state_province.strip() or None
    if tax_id is not None:
        company.tax_id = tax_id.strip() or None
    company.onboarding_step = 2
    company.save()

    return {
        "company_id": company.id,
        "onboarding_step": 2,
    }


def save_personalization(
    company_id: int,
    preferred_distributor_ids: list | None = None,
    top_categories: list | None = None,
    distributor_credentials: dict | None = None,
) -> dict:
    """
    Step 3: Save preferences and optionally create CompanyProviders with credentials.

    distributor_credentials format:
    {
        "turn_14": {"client_id": "...", "client_secret": "..."},
        "keystone": {"ftp_user": "...", "ftp_password": "..."},
        "meyer": {"sftp_user": "...", "sftp_password": "..."},
        "atech": {"sftp_user": "...", "sftp_password": "..."},
        "dlg": {"email_from": "dealer@example.com"},
        "wheelpros": {
            "sftp_user": "...",
            "sftp_password": "...",
            "sftp_path": "optional remote CSV path",
        },
    }
    """
    company = src_models.Company.objects.get(id=company_id)

    # Ensure CompanyOnboardingPreferences exists
    prefs, _ = src_models.CompanyOnboardingPreferences.objects.get_or_create(
        company=company,
        defaults={
            "preferred_distributor_ids": [],
            "top_categories": [],
        },
    )

    if preferred_distributor_ids is not None:
        prefs.preferred_distributor_ids = [int(x) for x in preferred_distributor_ids]
    if top_categories is not None:
        prefs.top_categories = [str(x).strip() for x in top_categories if x]
    prefs.save()

    # Create/update CompanyProviders for distributors with credentials
    if distributor_credentials:
        _upsert_company_providers(company, distributor_credentials)

    company.onboarding_step = 4
    company.save()

    return {
        "company_id": company.id,
        "onboarding_step": 4,
    }


def _upsert_company_providers(company: src_models.Company, credentials: dict) -> None:
    """Create or update CompanyProviders from distributor_credentials."""
    # Turn14: kind=1 (TURN_14)
    if "turn_14" in credentials:
        creds = credentials["turn_14"]
        if creds.get("client_id") and creds.get("client_secret"):
            provider = src_models.Providers.objects.filter(
                kind=src_enums.BrandProviderKind.TURN_14.value
            ).first()
            if provider:
                cp, _ = src_models.CompanyProviders.objects.update_or_create(
                    company=company,
                    provider=provider,
                    defaults={
                        "credentials": {
                            "client_id": creds["client_id"],
                            "client_secret": creds["client_secret"],
                        },
                        "primary": True,
                    },
                )
                integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)

    # Keystone: kind=3 (KEYSTONE)
    if "keystone" in credentials:
        creds = credentials["keystone"]
        if creds.get("ftp_user") and creds.get("ftp_password"):
            provider = src_models.Providers.objects.filter(
                kind=src_enums.BrandProviderKind.KEYSTONE.value
            ).first()
            if provider:
                cp, _ = src_models.CompanyProviders.objects.update_or_create(
                    company=company,
                    provider=provider,
                    defaults={
                        "credentials": {
                            "ftp_user": creds["ftp_user"],
                            "ftp_password": creds["ftp_password"],
                        },
                        "primary": False,
                    },
                )
                integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)

    # Meyer: kind=6 (MEYER) — user/password; host/port/dir/files default from MEYER_SFTP_* settings in MeyerSFTPClient
    if "meyer" in credentials:
        creds = credentials["meyer"]
        user_ok = str(creds.get("sftp_user") or "").strip()
        pass_ok = str(creds.get("sftp_password") or "").strip()
        if user_ok and pass_ok:
            provider = src_models.Providers.objects.filter(
                kind=src_enums.BrandProviderKind.MEYER.value
            ).first()
            if provider:
                cred_dict = {
                    "sftp_user": user_ok,
                    "sftp_password": pass_ok,
                }
                cp, _ = src_models.CompanyProviders.objects.update_or_create(
                    company=company,
                    provider=provider,
                    defaults={
                        "credentials": cred_dict,
                        "primary": False,
                    },
                )
                integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)

    # A-Tech: kind=7 (ATECH) — user/password; host/port/dir/feed file from ATECH_SFTP_* / ATECH_FEED_REMOTE_FILE
    if "atech" in credentials:
        creds = credentials["atech"]
        user_ok = str(creds.get("sftp_user") or "").strip()
        pass_ok = str(creds.get("sftp_password") or "").strip()
        if user_ok and pass_ok:
            provider = src_models.Providers.objects.filter(
                kind=src_enums.BrandProviderKind.ATECH.value
            ).first()
            if provider:
                cred_dict = {
                    "sftp_user": user_ok,
                    "sftp_password": pass_ok,
                }
                cp, _ = src_models.CompanyProviders.objects.update_or_create(
                    company=company,
                    provider=provider,
                    defaults={
                        "credentials": cred_dict,
                        "primary": False,
                    },
                )
                integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)

    # DLG: kind=8 — email_from only; relay SFTP auth is in settings (DLG_RELAY_SFTP_USER / DLG_RELAY_SFTP_PASSWORD).
    if "dlg" in credentials:
        creds = credentials["dlg"]
        email_from_ok = str(creds.get(src_constants.DLG_CREDENTIALS_EMAIL_FROM) or "").strip()
        if email_from_ok:
            provider = src_models.Providers.objects.filter(
                kind=src_enums.BrandProviderKind.DLG.value
            ).first()
            if provider:
                cred_dict = {
                    src_constants.DLG_CREDENTIALS_EMAIL_FROM: email_from_ok,
                }
                cp, _ = src_models.CompanyProviders.objects.update_or_create(
                    company=company,
                    provider=provider,
                    defaults={
                        "credentials": cred_dict,
                        "primary": False,
                    },
                )
                integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)

    # Wheel Pros: SFTP user/password only; host/port from settings (WHEELPROS_SFTP_HOST / PORT).
    if "wheelpros" in credentials:
        creds = credentials["wheelpros"]
        if creds.get("sftp_user") and creds.get("sftp_password"):
            provider = src_models.Providers.objects.filter(
                kind=src_enums.BrandProviderKind.WHEELPROS.value
            ).first()
            if provider:
                cred_dict = {
                    "sftp_user": str(creds["sftp_user"]).strip(),
                    "sftp_password": str(creds["sftp_password"]).strip(),
                }
                v = creds.get("sftp_path")
                if v is not None and str(v).strip():
                    cred_dict["sftp_path"] = str(v).strip()
                cp, _ = src_models.CompanyProviders.objects.update_or_create(
                    company=company,
                    provider=provider,
                    defaults={
                        "credentials": cred_dict,
                        "primary": False,
                    },
                )
                integration_pricing_sync_jobs.enqueue_company_provider_pricing_sync(cp.id)


def get_onboarding_status(company_id: int) -> dict:
    """Get current onboarding status and available options."""
    try:
        company = src_models.Company.objects.get(id=company_id)
    except src_models.Company.DoesNotExist:
        return {"onboarding_step": 0, "company_id": None}

    # Integrations catalog providers available during onboarding (same kinds as full catalog subset)
    catalog_kinds = [e["kind"].value for e in src_constants.PROVIDER_CATALOG]
    providers = src_models.Providers.objects.filter(kind__in=catalog_kinds).values(
        "id", "name", "kind_name", "kind"
    )
    available_distributors = []
    for p in providers:
        row = dict(p)
        kn = row.get("kind_name") or ""
        row["display_name"] = src_constants.PROVIDER_DISPLAY_NAMES.get(kn, row.get("name"))
        icon = src_constants.PROVIDER_IMAGE_URLS.get(kn) or ""
        row["icon_url"] = icon if icon else None
        available_distributors.append(row)

    try:
        prefs = company.onboarding_preferences
        preferred_ids = prefs.preferred_distributor_ids
        top_cats = prefs.top_categories
    except src_models.CompanyOnboardingPreferences.DoesNotExist:
        preferred_ids = []
        top_cats = []

    return {
        "company_id": company.id,
        "onboarding_step": company.onboarding_step or 0,
        "company_name": company.name,
        "business_type": company.business_type,
        "country": company.country,
        "state_province": company.state_province,
        "preferred_distributor_ids": preferred_ids,
        "top_categories": top_cats,
        "available_distributors": available_distributors,
        "business_types": BUSINESS_TYPES,
        "categories_options": TOP_CATEGORIES,
    }


def get_distributor_credentials_info() -> dict:
    """Return what credentials each distributor needs (for API docs / frontend)."""
    return {
        "turn_14": {
            "required": ["client_id", "client_secret"],
            "description": "OAuth2 credentials from Turn 14 API access",
            "display_name": src_constants.PROVIDER_DISPLAY_NAMES.get("TURN_14", "Turn 14"),
            "icon_url": src_constants.PROVIDER_IMAGE_URLS.get("TURN_14") or None,
        },
        "keystone": {
            "required": ["ftp_user", "ftp_password"],
            "description": "FTP credentials for Keystone inventory access",
            "display_name": src_constants.PROVIDER_DISPLAY_NAMES.get("KEYSTONE", "Keystone"),
            "icon_url": src_constants.PROVIDER_IMAGE_URLS.get("KEYSTONE") or None,
        },
        "rough_country": {
            "required": [src_constants.ROUGH_COUNTRY_CREDENTIALS_FEED_URL],
            "description": (
                "Rough Country jobber Excel URL (feed_url) per company. "
                "Catalog uses the primary connection; pricing loads each company's feed_url."
            ),
            "display_name": src_constants.PROVIDER_DISPLAY_NAMES.get("ROUGH_COUNTRY", "Rough Country"),
            "icon_url": src_constants.PROVIDER_IMAGE_URLS.get("ROUGH_COUNTRY") or None,
        },
        "wheelpros": {
            "required": [
                "sftp_user",
                "sftp_password",
                "wheel_markup",
                "tire_markup",
                "accessories_markup",
            ],
            "optional": ["sftp_path"],
            "description": (
                "Wheel Pros SFTP username and password (host sftp.wheelpros.com is fixed). "
                "wheel_markup, tire_markup, accessories_markup: percent off MSRP (0–100) per feed; "
                "default 20% per feed if not set. Optional sftp_path overrides default remote CSV paths."
            ),
            "display_name": src_constants.PROVIDER_DISPLAY_NAMES.get("WHEELPROS", "Wheel Pros"),
            "icon_url": src_constants.PROVIDER_IMAGE_URLS.get("WHEELPROS") or None,
        },
        "meyer": {
            "required": ["sftp_user", "sftp_password"],
            "description": (
                "Email info@aftermarketmonkey.com for SFTP credentials, then enter sftp_user and sftp_password. "
                "Meyer data is delivered to AftermarketMonkey's relay; your rep uses host 54.145.82.238, port 22, "
                "folder uploads, files Meyer Pricing.csv and Meyer Inventory.csv."
            ),
            "display_name": src_constants.PROVIDER_DISPLAY_NAMES.get("MEYER", "Meyer"),
            "icon_url": src_constants.PROVIDER_IMAGE_URLS.get("MEYER") or None,
        },
        "atech": {
            "required": ["sftp_user", "sftp_password"],
            "description": (
                "Email info@aftermarketmonkey.com for SFTP credentials, then enter sftp_user and sftp_password. "
                "A-Tech sends one combined catalog and pricing feed to AftermarketMonkey's relay "
                "(host 54.145.82.238, port 22, folder uploads): part data; cost, retail, and jobber; on-hand per DC "
                "(Tallmadge OH, Sparks NV, McDonough GA, Arlington TX); core, hazmat, and handling-related fees; GTIN. "
                "Optional overrides for host, port, folder, or remote filename may be set in credentials when needed."
            ),
            "display_name": src_constants.PROVIDER_DISPLAY_NAMES.get("ATECH", "A-Tech"),
            "icon_url": src_constants.PROVIDER_IMAGE_URLS.get("ATECH") or None,
        },
        "dlg": {
            "required": [src_constants.DLG_CREDENTIALS_EMAIL_FROM],
            "description": (
                "DLG emails the inventory CSV to you; forward to "
                + src_constants.DLG_INVENTORY_FORWARD_TO_EMAIL
                + ". Save "
                + src_constants.DLG_CREDENTIALS_EMAIL_FROM
                + " (the address DLG uses). Ingest does not use per-company SFTP credentials; relay login is in server settings."
            ),
            "display_name": src_constants.PROVIDER_DISPLAY_NAMES.get("DLG", "DLG"),
            "icon_url": src_constants.PROVIDER_IMAGE_URLS.get("DLG") or None,
        },
    }
