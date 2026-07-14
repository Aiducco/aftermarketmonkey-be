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

# User role display names (job function within the company)
USER_ROLES = [
    {"value": "owner", "label": "Owner"},
    {"value": "parts_manager", "label": "Parts Manager"},
    {"value": "service_advisor", "label": "Service Advisor"},
    {"value": "technician", "label": "Technician"},
    {"value": "other", "label": "Other"},
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
    role: str | None = None,
    business_type: list | None = None,
    country: str | None = None,
    state_province: str | None = None,
    city: str | None = None,
    postal_code: str | None = None,
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
        business_type=[str(bt).strip() for bt in (business_type or []) if str(bt).strip()],
        country=_opt(country),
        state_province=_opt(state_province),
        city=_opt(city),
        postal_code=_opt(postal_code),
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
        defaults={"company": company, "is_company_admin": True, "role": _opt(role)},
    )
    if not created:
        profile.company = company
        profile.is_company_admin = True
        profile.role = _opt(role)
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
    business_type: list | None = None,
    country: str | None = None,
    state_province: str | None = None,
    city: str | None = None,
    postal_code: str | None = None,
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
        company.business_type = [str(bt).strip() for bt in business_type if str(bt).strip()]
    if country is not None:
        company.country = country.strip() or None
    if state_province is not None:
        company.state_province = state_province.strip() or None
    if city is not None:
        company.city = city.strip() or None
    if postal_code is not None:
        company.postal_code = postal_code.strip() or None
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
) -> dict:
    """
    Step 3: Save distributor/category preferences.

    This stores intent only (which distributors/categories the company cares about)
    to personalize search & prompts. It does not create real distributor connections —
    unlocking live pricing/inventory requires connecting real credentials via the
    integrations flow (POST /integrations/catalog/<id>/connect/), handled separately
    from onboarding.
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

    company.onboarding_step = 4
    company.save()

    return {
        "company_id": company.id,
        "onboarding_step": 4,
    }


def get_onboarding_status(company_id: int, user=None) -> dict:
    """Get current onboarding step, saved details, and available options."""
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

    role = None
    if user is not None and getattr(user, "is_authenticated", False):
        role = (
            src_models.UserProfile.objects.filter(user=user, company_id=company_id)
            .values_list("role", flat=True)
            .first()
        )

    return {
        "company_id": company.id,
        "onboarding_step": company.onboarding_step or 0,
        "company_name": company.name,
        "business_type": company.business_type,
        "country": company.country,
        "state_province": company.state_province,
        "city": company.city,
        "postal_code": company.postal_code,
        "role": role,
        "preferred_distributor_ids": preferred_ids,
        "top_categories": top_cats,
        "available_distributors": available_distributors,
        "business_types": BUSINESS_TYPES,
        "categories_options": TOP_CATEGORIES,
        "roles": USER_ROLES,
    }


def get_distributor_credentials_info() -> dict:
    """Return what credentials each distributor needs (for the post-onboarding integrations connect flow)."""
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
                "Email info@aftermarketscout.com for SFTP credentials, then enter sftp_user and sftp_password. "
                "Meyer data is delivered to AfterMarketScout's relay; your rep uses host 5.161.121.143, port 22, "
                "folder uploads, files Meyer Pricing.csv and Meyer Inventory.csv."
            ),
            "display_name": src_constants.PROVIDER_DISPLAY_NAMES.get("MEYER", "Meyer"),
            "icon_url": src_constants.PROVIDER_IMAGE_URLS.get("MEYER") or None,
        },
        "atech": {
            "required": ["sftp_user", "sftp_password"],
            "description": (
                "Email info@aftermarketscout.com for SFTP credentials, then enter sftp_user and sftp_password. "
                "A-Tech sends one combined catalog and pricing feed to AfterMarketScout's relay "
                "(host 5.161.121.143, port 22, folder uploads): part data; cost, retail, and jobber; on-hand per DC "
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
