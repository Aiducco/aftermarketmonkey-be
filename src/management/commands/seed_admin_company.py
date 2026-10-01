"""
Seed the internal "AftermarketScout" admin company and its first staff user.

Staff users are grouped under this one real Company row (rather than company=null) so nothing
elsewhere that assumes every user has a company needs a carve-out. Actual admin-panel
authorization is `request.user.is_staff` (Django's own flag, checked against the live DB value by
JWTAuthenticationMiddleware) — membership in this company is purely organizational, not itself an
authorization check. The admin company is excluded from the admin panel's own "all companies" list
(see admin_services.ADMIN_COMPANY_SLUG).

Idempotent — safe to rerun (get_or_create throughout).

Usage:
    python manage.py seed_admin_company
    python manage.py seed_admin_company --email someone-else@aftermarketscout.com --password changeme123!
"""

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand

from src import enums as src_enums
from src.models import Company, UserProfile

ADMIN_COMPANY_NAME = "AftermarketScout"
ADMIN_COMPANY_SLUG = "aftermarketscout"


class Command(BaseCommand):
    help = "Seed the internal AftermarketScout admin company and its first staff user."

    def add_arguments(self, parser):
        parser.add_argument("--email", default="support@aftermarketscout.com", help="Email for the staff user.")
        parser.add_argument("--password", default="changeme123!", help="Password for the staff user, if newly created.")

    def handle(self, *args, **options):
        email = options["email"]
        password = options["password"]

        company, created = Company.objects.get_or_create(
            slug=ADMIN_COMPANY_SLUG,
            defaults={
                "name": ADMIN_COMPANY_NAME,
                "status": src_enums.CompanyStatus.ACTIVE.value,
                "status_name": src_enums.CompanyStatus.ACTIVE.name,
                "onboarding_step": 4,
            },
        )
        if created:
            self.stdout.write(self.style.SUCCESS(f"Created admin company id={company.id}: {company.name}"))
        else:
            self.stdout.write(f"Admin company already exists: id={company.id}")

        user, user_created = User.objects.get_or_create(
            email=email,
            defaults={"username": email, "first_name": "Aftermarket", "last_name": "Scout", "is_staff": True},
        )
        if user_created:
            user.set_password(password)
            user.is_staff = True
            user.save()
            self.stdout.write(self.style.SUCCESS(f"Created staff user: {email}"))
        else:
            if not user.is_staff:
                user.is_staff = True
                user.save(update_fields=["is_staff"])
                self.stdout.write(self.style.SUCCESS(f"Marked existing user {email} as is_staff=True"))
            else:
                self.stdout.write(f"User {email} already exists and is already staff.")

        profile, profile_created = UserProfile.objects.get_or_create(
            user=user,
            defaults={"company": company, "is_company_admin": True},
        )
        if not profile_created and profile.company_id != company.id:
            profile.company = company
            profile.is_company_admin = True
            profile.save(update_fields=["company", "is_company_admin"])
            self.stdout.write(self.style.SUCCESS(f"Linked existing profile for {email} to admin company."))
        elif profile_created:
            self.stdout.write(self.style.SUCCESS(f"Created UserProfile for {email}."))
        else:
            self.stdout.write(f"UserProfile for {email} already linked to admin company.")

        self.stdout.write(self.style.SUCCESS(f"\nDone. Staff login: {email}" + (f" / {password}" if user_created else "")))
