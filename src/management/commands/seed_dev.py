"""
Seed a throwaway local database with a test company and user for UI verification.

Refuses to run on anything except conf.settings_dev (SQLite file under .dev-data/), so it can never
write to the production database that the default settings point at. Idempotent.

Usage:
    python manage.py seed_dev --settings=conf.settings_dev
"""

from django.conf import settings
from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from src.api.services import onboarding as onboarding_services
from src.models import Company

DEV_EMAIL = "dev@example.test"
DEV_PASSWORD = "dev-password-123"  # throwaway: only ever valid in the local .dev-data SQLite file


class Command(BaseCommand):
    help = "Seed the local dev SQLite database with a test company, user and the distributor catalog."

    def handle(self, *args, **options):
        if settings.SETTINGS_MODULE != "conf.settings_dev":
            raise CommandError("seed_dev only runs with --settings=conf.settings_dev (never the production database).")
        if settings.DATABASES["default"]["ENGINE"] != "django.db.backends.sqlite3":
            raise CommandError("seed_dev only runs against SQLite.")

        # Migrations are disabled in conf.settings_dev (0058 uses Postgres-only NOW()), so build the tables
        # straight from the models. Idempotent: existing tables are left alone.
        call_command("migrate", run_syncdb=True, verbosity=0)

        call_command("seed_providers")
        call_command("seed_shop_management_providers")

        if User.objects.filter(email=DEV_EMAIL).exists():
            self.stdout.write(f"Dev user already exists: {DEV_EMAIL}")
            return

        result = onboarding_services.register_user(
            first_name="Dev",
            last_name="User",
            email=DEV_EMAIL,
            password=DEV_PASSWORD,
            company_name="Dev Shop",
            role="owner",
        )
        # Finished onboarding, so the app opens on /parts like a returning user.
        Company.objects.filter(id=result["company_id"]).update(onboarding_step=4)
        self.stdout.write(self.style.SUCCESS(f"Created {DEV_EMAIL} (company_id={result['company_id']})"))
