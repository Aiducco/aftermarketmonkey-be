"""Local development settings: throwaway SQLite file, never the prod DSN in .env.

Used by the local verification stack (workspace/scripts/dev-stack.sh) so agents and humans can click
through the real UI against a real running API without touching production data.

    python manage.py migrate --run-syncdb --settings=conf.settings_dev
    python manage.py seed_dev --settings=conf.settings_dev
    python manage.py runserver 8001 --settings=conf.settings_dev

This module never calls load_dotenv(), so .env (which points at the production database) is not read.
Migrations are disabled for the same reason as conf/settings_test.py: 0058 backfills with Postgres NOW().
"""
import os

os.environ.setdefault("APOLLO_WEBHOOK_SECRET", "dev-secret")
os.environ.setdefault("APOLLO_API_KEY", "dev-key")
from conf.settings_base import *  # noqa: F401,F403

DEBUG = True
ALLOWED_HOSTS = ["localhost", "127.0.0.1"]

_DEV_DATA_DIR = BASE_DIR / ".dev-data"  # noqa: F405
_DEV_DATA_DIR.mkdir(exist_ok=True)
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": str(_DEV_DATA_DIR / "db.sqlite3")}}
MIGRATION_MODULES = {"src": None}

CACHES = {"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}

# Emails (welcome, password reset) print to the runserver console instead of being sent.
EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
RESEND_API_KEY = ""

CORS_ALLOWED_ORIGIN_REGEXES = [r"^http://localhost:\d+$", r"^http://127\.0\.0\.1:\d+$"]
CORS_ALLOWED_ORIGINS = []
FRONTEND_BASE_URL = os.environ.get("FRONTEND_BASE_URL", "http://localhost:5174")

# Secrets that normally come from settings.py/.env. Dev placeholders only: no real third-party calls.
JWT_SECRET = "dev-only-jwt-secret-not-for-production"
STRIPE_SECRET_KEY = ""
STRIPE_WEBHOOK_SECRET = ""
MEILISEARCH_HOST = "http://localhost:7700"
MEILISEARCH_MASTER_KEY = ""

# Belt and braces: refuse to run if anything above was overridden back to a network database.
assert DATABASES["default"]["ENGINE"] == "django.db.backends.sqlite3"
