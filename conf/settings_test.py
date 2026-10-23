"""Throwaway settings for local verification: SQLite only, never the prod DSN in .env.

Migrations are disabled because 0058 backfills with Postgres NOW(), which SQLite has no
function for; tables are built straight from the models instead.
"""
import os
os.environ.setdefault("APOLLO_WEBHOOK_SECRET", "test-secret-123")
os.environ.setdefault("APOLLO_API_KEY", "test-key")
from conf.settings_base import *  # noqa: F401,F403

DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
MIGRATION_MODULES = {"src": None}
APOLLO_WEBHOOK_SECRET = "test-secret-123"
APOLLO_API_KEY = "test-key"
APOLLO_WEBHOOK_BASE_URL = "https://api.aftermarketscout.com"
