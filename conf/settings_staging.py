"""Staging settings: stage.aftermarketscout.com on its own VM (workspace docs/plans/staging-environment.md).

Everything comes from the environment (.env.app.staging on the staging VM, passed by docker-compose.staging.yml);
this module never calls load_dotenv(), so a stray .env with the production DSN is never read. It refuses to start
when a value would make staging share state with production:
  - JWT_SECRET must be set and differ from production's (a staging token must never be valid on prod),
  - DATABASE_HOST must not be the production server.

    DJANGO_SETTINGS_MODULE=conf.settings_staging   (set by docker-compose.staging.yml)
"""
import os

from django.core.exceptions import ImproperlyConfigured

from conf.settings_base import *  # noqa: F401,F403

PRODUCTION_HOSTS = {"5.161.121.143", "api.aftermarketscout.com", "app.aftermarketscout.com"}
_PRODUCTION_JWT_SECRET = "secret_jwt"  # settings.py; staging must never accept prod tokens or mint valid ones


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ImproperlyConfigured(f"{name} must be set for conf.settings_staging")
    return value


STAGING_HOST = os.environ.get("STAGING_HOST", "stage.aftermarketscout.com")

SECRET_KEY = _required("DJANGO_SECRET_KEY")
DEBUG = False
ALLOWED_HOSTS = [STAGING_HOST, "localhost", "127.0.0.1"]
CSRF_TRUSTED_ORIGINS = [f"https://{STAGING_HOST}"]
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

JWT_SECRET = _required("JWT_SECRET")
if JWT_SECRET == _PRODUCTION_JWT_SECRET:
    raise ImproperlyConfigured("JWT_SECRET is production's; give staging its own")

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql_psycopg2",
        "NAME": _required("DATABASE_NAME"),
        "USER": _required("DATABASE_USER"),
        "PASSWORD": _required("DATABASE_PASSWORD"),
        "HOST": os.environ.get("DATABASE_HOST", "postgres"),
        "PORT": os.environ.get("DATABASE_PORT", "5432"),
        "OPTIONS": {"keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 3},
    }
}
if DATABASES["default"]["HOST"] in PRODUCTION_HOSTS:
    raise ImproperlyConfigured("DATABASE_HOST points at production; staging has its own Postgres")

# One-time bootstrap of an EMPTY staging database. The migration history does not apply from zero (found
# 2026-11-23: 0099 re-adds premier_company_pricing.jobber_price, which 0097 already creates), so the first schema is
# built from the models and the history is then recorded without running it (deploy/staging/bootstrap-db.sh):
#   STAGING_SCHEMA_FROM_MODELS=1 python manage.py migrate --no-input --settings=conf.settings_staging   # django apps
#   STAGING_SCHEMA_FROM_MODELS=1 python manage.py migrate --run-syncdb --no-input --settings=conf.settings_staging
#   python manage.py migrate src --fake --no-input --settings=conf.settings_staging
# Every later deploy runs the real new migrations (deploy-staging.yml).
if os.environ.get("STAGING_SCHEMA_FROM_MODELS") == "1":
    MIGRATION_MODULES = {"src": None}

_cache_password = os.environ.get("CACHE_PASSWORD", "")
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://{}{}:{}/1".format(
            f":{_cache_password}@" if _cache_password else "",
            os.environ.get("CACHE_HOST", "redis"),
            os.environ.get("CACHE_PORT", "6379"),
        ),
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
    }
}

FRONTEND_BASE_URL = os.environ.get("FRONTEND_BASE_URL", f"https://{STAGING_HOST}")
# fe and api share one host, so CORS only needs that origin.
CORS_ALLOW_ALL_ORIGINS = False
CORS_ALLOWED_ORIGINS = [FRONTEND_BASE_URL]

MEILISEARCH_HOST = os.environ.get("MEILISEARCH_HOST", "http://meilisearch:7700")
MEILISEARCH_MASTER_KEY = _required("MEILISEARCH_MASTER_KEY")

# Stripe test keys only (empty until a test-mode account is set up: billing pages show their error state).
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
if STRIPE_SECRET_KEY.startswith(("sk_live_", "rk_live_")):
    raise ImproperlyConfigured("STRIPE_SECRET_KEY is a live key; staging takes test keys only")

# Email never leaves the box: Django mail prints to the container log, Resend stays off unless a key is set
# (only ever a key restricted to an allowlisted test address).
EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
LOGGING_EMAIL_RECIPIENT_LIST = []
SUPPORT_EMAIL_RECIPIENT_LIST = []
