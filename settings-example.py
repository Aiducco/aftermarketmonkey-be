import os

from dotenv import load_dotenv

from conf import settings_base

load_dotenv()

for setting in dir(settings_base):
    if setting == setting.upper():
        globals()[setting] = getattr(settings_base, setting)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql_psycopg2",
        "NAME": os.environ.get("DATABASE_NAME", "sailfish_dev"),
        "USER": os.environ.get("DATABASE_USER", "root"),
        "PASSWORD": os.environ.get("DATABASE_PASSWORD"),
        "HOST": os.environ.get("DATABASE_HOST", "127.0.0.1"),
        "PORT": os.environ.get("DATABASE_PORT", "5432"),
    }
}

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://{}:{}/1".format(
            os.environ.get("CACHE_HOST", "127.0.0.1"),
            os.environ.get("CACHE_PORT", "6379"),
        ),
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
        # "KEY_PREFIX": "example"
    }
}

ALLOWED_HOSTS = ["*"]
EMAIL_HOST_USER = os.environ.get("EMAIL_HOST_USER")
EMAIL_HOST_PASSWORD = os.environ.get("EMAIL_HOST_PASSWORD")
