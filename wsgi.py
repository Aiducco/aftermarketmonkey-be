"""
WSGI config for backend project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.1/howto/deployment/wsgi/
"""

import os
import threading

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "conf.settings_base")

application = get_wsgi_application()

# Warm the tire-search reference caches (brand names, tread vocabulary, facet rail) now, off the
# request path. Without this, a fresh gunicorn worker's first tire search pays that load cost
# directly in its response time -- normally ~1.6s, but under host pressure (see tire_search.py's
# REFERENCE_CACHE_TTL_SECONDS comment) enough to trip the worker timeout and kill the worker,
# handing the same cold cache to the next one. Backgrounded so a slow warm-up never delays this
# worker's own boot or its first request.


def _warm_tire_search_caches() -> None:
    from src.api.services import tire_search

    tire_search.warm_reference_caches()


threading.Thread(target=_warm_tire_search_caches, daemon=True).start()
