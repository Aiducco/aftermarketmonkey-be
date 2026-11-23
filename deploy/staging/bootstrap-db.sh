#!/usr/bin/env bash
# First-time schema + seed of the EMPTY staging database. Run as deploy in ~/aftermarketmonkey-be on the staging VM
# (bootstrap-vm.sh apps calls it). Safe to re-run: it does nothing to an already bootstrapped database.
#
# The migration history does not apply from zero (0099 re-adds a column 0097 creates), so the schema is built from
# the models once and the src history recorded as applied; later deploys run real migrations (deploy-staging.yml).
set -euo pipefail
DC="docker compose -f docker-compose.staging.yml --env-file .env.app.staging"
manage() { $DC exec -T app python manage.py "$@" --settings=conf.settings_staging; }
manage_models() { $DC exec -T -e STAGING_SCHEMA_FROM_MODELS=1 app python manage.py "$@" --settings=conf.settings_staging; }

for _ in $(seq 1 30); do manage check >/dev/null 2>&1 && break; sleep 2; done

# shellcheck disable=SC2016  # $POSTGRES_* expand inside the container
has_schema=$($DC exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA' \
  <<< "SELECT to_regclass('public.provider_parts') IS NOT NULL")
if [ "$has_schema" != "t" ]; then
  echo "==> empty database: schema from models"
  manage_models migrate --no-input
  manage_models migrate --run-syncdb --no-input
  manage migrate src --fake --no-input
else
  echo "==> schema exists: applying any new migrations"
  manage migrate --no-input
fi
manage migrate --check && echo "==> no unapplied migrations"

echo "==> seed test companies and users (password printed once if generated)"
manage seed_staging

echo "==> search indexes"
manage index_parts_meilisearch --setup
manage index_vehicles_meilisearch
manage index_tires_meilisearch --rebuild
manage index_wheels_meilisearch --rebuild
