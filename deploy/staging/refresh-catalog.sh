#!/usr/bin/env bash
# Refresh staging's catalog from production, read-only (tasks S-3/S-4). Run as deploy in ~/aftermarketmonkey-be on
# the staging VM, off-peak. Needs the staging_export role on prod (export_catalog_subset --print-grants) and a
# pg_hba entry for this VM. The password is asked for interactively and never written to disk.
#
#   bash deploy/staging/refresh-catalog.sh [per_provider]      # default 5000
set -euo pipefail
PER_PROVIDER=${1:-5000}
OUT=/srv/staging/catalog-subset
DC="docker compose -f docker-compose.staging.yml --env-file .env.app.staging"
manage() { $DC exec -T "$@"; }

df -h /srv | tail -1
read -r -s -p "staging_export password for the production database: " PW; echo
# Exported (not passed on the command line) so the password never shows up in `ps`.
export CATALOG_SOURCE_DSN="host=5.161.121.143 port=5432 dbname=aftermarketmonkey-db user=staging_export password=$PW sslmode=prefer application_name=staging_refresh"
unset PW

rm -rf "${OUT:?}/current.new"
mkdir -p "$OUT"
echo "==> export (read-only) into $OUT/current.new"
# Runs as the deploy user so the files can be rotated afterwards without root.
$DC exec -T -u "$(id -u):$(id -g)" -e HOME=/tmp -e CATALOG_SOURCE_DSN app python manage.py export_catalog_subset \
  --out /data/catalog-subset/current.new --per-provider "$PER_PROVIDER" --settings=conf.settings_staging
unset CATALOG_SOURCE_DSN
rm -rf "$OUT/previous"; [ -d "$OUT/current" ] && mv "$OUT/current" "$OUT/previous"; mv "$OUT/current.new" "$OUT/current"

echo "==> import into the staging database (replaces the catalog, then re-seeds the test companies)"
manage app python manage.py import_catalog_subset /data/catalog-subset/current --replace --settings=conf.settings_staging
manage app python manage.py seed_staging --settings=conf.settings_staging
for c in index_parts_meilisearch index_vehicles_meilisearch index_tires_meilisearch index_wheels_meilisearch; do
  manage app python manage.py "$c" --settings=conf.settings_staging
done
echo "==> done"
