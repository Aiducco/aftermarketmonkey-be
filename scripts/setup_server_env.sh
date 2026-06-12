#!/usr/bin/env bash
# Run this ONCE on the server to migrate from the old flat .env.app
# to the environment-specific naming that docker-compose.yml now expects.
#
# Usage:
#   bash scripts/setup_server_env.sh             # defaults to production
#   bash scripts/setup_server_env.sh development

set -euo pipefail

ENV="${1:-production}"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "Setting up env files for environment: $ENV (in $REPO_DIR)"

if [ -f "$REPO_DIR/.env.app" ] && [ ! -f "$REPO_DIR/.env.app.$ENV" ]; then
    cp "$REPO_DIR/.env.app" "$REPO_DIR/.env.app.$ENV"
    echo "  Copied .env.app → .env.app.$ENV"
else
    echo "  .env.app.$ENV already exists (or .env.app missing) — skipping"
fi

echo ""
echo "Done. Test with:"
echo "  DEPLOY_ENV=$ENV docker compose config"
