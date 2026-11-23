#!/usr/bin/env bash
# One-time setup of the STAGING VM (stage.aftermarketscout.com). Run as root on the new VM, never on production.
# Copy it over first (the repo is not cloned yet):  scp deploy/staging/bootstrap-vm.sh root@<VM_IP>:
#
#   bash bootstrap-vm.sh system   # packages, firewall, deploy user, SSH keys. Prints keys for GitHub.
#   bash bootstrap-vm.sh apps     # after the deploy keys are added and the staging branches exist on GitHub:
#                                 # clone both repos, generate secrets, start both stacks, nginx, certbot, cron.
# Idempotent: re-running a phase skips what is already done. Secrets are generated here and never leave the VM.
set -euo pipefail

PROD_IP=5.161.121.143
DOMAIN=stage.aftermarketscout.com
DEPLOY=deploy
HOME_D=/home/$DEPLOY
BE=$HOME_D/aftermarketmonkey-be
FE=$HOME_D/aftermarketmonkey-fe
GH_ORG=${GH_ORG:-}   # GitHub owner of both repos, e.g. GH_ORG=myorg bash bootstrap-vm.sh apps

[ "$(id -u)" = 0 ] || { echo "run as root"; exit 1; }
if hostname -I | tr ' ' '\n' | grep -qx "$PROD_IP"; then echo "This is the PRODUCTION host. Refusing."; exit 1; fi

as_deploy() { sudo -iu "$DEPLOY" bash -c "$1"; }
gen() { openssl rand -hex 32; }

system_phase() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -q
  apt-get install -yq docker.io docker-compose-v2 nginx certbot python3-certbot-nginx git ufw curl jq bind9-dnsutils unattended-upgrades
  systemctl enable --now docker

  # Small VMs (the first staging VM has 4 GB): swap so Postgres + Meilisearch + the fe build fit.
  if [ "$(awk '/MemTotal/{print int($2/1048576)}' /proc/meminfo)" -lt 8 ] && ! swapon --show | grep -q /swapfile; then
    fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile >/dev/null && swapon /swapfile
    grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
    sysctl -q vm.swappiness=10 && echo 'vm.swappiness=10' > /etc/sysctl.d/99-staging-swap.conf
  fi

  ufw allow OpenSSH
  ufw allow 'Nginx Full'
  ufw --force enable

  id "$DEPLOY" >/dev/null 2>&1 || useradd -m -s /bin/bash "$DEPLOY"
  usermod -aG docker "$DEPLOY"
  install -d -m 700 -o "$DEPLOY" -g "$DEPLOY" "$HOME_D/.ssh"
  install -d -o "$DEPLOY" -g "$DEPLOY" "$HOME_D/logs" /srv/staging /srv/staging/catalog-subset
  # Your own key (the one root accepts) also logs in as deploy.
  touch "$HOME_D/.ssh/authorized_keys"
  cat /root/.ssh/authorized_keys >> "$HOME_D/.ssh/authorized_keys"
  sort -u -o "$HOME_D/.ssh/authorized_keys" "$HOME_D/.ssh/authorized_keys"

  # Key GitHub Actions uses to SSH in as deploy (STAGING_SSH_KEY). Only valid on this VM.
  if [ ! -f /root/staging-ci-key ]; then
    ssh-keygen -q -t ed25519 -N '' -C "github-actions-staging-deploy" -f /root/staging-ci-key
    cat /root/staging-ci-key.pub >> "$HOME_D/.ssh/authorized_keys"
  fi
  # Read-only deploy keys for cloning (GitHub allows one repo per deploy key, so one each).
  for r in be fe; do
    k="$HOME_D/.ssh/github_$r"
    [ -f "$k" ] || sudo -u "$DEPLOY" ssh-keygen -q -t ed25519 -N '' -C "staging-vm-$r" -f "$k"
  done
  cat > "$HOME_D/.ssh/config" <<EOF
Host github-be
  HostName github.com
  User git
  IdentityFile $HOME_D/.ssh/github_be
  IdentitiesOnly yes
Host github-fe
  HostName github.com
  User git
  IdentityFile $HOME_D/.ssh/github_fe
  IdentitiesOnly yes
EOF
  chown -R "$DEPLOY:$DEPLOY" "$HOME_D/.ssh"; chmod 600 "$HOME_D/.ssh/"*
  as_deploy "ssh-keyscan -t ed25519 github.com >> ~/.ssh/known_hosts 2>/dev/null; sort -u -o ~/.ssh/known_hosts ~/.ssh/known_hosts"

  echo
  echo "==== Add these READ-ONLY deploy keys on GitHub (repo Settings -> Deploy keys -> Add, 'Allow write' OFF):"
  echo "-- aftermarketmonkey-be:"; cat "$HOME_D/.ssh/github_be.pub"
  echo "-- aftermarketmonkey-fe:"; cat "$HOME_D/.ssh/github_fe.pub"
  echo
  echo "==== GitHub Actions secrets (both repos): STAGING_SSH_HOST=$(hostname -I | awk '{print $1}'), STAGING_SSH_USER=$DEPLOY,"
  echo "     STAGING_SSH_KEY = the private key printed by:  cat /root/staging-ci-key"
}

apps_phase() {
  [ -n "$GH_ORG" ] || { echo "set GH_ORG=<github owner of the repos>"; exit 1; }
  for r in be fe; do
    dir=$HOME_D/aftermarketmonkey-$r
    if [ ! -d "$dir/.git" ]; then
      as_deploy "git clone -q github-$r:$GH_ORG/aftermarketmonkey-$r.git $dir"
    fi
    as_deploy "cd $dir && git fetch -q origin staging && git -c advice.detachedHead=false checkout -q -f --detach FETCH_HEAD && git log --oneline -1"
  done

  if [ ! -f "$BE/.env.app.staging" ]; then
    sed -e "s/^DJANGO_SECRET_KEY=.*/DJANGO_SECRET_KEY=$(gen)/" -e "s/^JWT_SECRET=.*/JWT_SECRET=$(gen)/" \
        -e "s/^DATABASE_PASSWORD=.*/DATABASE_PASSWORD=$(gen)/" -e "s/^CACHE_PASSWORD=.*/CACHE_PASSWORD=$(gen)/" \
        -e "s/^MEILISEARCH_MASTER_KEY=.*/MEILISEARCH_MASTER_KEY=$(gen)/" \
        "$BE/deploy/staging/env.app.staging.example" > "$BE/.env.app.staging"
    chown "$DEPLOY:$DEPLOY" "$BE/.env.app.staging"; chmod 600 "$BE/.env.app.staging"
    echo "wrote $BE/.env.app.staging with generated secrets"
  fi

  DC="docker compose -f docker-compose.staging.yml --env-file .env.app.staging"
  as_deploy "cd $BE && $DC up -d postgres redis meilisearch && $DC build app && $DC up -d app"
  as_deploy "cd $BE && bash deploy/staging/bootstrap-db.sh"

  if [ ! -f "$FE/.env.staging" ]; then
    master=$(grep '^MEILISEARCH_MASTER_KEY=' "$BE/.env.app.staging" | cut -d= -f2)
    search_key=""
    for _ in $(seq 1 30); do
      search_key=$(curl -fsS -H "Authorization: Bearer $master" http://127.0.0.1:7700/keys \
        | jq -r '.results[] | select(.actions == ["search"]) | .key' | head -1) && [ -n "$search_key" ] && break
      sleep 2
    done
    [ -n "$search_key" ] || { echo "could not read the Meilisearch default search key"; exit 1; }
    cat > "$FE/.env.staging" <<EOF
VITE_API_BASE_URL=https://$DOMAIN/api
VITE_MEILISEARCH_URL=https://$DOMAIN/meilisearch
VITE_MEILISEARCH_KEY=$search_key
VITE_POSTHOG_PROJECT_TOKEN=
EOF
    chown "$DEPLOY:$DEPLOY" "$FE/.env.staging"; chmod 600 "$FE/.env.staging"
  fi
  as_deploy "cd $FE && docker compose -f docker-compose.staging.yml --env-file .env.staging up -d --build"

  cp "$BE/deploy/staging/nginx-stage.conf" /etc/nginx/sites-available/$DOMAIN
  ln -sf /etc/nginx/sites-available/$DOMAIN /etc/nginx/sites-enabled/$DOMAIN
  rm -f /etc/nginx/sites-enabled/default
  nginx -t && systemctl reload nginx
  if [ ! -d /etc/letsencrypt/live/$DOMAIN ]; then
    if [ "$(dig +short $DOMAIN A | tail -1)" = "$(hostname -I | awk '{print $1}')" ]; then
      certbot --nginx -d $DOMAIN --redirect --register-unsafely-without-email --agree-tos -n
    else
      echo "DNS for $DOMAIN does not point here yet: re-run 'bash bootstrap-vm.sh apps' once it does (certbot step)."
    fi
  fi

  as_deploy "crontab $BE/deploy/staging/crontab && crontab -l | grep -c manage.py"
  echo "==== staging up. Check: curl -sI https://$DOMAIN/ | grep -i x-robots-tag"
}

case "${1:-}" in
  system) system_phase ;;
  apps) apps_phase ;;
  *) echo "usage: bash bootstrap-vm.sh system|apps"; exit 1 ;;
esac
