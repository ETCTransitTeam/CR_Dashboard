#!/usr/bin/env bash
# Install / refresh systemd + nginx units from this repo onto the current server.
# Run from repo root on the EC2 box (as ubuntu, with sudo):
#   bash deploy/install_on_server.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Repo: $REPO_ROOT"

if [[ ! -f .env ]]; then
  echo "ERROR: .env missing. Copy deploy/env.example to .env and fill secrets."
  exit 1
fi
if [[ ! -f path/to/key.p8 ]]; then
  echo "ERROR: path/to/key.p8 missing. See path/to/README.md"
  exit 1
fi

mkdir -p logs .streamlit
if [[ ! -f .streamlit/config.toml ]]; then
  cp .streamlit/config.toml.example .streamlit/config.toml
  echo "Created .streamlit/config.toml from example"
fi

echo "==> Installing systemd units"
sudo cp deploy/streamlit.service /etc/systemd/system/streamlit.service
sudo cp deploy/morning-od-sync.service /etc/systemd/system/morning-od-sync.service
sudo cp deploy/morning-od-sync.timer /etc/systemd/system/morning-od-sync.timer
sudo systemctl daemon-reload
sudo systemctl enable streamlit
sudo systemctl restart streamlit
sudo systemctl enable --now morning-od-sync.timer

echo "==> Installing logrotate for morning sync log"
sudo cp deploy/logrotate-morning-od-sync.conf /etc/logrotate.d/morning-od-sync

echo "==> Installing nginx site (HTTP; run certbot separately for HTTPS)"
if command -v nginx >/dev/null 2>&1; then
  sudo cp deploy/nginx-odcollection.conf /etc/nginx/sites-available/odcollection
  sudo ln -sf /etc/nginx/sites-available/odcollection /etc/nginx/sites-enabled/odcollection
  sudo rm -f /etc/nginx/sites-enabled/default
  sudo nginx -t
  sudo systemctl enable nginx
  sudo systemctl reload nginx
else
  echo "nginx not installed — skip. Install with: sudo apt install -y nginx"
fi

echo "==> Status"
sudo systemctl --no-pager --full status streamlit || true
systemctl list-timers --no-pager | grep morning-od || true
echo "Done. App should be on :8501; domain via nginx on :80."
echo "HTTPS (if needed): sudo certbot --nginx -d odcollection.etc-research.com"
