#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/tissint/backend}"
REPO_URL="${REPO_URL:-https://github.com/ecologiciel/Tissint_serveur.git}"
BRANCH="${BRANCH:-main}"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run this script as root on the Hostinger VPS." >&2
  exit 1
fi

echo "[1/7] Installing system packages"
apt-get update
apt-get install -y ca-certificates curl git git-lfs gnupg openssl
git lfs install --system

if ! command -v docker >/dev/null 2>&1; then
  echo "[2/7] Installing Docker Engine"
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  . /etc/os-release
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
else
  echo "[2/7] Docker already installed"
fi

systemctl enable --now docker

echo "[3/7] Fetching backend repository"
mkdir -p "$(dirname "$APP_DIR")"
if [ ! -d "$APP_DIR/.git" ]; then
  git clone --branch "$BRANCH" "$REPO_URL" "$APP_DIR"
else
  git -C "$APP_DIR" fetch origin "$BRANCH"
  git -C "$APP_DIR" checkout "$BRANCH"
  git -C "$APP_DIR" pull --ff-only origin "$BRANCH"
fi

git -C "$APP_DIR" lfs pull

cd "$APP_DIR"

echo "[4/7] Creating production .env if missing"
if [ ! -f .env ]; then
  DB_PASSWORD="$(openssl rand -base64 36 | tr -d '\n=+/ ' | cut -c1-32)"
  API_KEY="$(openssl rand -hex 32)"
  cat > .env <<ENV
API_KEY=${API_KEY}
DB_USER=postgres
DB_PASSWORD=${DB_PASSWORD}
DB_NAME=meteorite_db
DATABASE_URL=postgresql+asyncpg://postgres:${DB_PASSWORD}@postgres_db:5432/meteorite_db
STORAGE_DIR=/app/storage_vessel
CORS_ALLOWED_ORIGINS=*
ACCESS_TOKEN_TTL_MINUTES=30
REFRESH_TOKEN_TTL_DAYS=30
TINSSIT_SKIP_MODEL_LOAD=0
TELEGRAM_BOT_TOKEN=
TELEGRAM_ADMIN_CHAT_ID=
APP_URL=http://$(curl -fsS https://api.ipify.org || hostname -I | awk '{print $1}'):8000
ENV
  chmod 600 .env
  echo "Created $APP_DIR/.env with generated secrets."
else
  echo ".env already exists; keeping current production secrets."
fi

echo "[5/7] Building and starting containers"
docker compose up -d --build

echo "[6/7] Container status"
docker compose ps

echo "[7/7] Waiting for healthcheck"
for attempt in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8000/health >/tmp/tissint-health.json; then
    cat /tmp/tissint-health.json
    echo
    echo "Tissint backend is running on http://$(curl -fsS https://api.ipify.org || hostname -I | awk '{print $1}'):8000"
    exit 0
  fi
  sleep 5
done

echo "Backend did not become healthy in time. Recent logs:" >&2
docker compose logs --tail=120 api_server >&2
exit 1
