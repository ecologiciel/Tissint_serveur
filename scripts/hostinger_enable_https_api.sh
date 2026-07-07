#!/usr/bin/env bash
set -euo pipefail

API_DOMAIN="${API_DOMAIN:-api.tissint.ma}"
UPSTREAM_HOST="${UPSTREAM_HOST:-127.0.0.1}"
UPSTREAM_PORT="${UPSTREAM_PORT:-8000}"
APP_DIR="${APP_DIR:-/opt/tissint/backend}"
PROXY_STACK="${PROXY_STACK:-auto}"
LETSENCRYPT_EMAIL="${LETSENCRYPT_EMAIL:-}"
SKIP_DNS_CHECK="${SKIP_DNS_CHECK:-0}"

log() {
  printf '\n[%s] %s\n' "$(date -Is)" "$*"
}

fail() {
  printf '\nERROR: %s\n' "$*" >&2
  exit 1
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    fail "Run this script as root from the Hostinger VPS terminal."
  fi
}

public_ip() {
  curl -fsS --max-time 8 https://api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}'
}

resolved_ip() {
  getent ahostsv4 "$API_DOMAIN" | awk 'NR == 1 { print $1 }'
}

check_dns() {
  if [ "$SKIP_DNS_CHECK" = "1" ]; then
    log "Skipping DNS check because SKIP_DNS_CHECK=1."
    return
  fi

  local server_ip domain_ip
  server_ip="$(public_ip)"
  domain_ip="$(resolved_ip || true)"

  [ -n "$domain_ip" ] || fail "$API_DOMAIN does not resolve yet. Create an A record to $server_ip first."
  [ "$domain_ip" = "$server_ip" ] || fail "$API_DOMAIN resolves to $domain_ip, expected $server_ip."

  log "$API_DOMAIN resolves to this VPS ($server_ip)."
}

check_backend() {
  log "Checking local backend health on http://${UPSTREAM_HOST}:${UPSTREAM_PORT}/health"
  local health
  health="$(curl -fsS --max-time 10 "http://${UPSTREAM_HOST}:${UPSTREAM_PORT}/health")" \
    || fail "Backend health failed. Check: cd $APP_DIR && docker compose ps && docker compose logs --tail=120 api_server"
  printf '%s\n' "$health"
}

install_nginx_proxy() {
  log "Configuring Nginx reverse proxy for $API_DOMAIN"
  apt-get update
  apt-get install -y nginx certbot python3-certbot-nginx

  cat >"/etc/nginx/sites-available/tissint-api.conf" <<NGINX
server {
    listen 80;
    listen [::]:80;
    server_name ${API_DOMAIN};

    client_max_body_size 32m;

    location / {
        proxy_pass http://${UPSTREAM_HOST}:${UPSTREAM_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 120s;
        proxy_send_timeout 120s;
    }
}
NGINX

  ln -sfn /etc/nginx/sites-available/tissint-api.conf /etc/nginx/sites-enabled/tissint-api.conf
  nginx -t
  systemctl enable --now nginx
  systemctl reload nginx

  if [ -n "$LETSENCRYPT_EMAIL" ]; then
    certbot --nginx -d "$API_DOMAIN" --non-interactive --agree-tos -m "$LETSENCRYPT_EMAIL" --redirect
  else
    certbot --nginx -d "$API_DOMAIN" --non-interactive --agree-tos --register-unsafely-without-email --redirect
  fi

  nginx -t
  systemctl reload nginx
}

install_caddy_proxy() {
  log "Configuring Caddy reverse proxy for $API_DOMAIN"

  if ! command -v caddy >/dev/null 2>&1; then
    apt-get update
    apt-get install -y debian-keyring debian-archive-keyring apt-transport-https curl gnupg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
      | gpg --dearmor --yes -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
      > /etc/apt/sources.list.d/caddy-stable.list
    apt-get update
    apt-get install -y caddy
  fi

  mkdir -p /etc/caddy/conf.d

  if [ ! -f /etc/caddy/Caddyfile ]; then
    printf 'import /etc/caddy/conf.d/*.caddy\n' >/etc/caddy/Caddyfile
  elif ! grep -q 'import /etc/caddy/conf.d/\*.caddy' /etc/caddy/Caddyfile; then
    cp /etc/caddy/Caddyfile "/etc/caddy/Caddyfile.bak.$(date +%Y%m%d%H%M%S)"
    printf '\nimport /etc/caddy/conf.d/*.caddy\n' >>/etc/caddy/Caddyfile
  fi

  cat >"/etc/caddy/conf.d/tissint-api.caddy" <<CADDY
${API_DOMAIN} {
    encode zstd gzip

    header {
        Strict-Transport-Security "max-age=31536000; includeSubDomains"
        X-Content-Type-Options "nosniff"
        Referrer-Policy "no-referrer"
    }

    reverse_proxy ${UPSTREAM_HOST}:${UPSTREAM_PORT}
}
CADDY

  caddy fmt --overwrite /etc/caddy/Caddyfile /etc/caddy/conf.d/tissint-api.caddy
  caddy validate --config /etc/caddy/Caddyfile
  systemctl enable --now caddy
  systemctl reload caddy || systemctl restart caddy
}

configure_firewall_note() {
  if command -v ufw >/dev/null 2>&1 && ufw status | grep -qi '^Status: active'; then
    log "Opening 80/443 in UFW because it is active."
    ufw allow 80/tcp
    ufw allow 443/tcp
  fi

  log "Hostinger firewall reminder: keep 80/443 open. Close public 8000 only after HTTPS and Vercel smoke tests pass."
}

final_smoke() {
  log "Checking HTTPS health"
  curl -fsS --max-time 20 "https://${API_DOMAIN}/health"
  printf '\n'
  log "HTTPS API is ready: https://${API_DOMAIN}"
}

main() {
  require_root
  check_dns
  check_backend

  case "$PROXY_STACK" in
    nginx)
      install_nginx_proxy
      ;;
    caddy)
      install_caddy_proxy
      ;;
    auto)
      if systemctl is-active --quiet nginx || command -v nginx >/dev/null 2>&1; then
        install_nginx_proxy
      else
        install_caddy_proxy
      fi
      ;;
    *)
      fail "Unsupported PROXY_STACK=$PROXY_STACK. Use auto, nginx, or caddy."
      ;;
  esac

  configure_firewall_note
  final_smoke
}

main "$@"
