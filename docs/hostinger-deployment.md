# Hostinger VPS Deployment

Target VPS:

- IPv4: `72.62.236.197`
- Hostname: `srv1610573.hstgr.cloud`
- OS: Ubuntu 24.04 LTS

## Before Running The Deploy

In Hostinger hPanel, verify:

- VPS status is running.
- SSH key `codex-tissint-hostinger` is attached to root.
- Firewall allows inbound TCP `22`, `80`, `443`, and `8000`.
- If SSH times out, temporarily disable the Hostinger firewall, sync rules, then reboot the VPS.

From a local terminal, SSH should answer:

```bash
ssh -i ~/.ssh/codex_hostinger_tissint root@72.62.236.197
```

## Bootstrap Command

Run this from the Hostinger web terminal or over SSH:

```bash
curl -fsSL https://raw.githubusercontent.com/ecologiciel/Tissint_serveur/main/scripts/hostinger_bootstrap.sh | bash
```

The script installs Docker, clones/pulls the backend into `/opt/tissint/backend`, creates a production `.env` with generated secrets if missing, starts `docker compose`, and checks `/health`.

## Useful Commands

```bash
cd /opt/tissint/backend
docker compose ps
docker compose logs -f api_server
docker compose logs -f postgres_db
curl http://127.0.0.1:8000/health
```

External smoke test after ports are open:

```bash
curl http://72.62.236.197:8000/health
```

## Mobile Configuration

For a temporary IP-based test build:

```bash
EXPO_PUBLIC_TISSINT_API_MODE=http
EXPO_PUBLIC_TISSINT_API_BASE_URL=http://72.62.236.197:8000
```

For production app stores, use HTTPS with a real domain:

```bash
EXPO_PUBLIC_TISSINT_API_MODE=http
EXPO_PUBLIC_TISSINT_API_BASE_URL=https://api.tissint.ma
```

Do not put the private server `API_KEY` in public Expo variables. Use only a public gateway identifier there, or move the API key check behind a server-side edge/gateway before app-store release.
