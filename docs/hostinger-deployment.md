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

## Durable HTTPS API

For production, Vercel must call the backend through HTTPS:

```text
Vercel frontend -> Vercel proxy -> https://api.tissint.ma -> Hostinger backend -> Postgres
```

Before switching Vercel, create an A record:

```text
api.tissint.ma  A  72.62.236.197
```

Then run this from the Hostinger web terminal or SSH:

```bash
cd /opt/tissint/backend
API_DOMAIN=api.tissint.ma PROXY_STACK=auto bash scripts/hostinger_enable_https_api.sh
```

The script refuses to continue if DNS is not pointing at the VPS or if local backend health fails. It uses Nginx when Nginx is already present; otherwise it installs Caddy for automatic TLS.

For the temporary Hostinger hostname, run it with the DNS guard disabled because `/etc/hosts` can resolve the local hostname to `127.0.1.1` from inside the VPS even when public DNS is correct:

```bash
cd /opt/tissint/backend
SKIP_DNS_CHECK=1 API_DOMAIN=srv1610573.hstgr.cloud PROXY_STACK=auto bash scripts/hostinger_enable_https_api.sh
```

After `https://api.tissint.ma/health` returns `database=ok`, update the Vercel production environment to:

```text
HOSTINGER_API_ORIGIN=https://api.tissint.ma
HOSTINGER_API_KEY=<same value as backend API_KEY>
```

Remove `ALLOW_INSECURE_HOSTINGER_ORIGIN`, redeploy Vercel, and only then close public port `8000` in the Hostinger firewall. Keep `80` and `443` open.

If no custom domain has been purchased yet, use the Hostinger hostname instead:

```text
HOSTINGER_API_ORIGIN=https://srv1610573.hstgr.cloud
```

From the mobile app repository, the guarded Vercel switch is:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\switch-vercel-hostinger-api.ps1 -Apply -Redeploy
```

The script checks `https://api.tissint.ma/health` before changing Vercel and then smokes the Vercel proxy.

After the Vercel smoke tests pass, pull the latest backend and recreate the API container so Docker binds port `8000` to localhost only:

```bash
cd /opt/tissint/backend
git pull --ff-only origin main
docker compose up -d --build api_server
curl http://127.0.0.1:8000/health
curl https://srv1610573.hstgr.cloud/health
```

At that point, direct public access to `72.62.236.197:8000` is no longer needed.

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
