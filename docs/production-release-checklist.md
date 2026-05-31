# Production Release Checklist

Use this checklist before deploying the backend to Hostinger.

## Required Gates

- `python -m compileall main.py schemas.py database.py billing.py security.py`
- `pytest`
- `npm run lint`
- `npm run api:contract`
- `git diff --exit-code docs/openapi.json src/api/generated/types.ts`

## Environment

- `API_KEY` is a long production secret.
- `DATABASE_URL` points to the production Postgres instance.
- `STORAGE_DIR` points to persistent disk.
- `TINSSIT_SKIP_MODEL_LOAD=0` in production.
- `CORS_ALLOWED_ORIGINS` is reviewed for mobile/web clients.
- `TELEGRAM_BOT_TOKEN` and `TELEGRAM_ADMIN_CHAT_ID` are set when radar alerts are enabled.

## Deployment Smoke Test

- `GET /health` returns `status=ok` and `database=ok`.
- `POST /api/v1/auth/register` creates a free user.
- `POST /api/v1/auth/login` returns access and refresh tokens.
- `GET /api/v1/billing/subscription` returns the server subscription source of truth.
- `POST /api/v1/billing/checkout` with the configured provider creates a session.
- `GET /api/v1/admin/radar` rejects non-admin users with `FORBIDDEN`.
