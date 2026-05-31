# Tissint Backend

FastAPI backend for the Tissint mobile app: auth, quota, scan intake, collection, marketplace, admin radar, audit, and billing contracts.

## Local Production-Like Run

1. Copy `.env.example` to `.env` and set real secrets.
2. Start Postgres and the API:

```bash
docker compose up -d --build
```

3. Verify the service:

```bash
curl http://127.0.0.1:8000/health
```

## Contract Workflow

The mobile app consumes the OpenAPI-derived TypeScript types from:

- `docs/openapi.json`
- `src/api/generated/types.ts`

Regenerate them after API schema changes:

```bash
npm run api:contract
```

## Tests

The API test suite needs Postgres. For local Codex runs, set `DATABASE_URL` to a running test database and skip AI model loading:

```bash
set TINSSIT_SKIP_MODEL_LOAD=1
set API_KEY=tissint_ci_key
set DATABASE_URL=postgresql+asyncpg://postgres@127.0.0.1:55432/meteorite_db
pytest
```

CI uses `requirements-ci.txt`, which intentionally excludes the Torch inference stack. The production Docker image still installs `requirements.txt`.
