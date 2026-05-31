# Tinssit Mobile API Contract

This document tracks the server contract that the mobile app should consume.

## Base URLs

- Local Android emulator: `http://10.0.2.2:8000`
- Local iOS simulator: `http://127.0.0.1:8000`
- Physical device on local Wi-Fi: `http://<computer-lan-ip>:8000`
- Production VPS: pending Hostinger network access

## Auth

All `/api/v1/*` endpoints require:

```http
X-API-Key: <api key>
```

`GET /health` is public and can be used before login or sync.

## Error Shape

Every handled API error returns:

```json
{
  "status_code": "DIAGNOSTIC_FAILED",
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Requete invalide.",
    "details": []
  }
}
```

The mobile app should branch on `error.code`, not on translated `message`.

## Endpoints

| Mobile flow | Method | Path | Response type |
| --- | --- | --- | --- |
| Connectivity check | `GET` | `/health` | `HealthResponse` |
| Exterior scan | `POST` multipart | `/api/v1/scan/exterior` | `ScanDecisionResponse` |
| Add interior cut | `PATCH` multipart | `/api/v1/scan/{scan_id}/interior` | `ScanDecisionResponse` |
| Publish scan | `POST` | `/api/v1/marketplace/publish/{scan_id}` | `MarketplaceListingResponse` |
| Marketplace list | `GET` | `/api/v1/marketplace/listings` | `PublicListingItem[]` |
| Send chat message | `POST` JSON | `/api/v1/marketplace/chat/send` | `MessageResponse` |
| Chat history | `GET` | `/api/v1/marketplace/chat/history/{conversation_id}` | `MessageResponse[]` |

## Generated Artifacts

- OpenAPI: `docs/openapi.json`
- TypeScript types: `src/api/generated/types.ts`
- Reference client: `src/api/tinssitClient.ts`

Regenerate the contract with:

```bash
npm run api:contract
```

## Mock Replacement Order

1. Replace health/connectivity mock with `client.health()`.
2. Replace marketplace list mock with `client.getMarketplaceListings()`.
3. Replace chat mocks with `client.sendChatMessage()` and `client.getChatHistory()`.
4. Replace scan mock with `client.scanExterior()`.
5. Replace interior update mock with `client.scanInteriorUpdate()`.
