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

Authenticated user endpoints also use:

```http
Authorization: Bearer <access_token>
```

Access and refresh tokens are opaque server-side session tokens; only their hashes are persisted.

For authenticated mobile flows, `Authorization: Bearer` is the identity source of truth. The legacy `X-User-Id` header remains accepted for transition and tests, but if it disagrees with the Bearer user the API returns `FORBIDDEN`.

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

Marketplace descriptions and titles must not contain direct phone, email, WhatsApp, or external contact handles. The server rejects those payloads with `CONTACT_LEAK_DETECTED` so premium/admin contact visibility remains the source of truth.

## Endpoints

| Mobile flow | Method | Path | Response type |
| --- | --- | --- | --- |
| Connectivity check | `GET` | `/health` | `HealthResponse` |
| Register | `POST` JSON | `/api/v1/auth/register` | `AuthResponse` |
| Login | `POST` JSON | `/api/v1/auth/login` | `AuthResponse` |
| Current user | `GET` | `/api/v1/auth/me` | `AuthResponse` |
| Refresh token | `POST` JSON | `/api/v1/auth/refresh` | `AuthResponse` |
| Logout | `POST` JSON | `/api/v1/auth/logout` | `{ status: "ok" }` |
| Quota snapshot | `GET` | `/api/v1/quota/me` | `QuotaResponse` |
| Exterior scan | `POST` multipart | `/api/v1/scan/exterior` | `ScanDecisionResponse` |
| Add interior cut | `PATCH` multipart | `/api/v1/scan/{scan_id}/interior` | `ScanDecisionResponse` |
| Collection list | `GET` | `/api/v1/collection` | `CollectionItemResponse[]` |
| Add scan to collection | `POST` | `/api/v1/collection/{scan_id}` | `CollectionItemResponse` |
| Collection detail | `GET` | `/api/v1/collection/{scan_id}` | `CollectionItemResponse` |
| Remove from collection | `DELETE` | `/api/v1/collection/{scan_id}` | `{ ok: true }` |
| Publish scan | `POST` JSON `{ price, title, description, price_mode?, region, weight_g? }` | `/api/v1/marketplace/publish/{scan_id}` | `MarketplaceListingResponse` |
| Marketplace list | `GET` | `/api/v1/marketplace/listings` | `PublicListingItem[]` |
| Marketplace detail | `GET` | `/api/v1/marketplace/listings/{listing_id}` | `PublicListingItem` |
| Admin radar list | `GET` | `/api/v1/admin/radar` | `AdminRadarListingResponse[]` |
| Admin reserve listing | `POST` JSON `{ reason? }` | `/api/v1/admin/radar/{listing_id}/reserve` | `AdminActionResponse` |
| Admin release listing | `POST` JSON `{ reason? }` | `/api/v1/admin/radar/{listing_id}/release` | `AdminActionResponse` |
| Admin reject listing | `POST` JSON `{ reason? }` | `/api/v1/admin/radar/{listing_id}/reject` | `AdminActionResponse` |
| Admin audit logs | `GET` | `/api/v1/admin/audit` | `AuditLogResponse[]` |
| Create checkout | `POST` JSON `{ plan, provider, return_url? }` | `/api/v1/billing/checkout` | `CheckoutSessionResponse` |
| Subscription source of truth | `GET` | `/api/v1/billing/subscription` | `SubscriptionResponse` |
| Cancel subscription | `POST` | `/api/v1/billing/cancel` | `SubscriptionResponse` |
| Invoice history | `GET` | `/api/v1/billing/invoices` | `InvoiceResponse[]` |
| Billing webhook | `POST` JSON | `/api/v1/billing/webhooks/{provider}` | `BillingWebhookResponse` |
| Send chat message | `POST` JSON | `/api/v1/marketplace/chat/send` | `MessageResponse` |
| Chat history | `GET` | `/api/v1/marketplace/chat/history/{conversation_id}` | `MessageResponse[]` |

## Generated Artifacts

- OpenAPI: `docs/openapi.json`
- TypeScript types: `src/api/generated/types.ts`
- Reference client: `src/api/tinssitClient.ts`

Scan, collection, marketplace, and admin radar payloads expose `has_interior_cut` so the app can align badges and CTAs with the media state. The app must display the server-provided `meteorite_probability` as the single Tissint score and must not recalculate it locally. Scores above 90% are reserved for dossiers with an interior cut photo; when `actions.invite_interior_cut` is true, prompt the user to add a cut photo to unlock the reinforced badge, stronger marketplace visibility, and a more credible dossier.

Marketplace listing payloads expose `fusion_score` / `meteorite_probability` for the Tissint score, `class_confidence` for classification confidence, `weight_g` as the mobile-facing weight alias, and `gallery_images` / `interior_image_uri` when scan media is available. Legacy fields such as `confidence` and `weight` remain for backward compatibility.

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
