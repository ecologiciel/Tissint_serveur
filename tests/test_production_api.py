import os
import uuid

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("TINSSIT_SKIP_MODEL_LOAD", "1")
os.environ.setdefault("API_KEY", "tissint_ci_key")
os.environ.setdefault(
    "DATABASE_URL",
    os.getenv("TEST_DATABASE_URL", "postgresql+asyncpg://postgres@127.0.0.1:55432/meteorite_db"),
)
os.environ.setdefault("STORAGE_DIR", os.getenv("TEST_STORAGE_DIR", ".runtime/test-storage"))

from business_logic import BusinessOrchestrator
from billing import PREMIUM_DAILY_SCAN_LIMIT
from database import AsyncSessionLocal, ListingModel, ScanModel, UserModel, UserSubscription
from main import app

API_KEY = os.environ["API_KEY"]


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as test_client:
        yield test_client


def api_headers(access_token: str | None = None) -> dict[str, str]:
    headers = {"X-API-Key": API_KEY}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    return headers


def unique_suffix() -> str:
    return uuid.uuid4().hex[:12]


def register_user(client: TestClient, prefix: str = "ci") -> dict:
    suffix = unique_suffix()
    response = client.post(
        "/api/v1/auth/register",
        headers=api_headers(),
        json={
            "first_name": "CI",
            "last_name": prefix,
            "phone": f"+2127{suffix[:9]}",
            "email": f"{prefix}-{suffix}@example.com",
            "password": "SecurePass123!",
            "desired_role": "free",
            "device_id": f"ci-{prefix}",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def error_code(response) -> str:
    payload = response.json()
    return payload["error"]["code"]


def run_in_app_loop(client: TestClient, async_fn, *args):
    return client.portal.call(async_fn, *args)


async def promote_user_to_admin(user_id: str) -> None:
    async with AsyncSessionLocal() as db:
        user = await db.get(UserModel, user_id)
        subscription = await db.get(UserSubscription, user_id)
        assert user is not None
        assert subscription is not None
        user.role = "admin"
        subscription.tier = "admin"
        subscription.status = "active"
        subscription.remaining_tokens = PREMIUM_DAILY_SCAN_LIMIT
        await db.commit()


async def seed_rare_listing(user_id: str) -> tuple[str, str]:
    scan_id = f"scan-{unique_suffix()}"
    listing_id = f"listing-{unique_suffix()}"
    async with AsyncSessionLocal() as db:
        scan = ScanModel(
            id=scan_id,
            client_uuid=f"client-{unique_suffix()}",
            user_id=user_id,
            status_code="DIAGNOSTIC_SUCCESS_HIGH",
            is_meteorite=True,
            meteorite_probability=0.96,
            dominant_class="Martian",
            class_confidence=0.91,
            weight=18.4,
            magnetic=True,
            latitude=31.6,
            longitude=-7.9,
            raw_vision_outputs={"exterior": {}},
            exterior_images_paths=["storage/test/exterior.jpg"],
        )
        db.add(scan)
        await db.flush()
        db.add(
            ListingModel(
                id=listing_id,
                scan_id=scan_id,
                price=42000.0,
                status="institutional_hold_24h",
                title="Rare Martian candidate",
                description="Admin radar candidate without public contact data.",
                price_mode="on_request",
                region="Tissint",
            )
        )
        await db.commit()
    return scan_id, listing_id


async def seed_scan_for_idempotent_message(user_id: str, client_uuid: str) -> str:
    scan_id = f"scan-{unique_suffix()}"
    async with AsyncSessionLocal() as db:
        db.add(
            ScanModel(
                id=scan_id,
                client_uuid=client_uuid,
                user_id=user_id,
                status_code="DIAGNOSTIC_SUCCESS_HIGH",
                is_meteorite=True,
                meteorite_probability=0.8263893872499467,
                dominant_class="Chondrite",
                class_confidence=0.7738270749648412,
                weight=120.0,
                magnetic=True,
                latitude=31.6,
                longitude=-7.9,
                raw_vision_outputs={"exterior": {}},
                exterior_images_paths=["storage/test/chondrite.jpg"],
            )
        )
        await db.commit()
    return scan_id


def test_scan_diagnostic_messages_are_deterministic():
    orchestrator = BusinessOrchestrator()

    success = orchestrator.evaluate_decision(
        {
            "is_meteorite": True,
            "meteorite_probability": 0.8263893872499467,
            "dominant_class": "Chondrite",
            "class_confidence": 0.7738270749648412,
            "metadata_applied": {},
        },
        language="fr-FR",
    )
    assert success["status_code"] == "DIAGNOSTIC_SUCCESS_HIGH"
    assert success["message"]["language"] == "fr"
    assert success["message"]["tone"] == "success"
    assert "82.6%" in success["message"]["body"]
    assert "marketplace" in success["message"]["body"]

    hesitant = orchestrator.evaluate_decision(
        {
            "is_meteorite": True,
            "meteorite_probability": 0.72,
            "dominant_class": "Meteore_Unknown",
            "class_confidence": 0.61,
            "metadata_applied": {},
        }
    )
    assert hesitant["status_code"] == "DIAGNOSTIC_HESITANT"
    assert hesitant["message"]["language"] == "ar"
    assert hesitant["message"]["tone"] == "warning"
    assert "72.0%" in hesitant["message"]["body"]

    rejected = orchestrator.evaluate_decision(
        {
            "is_meteorite": False,
            "meteorite_probability": 0.123,
            "dominant_class": "None",
            "class_confidence": 0.88,
            "metadata_applied": {},
        },
        language="en-US",
    )
    assert rejected["status_code"] == "DIAGNOSTIC_REJECTED"
    assert rejected["message"]["language"] == "ar"
    assert rejected["message"]["tone"] == "neutral"
    assert "12.3%" in rejected["message"]["body"]


def test_auth_refresh_logout_and_billing_source_of_truth(client: TestClient):
    session = register_user(client, "billing")
    access_token = session["access_token"]
    refresh_token = session["refresh_token"]

    me_response = client.get("/api/v1/auth/me", headers=api_headers(access_token))
    assert me_response.status_code == 200, me_response.text
    assert me_response.json()["user"]["role"] == "free"

    refresh_response = client.post(
        "/api/v1/auth/refresh",
        headers=api_headers(),
        json={"refresh_token": refresh_token},
    )
    assert refresh_response.status_code == 200, refresh_response.text
    refreshed = refresh_response.json()

    old_refresh_response = client.post(
        "/api/v1/auth/refresh",
        headers=api_headers(),
        json={"refresh_token": refresh_token},
    )
    assert old_refresh_response.status_code == 401
    assert error_code(old_refresh_response) == "UNAUTHORIZED"

    auth_headers = api_headers(refreshed["access_token"])
    checkout_response = client.post(
        "/api/v1/billing/checkout",
        headers=auth_headers,
        json={"plan": "monthly", "provider": "mock", "return_url": "tissint://checkout/success"},
    )
    assert checkout_response.status_code == 201, checkout_response.text
    checkout = checkout_response.json()
    assert checkout["status"] == "paid"
    assert checkout["amount_dh"] == 100.0

    subscription_response = client.get("/api/v1/billing/subscription", headers=auth_headers)
    assert subscription_response.status_code == 200, subscription_response.text
    subscription = subscription_response.json()
    assert subscription["status"] == "active"
    assert subscription["role"] == "premium"

    me_after_billing = client.get("/api/v1/auth/me", headers=auth_headers)
    assert me_after_billing.status_code == 200, me_after_billing.text
    assert me_after_billing.json()["user"]["role"] == "premium"

    invoices_response = client.get("/api/v1/billing/invoices", headers=auth_headers)
    assert invoices_response.status_code == 200, invoices_response.text
    invoices = invoices_response.json()
    assert len(invoices) == 1
    assert invoices[0]["status"] == "paid"
    assert invoices[0]["total_dh"] == 100.0

    cancel_response = client.post("/api/v1/billing/cancel", headers=auth_headers)
    assert cancel_response.status_code == 200, cancel_response.text
    cancelled = cancel_response.json()
    assert cancelled["status"] == "cancelled"
    assert cancelled["role"] == "premium"

    logout_response = client.post(
        "/api/v1/auth/logout",
        headers=api_headers(),
        json={"refresh_token": refreshed["refresh_token"]},
    )
    assert logout_response.status_code == 200, logout_response.text
    assert logout_response.json()["status"] == "ok"

    revoked_me = client.get("/api/v1/auth/me", headers=auth_headers)
    assert revoked_me.status_code == 401
    assert error_code(revoked_me) == "UNAUTHORIZED"


def test_billing_webhook_is_idempotent(client: TestClient):
    session = register_user(client, "webhook")
    auth_headers = api_headers(session["access_token"])

    checkout_response = client.post(
        "/api/v1/billing/checkout",
        headers=auth_headers,
        json={"plan": "yearly", "provider": "stripe"},
    )
    assert checkout_response.status_code == 201, checkout_response.text
    checkout = checkout_response.json()
    assert checkout["status"] == "pending"
    assert checkout["amount_dh"] == 960.0

    event_id = f"evt-{unique_suffix()}"
    webhook_payload = {
        "event_id": event_id,
        "type": "checkout.completed",
        "checkout_session_id": checkout["id"],
        "plan": "yearly",
    }
    first_webhook = client.post(
        "/api/v1/billing/webhooks/stripe",
        headers=api_headers(),
        json=webhook_payload,
    )
    assert first_webhook.status_code == 200, first_webhook.text
    assert first_webhook.json()["status"] == "processed"
    assert first_webhook.json()["processed"] is True

    duplicate_webhook = client.post(
        "/api/v1/billing/webhooks/stripe",
        headers=api_headers(),
        json=webhook_payload,
    )
    assert duplicate_webhook.status_code == 200, duplicate_webhook.text
    assert duplicate_webhook.json()["status"] == "duplicate"
    assert duplicate_webhook.json()["processed"] is False

    subscription = client.get("/api/v1/billing/subscription", headers=auth_headers).json()
    assert subscription["status"] == "active"
    assert subscription["role"] == "premium"
    assert subscription["plan"] == "yearly"

    invoices = client.get("/api/v1/billing/invoices", headers=auth_headers).json()
    assert len(invoices) == 1
    assert invoices[0]["total_dh"] == 960.0


def test_scan_validation_and_error_envelope(client: TestClient):
    no_key_response = client.get("/api/v1/quota/me")
    assert no_key_response.status_code == 422
    assert error_code(no_key_response) == "VALIDATION_ERROR"

    user_id = f"scan-user-{unique_suffix()}"
    too_few_photos = client.post(
        "/api/v1/scan/exterior",
        headers=api_headers(),
        data={"client_uuid": f"scan-{unique_suffix()}", "user_id": user_id},
        files=[
            ("files_exterior", ("one.jpg", b"jpeg-one", "image/jpeg")),
            ("files_exterior", ("two.jpg", b"jpeg-two", "image/jpeg")),
        ],
    )
    assert too_few_photos.status_code == 400
    assert error_code(too_few_photos) == "MISSING_EXTERNAL_PHOTOS"

    invalid_file_type = client.post(
        "/api/v1/scan/exterior",
        headers=api_headers(),
        data={"client_uuid": f"scan-{unique_suffix()}", "user_id": user_id},
        files=[
            ("files_exterior", ("one.txt", b"not an image", "text/plain")),
            ("files_exterior", ("two.txt", b"not an image", "text/plain")),
            ("files_exterior", ("three.txt", b"not an image", "text/plain")),
        ],
    )
    assert invalid_file_type.status_code == 415
    assert error_code(invalid_file_type) == "INVALID_FILE_FORMAT"


def test_scan_idempotent_response_includes_localized_message(client: TestClient):
    user_id = f"scan-message-user-{unique_suffix()}"
    client_uuid = f"scan-message-{unique_suffix()}"
    scan_id = run_in_app_loop(client, seed_scan_for_idempotent_message, user_id, client_uuid)

    response = client.post(
        "/api/v1/scan/exterior",
        headers={**api_headers(), "Accept-Language": "fr-FR"},
        data={"client_uuid": client_uuid, "user_id": user_id},
        files=[
            ("files_exterior", ("one.txt", b"not-an-image", "text/plain")),
            ("files_exterior", ("two.txt", b"not-an-image", "text/plain")),
            ("files_exterior", ("three.txt", b"not-an-image", "text/plain")),
        ],
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["scan_id"] == scan_id
    assert payload["is_sync_retry"] is True
    assert payload["message"]["language"] == "fr"
    assert payload["message"]["tone"] == "success"
    assert "82.6%" in payload["message"]["body"]
    assert "marketplace" in payload["message"]["body"]


def test_admin_radar_requires_admin_and_writes_audit_log(client: TestClient):
    seller_session = register_user(client, "seller")
    admin_session = register_user(client, "admin")
    seller_id = seller_session["user"]["id"]
    admin_id = admin_session["user"]["id"]
    run_in_app_loop(client, promote_user_to_admin, admin_id)
    _scan_id, listing_id = run_in_app_loop(client, seed_rare_listing, seller_id)

    forbidden_response = client.get(
        "/api/v1/admin/radar",
        headers=api_headers(seller_session["access_token"]),
    )
    assert forbidden_response.status_code == 403
    assert error_code(forbidden_response) == "FORBIDDEN"

    admin_headers = api_headers(admin_session["access_token"])
    radar_response = client.get("/api/v1/admin/radar", headers=admin_headers)
    assert radar_response.status_code == 200, radar_response.text
    radar_ids = {item["listing_id"] for item in radar_response.json()}
    assert listing_id in radar_ids

    reserve_response = client.post(
        f"/api/v1/admin/radar/{listing_id}/reserve",
        headers=admin_headers,
        json={"reason": "CI reserve"},
    )
    assert reserve_response.status_code == 200, reserve_response.text
    assert reserve_response.json()["status"] == "admin_reserved"

    audit_response = client.get(
        f"/api/v1/admin/audit?entity_type=listing&entity_id={listing_id}",
        headers=admin_headers,
    )
    assert audit_response.status_code == 200, audit_response.text
    audit_actions = {entry["action"] for entry in audit_response.json()}
    assert "admin_reserve_listing" in audit_actions
