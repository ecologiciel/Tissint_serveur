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

from business_logic import BusinessOrchestrator, HESITANT_THRESHOLD, SUCCESS_THRESHOLD
from billing import PREMIUM_DAILY_SCAN_LIMIT
from database import AsyncSessionLocal, CollectionItemModel, ListingModel, ScanModel, UserModel, UserSubscription
from fusion_engine import MeteoriteFusionEngine
import main as main_module
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
        db.add(
            CollectionItemModel(
                id=f"collection-{unique_suffix()}",
                user_id=user_id,
                scan_id=scan_id,
                status="marketplace_listed",
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


async def seed_scan_for_interior_update(user_id: str) -> str:
    scan_id = f"scan-{unique_suffix()}"
    async with AsyncSessionLocal() as db:
        db.add(
            ScanModel(
                id=scan_id,
                client_uuid=f"interior-{unique_suffix()}",
                user_id=user_id,
                status_code="DIAGNOSTIC_SUCCESS_HIGH",
                is_meteorite=True,
                meteorite_probability=0.86,
                dominant_class="Chondrite",
                class_confidence=0.43,
                weight=52.0,
                magnetic=None,
                latitude=None,
                longitude=None,
                raw_vision_outputs={
                    "exterior": {
                        "dino": {
                            "prob_bin": 0.95,
                            "prob_sub": [0.02, 0.10, 0.01, 0.45, 0.02, 0.40],
                        },
                        "swin": {
                            "prob_bin": 0.90,
                            "prob_sub": [0.04, 0.20, 0.01, 0.43, 0.02, 0.30],
                        },
                        "convnext": {
                            "prob_bin": 0.73,
                            "prob_sub": [0.12, 0.10, 0.01, 0.41, 0.10, 0.26],
                        },
                    },
                    "interior": None,
                },
                exterior_images_paths=["storage/test/interior-before.jpg"],
            )
        )
        await db.commit()
    return scan_id


async def load_scan_raw_vision(scan_id: str) -> dict:
    async with AsyncSessionLocal() as db:
        scan = await db.get(ScanModel, scan_id)
        assert scan is not None
        return scan.raw_vision_outputs


def test_scan_diagnostic_messages_are_deterministic():
    orchestrator = BusinessOrchestrator()

    success = orchestrator.evaluate_decision(
        {
            "is_meteorite": True,
            "meteorite_probability": SUCCESS_THRESHOLD,
            "dominant_class": "Chondrite",
            "class_confidence": 0.7738270749648412,
            "metadata_applied": {},
        },
        language="fr-FR",
    )
    assert success["status_code"] == "DIAGNOSTIC_SUCCESS_HIGH"
    assert success["is_meteorite"] is True
    assert success["actions"]["invite_interior_cut"] is True
    assert success["message"]["language"] == "fr"
    assert success["message"]["tone"] == "success"
    assert "80.8%" in success["message"]["body"]
    assert "marketplace" in success["message"]["body"]

    hesitant = orchestrator.evaluate_decision(
        {
            "is_meteorite": True,
            "meteorite_probability": HESITANT_THRESHOLD,
            "dominant_class": "Meteore_Unknown",
            "class_confidence": 0.61,
            "metadata_applied": {},
        }
    )
    assert hesitant["status_code"] == "DIAGNOSTIC_HESITANT"
    assert hesitant["is_meteorite"] is True
    assert hesitant["actions"]["invite_interior_cut"] is True
    assert hesitant["message"]["language"] == "ar"
    assert hesitant["message"]["tone"] == "warning"
    assert "70.0%" in hesitant["message"]["body"]
    assert "Meteore_Unknown" not in hesitant["message"]["body"]

    rejected_by_threshold = orchestrator.evaluate_decision(
        {
            "is_meteorite": True,
            "meteorite_probability": HESITANT_THRESHOLD - 0.01,
            "dominant_class": "Chondrite",
            "class_confidence": 0.61,
            "metadata_applied": {},
        },
        language="fr-FR",
    )
    assert rejected_by_threshold["status_code"] == "DIAGNOSTIC_REJECTED"
    assert rejected_by_threshold["is_meteorite"] is False
    assert rejected_by_threshold["actions"]["invite_interior_cut"] is False

    success_with_cut = orchestrator.evaluate_decision(
        {
            "is_meteorite": True,
            "meteorite_probability": 0.91,
            "dominant_class": "Meteore_Unknown",
            "class_confidence": 0.88,
            "metadata_applied": {},
        },
        language="fr-FR",
        has_interior_cut=True,
    )
    assert success_with_cut["status_code"] == "DIAGNOSTIC_SUCCESS_HIGH"
    assert success_with_cut["actions"]["invite_interior_cut"] is False
    assert "Meteore_Unknown" not in success_with_cut["message"]["body"]
    assert "prise en compte" in success_with_cut["message"]["body"]
    assert "renforcera" not in success_with_cut["message"]["body"]

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


def test_fusion_engine_uses_hesitant_threshold_for_binary_verdict():
    engine = MeteoriteFusionEngine()
    model_output = {"prob_bin": HESITANT_THRESHOLD - 0.01, "prob_sub": [0.02, 0.03, 0.02, 0.86, 0.03, 0.04]}

    output = engine.fuse_outputs(
        {
            "exterior": {
                "dino": model_output,
                "swin": model_output,
                "convnext": model_output,
            }
        }
    )

    assert output["meteorite_probability"] == pytest.approx(HESITANT_THRESHOLD - 0.01)
    assert output["is_meteorite"] is False


def test_auth_refresh_logout_and_billing_source_of_truth(client: TestClient):
    session = register_user(client, "billing")
    access_token = session["access_token"]
    refresh_token = session["refresh_token"]

    me_response = client.get("/api/v1/auth/me", headers=api_headers(access_token))
    assert me_response.status_code == 200, me_response.text
    me_payload = me_response.json()
    assert me_payload["user"]["role"] == "free"
    assert me_payload["quota"]["role"] == "free"

    refresh_response = client.post(
        "/api/v1/auth/refresh",
        headers=api_headers(),
        json={"refresh_token": refresh_token},
    )
    assert refresh_response.status_code == 200, refresh_response.text
    refreshed = refresh_response.json()
    assert refreshed["access_token"]
    assert refreshed["refresh_token"]

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


def test_scan_exterior_with_initial_interior_cut_suppresses_cut_invite(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
):
    class FakeVisionPipeline:
        def process_full_scan(self, list_exterior_bytes: list[bytes], interior_bytes: bytes | None = None) -> dict:
            assert len(list_exterior_bytes) == 3
            assert interior_bytes == b"cut-photo"
            sub_vector = [0.02, 0.03, 0.02, 0.86, 0.03, 0.04]
            model_output = {"prob_bin": 0.72, "prob_sub": sub_vector}
            return {
                "exterior": {
                    "dino": model_output,
                    "swin": model_output,
                    "convnext": model_output,
                },
                "interior": {
                    "dino": model_output,
                    "swin": model_output,
                    "convnext": model_output,
                },
            }

    monkeypatch.setattr(main_module, "vision_pipeline", FakeVisionPipeline())
    session = register_user(client, "initial-cut")
    user_id = session["user"]["id"]

    response = client.post(
        "/api/v1/scan/exterior",
        headers={**api_headers(), "Accept-Language": "fr-FR"},
        data={"client_uuid": f"initial-cut-{unique_suffix()}", "user_id": user_id},
        files=[
            ("files_exterior", ("one.jpg", b"jpeg-one", "image/jpeg")),
            ("files_exterior", ("two.jpg", b"jpeg-two", "image/jpeg")),
            ("files_exterior", ("three.jpg", b"jpeg-three", "image/jpeg")),
            ("file_interior", ("cut.jpg", b"cut-photo", "image/jpeg")),
        ],
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["status_code"] == "DIAGNOSTIC_HESITANT"
    assert payload["actions"]["invite_interior_cut"] is False
    assert "prise en compte" in payload["message"]["body"]
    assert "indispensable" not in payload["message"]["body"]

    collection_response = client.post(
        f"/api/v1/collection/{payload['scan_id']}",
        headers={**api_headers(), "X-User-Id": user_id},
    )
    assert collection_response.status_code == 201, collection_response.text
    assert collection_response.json()["status"] == "pending_validation"


def test_scan_interior_update_persists_raw_vision_outputs(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    class FakeVisionPipeline:
        def predict_image_parallel(self, _image_bytes: bytes) -> dict:
            return {
                "dino": {
                    "prob_bin": 0.99,
                    "prob_sub": [0.01, 0.03, 0.01, 0.05, 0.01, 0.89],
                },
                "swin": {
                    "prob_bin": 0.98,
                    "prob_sub": [0.01, 0.04, 0.01, 0.05, 0.01, 0.88],
                },
                "convnext": {
                    "prob_bin": 0.97,
                    "prob_sub": [0.01, 0.05, 0.01, 0.06, 0.01, 0.86],
                },
            }

    monkeypatch.setattr(main_module, "vision_pipeline", FakeVisionPipeline())
    session = register_user(client, "interior")
    scan_id = run_in_app_loop(client, seed_scan_for_interior_update, session["user"]["id"])

    response = client.patch(
        f"/api/v1/scan/{scan_id}/interior",
        headers={**api_headers(), "Accept-Language": "fr-FR"},
        files={"file_interior": ("interior.jpg", b"not-a-real-image-but-valid-for-fake-pipeline", "image/jpeg")},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["scan_id"] == scan_id
    assert payload["dominant_class"] == "Meteore_Unknown"
    assert payload["actions"]["invite_interior_cut"] is False
    assert "Meteore_Unknown" not in payload["message"]["body"]
    assert "prise en compte" in payload["message"]["body"]
    assert "renforcera" not in payload["message"]["body"]

    raw_vision_outputs = run_in_app_loop(client, load_scan_raw_vision, scan_id)
    assert raw_vision_outputs["interior"] is not None
    assert raw_vision_outputs["interior"]["dino"]["prob_bin"] == 0.99
    assert raw_vision_outputs["interior"]["convnext"]["prob_sub"][5] == 0.86


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
    radar_items = radar_response.json()
    radar_ids = {item["listing_id"] for item in radar_items}
    assert listing_id in radar_ids
    radar_item = next(item for item in radar_items if item["listing_id"] == listing_id)
    assert radar_item["main_image_uri"].startswith("/storage/")
    assert radar_item["image_url"] == radar_item["main_image_uri"]
    assert radar_item["thumbnail_uri"] == radar_item["main_image_uri"]

    reserve_response = client.post(
        f"/api/v1/admin/radar/{listing_id}/reserve",
        headers=admin_headers,
        json={"reason": "CI reserve"},
    )
    assert reserve_response.status_code == 200, reserve_response.text
    assert reserve_response.json()["ok"] is True
    assert reserve_response.json()["status"] == "admin_reserved"

    audit_response = client.get(
        f"/api/v1/admin/audit?entity_type=listing&entity_id={listing_id}",
        headers=admin_headers,
    )
    assert audit_response.status_code == 200, audit_response.text
    audit_actions = {entry["action"] for entry in audit_response.json()}
    assert "admin_reserve_listing" in audit_actions


def test_ui_alignment_collection_and_marketplace_images(client: TestClient):
    seller_session = register_user(client, "ui-seller")
    seller_id = seller_session["user"]["id"]
    scan_id, listing_id = run_in_app_loop(client, seed_rare_listing, seller_id)

    listings_response = client.get("/api/v1/marketplace/listings", headers=api_headers())
    assert listings_response.status_code == 200, listings_response.text
    listing = next(item for item in listings_response.json() if item["listing_id"] == listing_id)
    assert listing["main_image_uri"].startswith("/storage/")
    assert listing["image_url"] == listing["main_image_uri"]
    assert listing["thumbnail_uri"] == listing["main_image_uri"]
    assert listing["can_contact"] is False
    assert listing["seller_phone"] is None
    assert listing["seller_whatsapp"] is None

    detail_response = client.get(f"/api/v1/marketplace/listings/{listing_id}", headers=api_headers())
    assert detail_response.status_code == 200, detail_response.text
    detail = detail_response.json()
    assert detail["listing_id"] == listing_id
    assert detail["image_url"] == listing["image_url"]

    collection_response = client.get(
        "/api/v1/collection",
        headers={**api_headers(), "X-User-Id": seller_id},
    )
    assert collection_response.status_code == 200, collection_response.text
    collection_item = next(item for item in collection_response.json() if item["scan_id"] == scan_id)
    assert collection_item["main_image_uri"].startswith("/storage/")
    assert collection_item["image_url"] == collection_item["main_image_uri"]
    assert collection_item["thumbnail_uri"] == collection_item["main_image_uri"]
    assert collection_item["weight_g"] == 18.4
    assert collection_item["region"] == "Tissint"
    assert collection_item["notes"] == "Admin radar candidate without public contact data."


def test_ui_messages_favorites_notifications_ratings_wallet_and_marketplace_extensions(client: TestClient):
    seller_session = register_user(client, "social-seller")
    buyer_session = register_user(client, "social-buyer")
    seller_id = seller_session["user"]["id"]
    scan_id, listing_id = run_in_app_loop(client, seed_rare_listing, seller_id)
    seller_headers = api_headers(seller_session["access_token"])
    buyer_headers = api_headers(buyer_session["access_token"])

    add_favorite = client.post(f"/api/v1/favorites/{listing_id}", headers=buyer_headers)
    assert add_favorite.status_code == 200, add_favorite.text
    assert add_favorite.json()["ok"] is True
    duplicate_favorite = client.post(f"/api/v1/favorites/{listing_id}", headers=buyer_headers)
    assert duplicate_favorite.status_code == 200, duplicate_favorite.text

    favorites = client.get("/api/v1/favorites", headers=buyer_headers)
    assert favorites.status_code == 200, favorites.text
    favorite_listing = next(item for item in favorites.json() if item["listing_id"] == listing_id)
    assert favorite_listing["image_url"].startswith("/storage/")

    first_message = client.post(
        "/api/v1/messages",
        headers=buyer_headers,
        json={"listing_id": listing_id, "text": "Bonjour, je suis interesse."},
    )
    assert first_message.status_code == 201, first_message.text
    first_message_payload = first_message.json()
    assert first_message_payload["from_me"] is True
    thread_id = first_message_payload["thread_id"]

    seller_inbox = client.get("/api/v1/messages", headers=seller_headers)
    assert seller_inbox.status_code == 200, seller_inbox.text
    seller_thread = next(thread for thread in seller_inbox.json() if thread["id"] == thread_id)
    assert seller_thread["unread"] == 1
    assert seller_thread["listing_image_uri"].startswith("/storage/")

    seller_notifications = client.get("/api/v1/notifications", headers=seller_headers)
    assert seller_notifications.status_code == 200, seller_notifications.text
    notification = next(item for item in seller_notifications.json() if item["type"] == "message")
    assert notification["read"] is False

    read_notification = client.patch(f"/api/v1/notifications/{notification['id']}/read", headers=seller_headers)
    assert read_notification.status_code == 200, read_notification.text
    assert read_notification.json()["ok"] is True
    read_all = client.post("/api/v1/notifications/read-all", headers=seller_headers)
    assert read_all.status_code == 200, read_all.text

    seller_thread_messages = client.get(f"/api/v1/messages/{thread_id}", headers=seller_headers)
    assert seller_thread_messages.status_code == 200, seller_thread_messages.text
    assert seller_thread_messages.json()[0]["from_me"] is False

    seller_reply = client.post(
        "/api/v1/messages",
        headers=seller_headers,
        json={"thread_id": thread_id, "text": "Merci, je vous reponds ici."},
    )
    assert seller_reply.status_code == 201, seller_reply.text
    assert seller_reply.json()["from_me"] is True

    buyer_inbox = client.get("/api/v1/messages", headers=buyer_headers)
    assert buyer_inbox.status_code == 200, buyer_inbox.text
    buyer_thread = next(thread for thread in buyer_inbox.json() if thread["id"] == thread_id)
    assert buyer_thread["unread"] == 1

    push_response = client.post(
        "/api/v1/notifications/push-subscribe",
        headers=buyer_headers,
        json={
            "endpoint": f"https://push.example.test/{unique_suffix()}",
            "keys": {"p256dh": "p256dh-test-key", "auth": "auth-test-key"},
        },
    )
    assert push_response.status_code == 200, push_response.text
    assert push_response.json()["subscribed"] is True

    rating_response = client.post(
        "/api/v1/ratings",
        headers=buyer_headers,
        json={"listing_id": listing_id, "seller_id": seller_id, "stars": 5, "comment": "Tres serieux."},
    )
    assert rating_response.status_code == 201, rating_response.text
    assert rating_response.json()["ok"] is True

    seller_profile = client.get(f"/api/v1/sellers/{seller_id}", headers=buyer_headers)
    assert seller_profile.status_code == 200, seller_profile.text
    seller_profile_payload = seller_profile.json()
    assert seller_profile_payload["average_rating"] == 5.0
    assert seller_profile_payload["total_ratings"] == 1
    assert any(item["listing_id"] == listing_id for item in seller_profile_payload["listings"])

    wallet_response = client.get("/api/v1/wallet", headers=buyer_headers)
    assert wallet_response.status_code == 200, wallet_response.text
    assert wallet_response.json()["balance"] == 0.0
    withdrawal_response = client.post(
        "/api/v1/wallet/withdraw",
        headers=buyer_headers,
        json={"amount": 100.0, "iban": "MA64011519000001205000534921"},
    )
    assert withdrawal_response.status_code == 409
    assert error_code(withdrawal_response) == "INSUFFICIENT_FUNDS"

    my_listings = client.get("/api/v1/marketplace/my-listings", headers=seller_headers)
    assert my_listings.status_code == 200, my_listings.text
    assert any(item["scan_id"] == scan_id for item in my_listings.json())

    search_response = client.post(
        "/api/v1/marketplace/search",
        headers=api_headers(),
        json={"query": "Martian", "region": "Tissint", "classification": "Martian"},
    )
    assert search_response.status_code == 200, search_response.text
    assert any(item["listing_id"] == listing_id for item in search_response.json())

    stats_response = client.get("/api/v1/marketplace/stats", headers=api_headers())
    assert stats_response.status_code == 200, stats_response.text
    stats = stats_response.json()
    assert stats["total_listings"] >= 1
    assert stats["avg_price_dh"] > 0

    delete_favorite = client.delete(f"/api/v1/favorites/{listing_id}", headers=buyer_headers)
    assert delete_favorite.status_code == 200, delete_favorite.text
    delete_favorite_again = client.delete(f"/api/v1/favorites/{listing_id}", headers=buyer_headers)
    assert delete_favorite_again.status_code == 200, delete_favorite_again.text
