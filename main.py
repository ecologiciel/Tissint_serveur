from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, status, BackgroundTasks, Body, Header, Query
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import uuid
import anyio
import os
import re
import hashlib
import json
import io
import csv
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import Integer, case, func, or_, text
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.staticfiles import StaticFiles
from PIL import Image, ImageOps

from exceptions import (
    AppProductionException,
    app_exception_handler,
    http_exception_handler,
    request_validation_exception_handler,
)
from schemas import (
    ApiErrorResponse,
    AuthResponse,
    AuthUserResponse,
    CaptureImageResponse,
    CaptureSessionCreateInput,
    CaptureSessionResponse,
    CollectionItemResponse,
    HealthResponse,
    LoginInput,
    LogoutInput,
    QuotaResponse,
    RefreshTokenInput,
    RegisterInput,
    ScanDecisionResponse,
    ScanMetadataInput,
    PublishListingInput,
    UpdateListingInput,
    MarketplaceListingResponse,
    PublicListingItem,
    AdminActionResponse,
    AdminListingActionInput,
    AdminRadarListingResponse,
    AuditLogResponse,
    BillingCheckoutInput,
    BillingWebhookResponse,
    CheckoutSessionResponse,
    CreateMessageInput,
    InvoiceResponse,
    MarketplaceSearchInput,
    MarketplaceStatsResponse,
    MessageThreadResponse,
    MessageResponse,
    NotificationResponse,
    OkResponse,
    PushSubscribeInput,
    PushSubscribeResponse,
    RatingInput,
    RatingResponse,
    SellerProfileResponse,
    SendMessageInput,
    SubscriptionResponse,
    UiMessageResponse,
    WalletResponse,
    WalletTransactionResponse,
    WithdrawInput,
    WithdrawResponse,
    ExpertAnnotationInput,
    ExpertAnnotationResponse,
    ExpertAccountCreateInput,
    ExpertAccountResponse,
    ExpertAuditCreateInput,
    ExpertAuditResponse,
    ExpertDatasetCreateInput,
    ExpertDatasetResponse,
    ExpertDatasetStatsResponse,
    ExpertImportCreateInput,
    ExpertImportMetadataUpdateInput,
    ExpertImportResponse,
    ExpertRoleUpdateInput,
    ExpertUserResponse,
    ExpertAdjudicationInput,
    ExpertExportCreateInput,
    ExpertExportResponse,
    ExpertFinalizeImportInput,
    ExpertModelPrediction,
    ExpertPresignUploadInput,
    ExpertPresignUploadResponse,
    ExpertPresignedUpload,
    ExpertQueueItemResponse,
)
from security import create_token, hash_password, hash_token, verify_api_key, verify_password, validate_upload_file
from app.services.notifier import send_telegram_radar_alert
from billing import (
    activate_subscription,
    cancel_subscription,
    checkout_payload,
    create_checkout_session,
    create_invoice,
    decrement_quota,
    get_or_create_subscription,
    invoice_payload,
    is_unlimited_scan_user,
    normalize_billing_provider,
    quota_limit_for_tier,
    refresh_subscription_state,
    subscription_is_active,
    subscription_payload,
    UNLIMITED_SCAN_LIMIT,
)

# Import of our processing modules
SKIP_MODEL_LOAD = os.getenv("TINSSIT_SKIP_MODEL_LOAD") == "1"
if SKIP_MODEL_LOAD:
    VisionPipeline = None
else:
    from pipeline_vision import VisionPipeline
from fusion_engine import MeteoriteFusionEngine
from business_logic import (
    BusinessOrchestrator,
    INTERIOR_CUT_UNLOCK_THRESHOLD,
    NO_CUT_MAX_SCORE,
    NO_CUT_SCORE_FACTOR,
    apply_interior_cut_score_policy,
)

# Import database and storage components
from database import (
    engine,
    AsyncSessionLocal,
    Base,
    get_db,
    UserModel,
    AuthSessionModel,
    ScanModel,
    CaptureSessionModel,
    ListingModel,
    CollectionItemModel,
    MessageModel,
    MessageThreadModel,
    FavoriteModel,
    NotificationModel,
    PushSubscriptionModel,
    SellerRatingModel,
    WalletAccountModel,
    WalletTransactionModel,
    WithdrawalRequestModel,
    AuditLogModel,
    BillingCheckoutSessionModel,
    BillingEventModel,
    InvoiceModel,
    DatasetBatchModel,
    DatasetImportModel,
    DatasetItemModel,
    AnnotationEventModel,
    DatasetConsensusModel,
    AuditRunModel,
    DatasetExportModel,
)
from storage import storage_provider, UPLOAD_DIR
from expert_dataset import (
    ANNOTATION_POLICY_VERSION,
    ANNOTATION_POLICY_V2_VERSION,
    METEORITE_SUBCLASSES,
    MODEL_VERSION,
    TERRESTRIAL_FAMILIES,
    TAXONOMY_VERSION,
    TAXONOMY_V2_VERSION,
    V2_EVIDENCE_TIERS,
    V2_PRIMARY_VERDICTS,
    V2_SOURCE_TYPES,
    V2_TRAINING_ROLES,
    V2_VIEW_TYPES,
    VISION_V2_DATASET_CONTRACT_VERSION,
    VISION_V2_RELEASE_CONTRACT_VERSION,
    build_audit,
    build_single_image_prediction,
    deterministic_group_split,
    image_quality_report,
    normalize_image_assets,
    perceptual_hash,
    render_audit_html,
    sha256_hex,
    validate_annotation,
    validate_v2_annotation,
    v2_stratified_group_split,
    v2_training_role,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Lifecycle: Initialize database schema at startup if needed
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("ALTER TABLE listings ADD COLUMN IF NOT EXISTS title VARCHAR"))
        await conn.execute(text("ALTER TABLE listings ADD COLUMN IF NOT EXISTS description VARCHAR"))
        await conn.execute(text("ALTER TABLE listings ADD COLUMN IF NOT EXISTS price_mode VARCHAR DEFAULT 'fixed_total'"))
        await conn.execute(text("ALTER TABLE listings ADD COLUMN IF NOT EXISTS region VARCHAR"))
        await conn.execute(text("UPDATE listings SET price_mode = 'fixed_total' WHERE price_mode IS NULL"))
        await conn.execute(text("UPDATE listings SET status = 'published' WHERE status = 'available'"))
        await conn.execute(text("UPDATE listings SET status = 'admin_reserved' WHERE status = 'reserved'"))
        await conn.execute(text("UPDATE listings SET status = 'archived' WHERE status = 'inactive'"))
        await conn.execute(text("ALTER TABLE scans ADD COLUMN IF NOT EXISTS capture_session_id VARCHAR"))
        await conn.execute(text("ALTER TABLE scans ADD COLUMN IF NOT EXISTS capture_mode VARCHAR"))
        await conn.execute(text("ALTER TABLE scans ADD COLUMN IF NOT EXISTS capture_verified BOOLEAN DEFAULT FALSE"))
        await conn.execute(text("ALTER TABLE scans ADD COLUMN IF NOT EXISTS quality_report JSONB"))
        await conn.execute(text("ALTER TABLE scans ADD COLUMN IF NOT EXISTS image_hashes JSONB"))
        await conn.execute(text("ALTER TABLE scans ADD COLUMN IF NOT EXISTS contact_guard JSONB"))
        await conn.execute(text("UPDATE scans SET capture_mode = 'legacy_upload' WHERE capture_mode IS NULL"))
        await conn.execute(text("UPDATE scans SET capture_verified = FALSE WHERE capture_verified IS NULL"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS status VARCHAR DEFAULT 'none'"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS provider VARCHAR"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS plan VARCHAR"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS cancel_at_period_end BOOLEAN DEFAULT FALSE"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS subscription_started_at TIMESTAMP"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP"))
        await conn.execute(text("ALTER TABLE dataset_items ADD COLUMN IF NOT EXISTS import_id VARCHAR"))
        await conn.execute(text("ALTER TABLE dataset_consensus ADD COLUMN IF NOT EXISTS training_eligible BOOLEAN DEFAULT FALSE"))
        await conn.execute(text("ALTER TABLE dataset_consensus ADD COLUMN IF NOT EXISTS final_confidence VARCHAR"))
        await conn.execute(text("ALTER TABLE dataset_exports ADD COLUMN IF NOT EXISTS archive_object_key VARCHAR"))
        await conn.execute(text("ALTER TABLE dataset_imports ADD COLUMN IF NOT EXISTS source_type VARCHAR DEFAULT 'other'"))
        await conn.execute(text("ALTER TABLE dataset_imports ADD COLUMN IF NOT EXISTS source_reference VARCHAR"))
        await conn.execute(text("ALTER TABLE dataset_imports ADD COLUMN IF NOT EXISTS rights_status VARCHAR DEFAULT 'attested'"))
        await conn.execute(text("ALTER TABLE dataset_imports ADD COLUMN IF NOT EXISTS evidence_tier VARCHAR DEFAULT 'unresolved'"))
        await conn.execute(text("ALTER TABLE dataset_imports ADD COLUMN IF NOT EXISTS view_type VARCHAR DEFAULT 'unknown'"))
        await conn.execute(text("ALTER TABLE dataset_imports ADD COLUMN IF NOT EXISTS import_metadata JSONB DEFAULT '{}'::jsonb"))
        await conn.execute(text("ALTER TABLE dataset_items ADD COLUMN IF NOT EXISTS duplicate_group_id VARCHAR"))
        await conn.execute(text("ALTER TABLE dataset_items ADD COLUMN IF NOT EXISTS source_type VARCHAR DEFAULT 'other'"))
        await conn.execute(text("ALTER TABLE dataset_items ADD COLUMN IF NOT EXISTS source_reference VARCHAR"))
        await conn.execute(text("ALTER TABLE dataset_items ADD COLUMN IF NOT EXISTS rights_status VARCHAR DEFAULT 'attested'"))
        await conn.execute(text("ALTER TABLE dataset_items ADD COLUMN IF NOT EXISTS evidence_tier VARCHAR DEFAULT 'unresolved'"))
        await conn.execute(text("ALTER TABLE dataset_items ADD COLUMN IF NOT EXISTS view_type VARCHAR DEFAULT 'unknown'"))
        await conn.execute(text("ALTER TABLE dataset_items ADD COLUMN IF NOT EXISTS historical_label VARCHAR"))
        await conn.execute(text("ALTER TABLE dataset_items ADD COLUMN IF NOT EXISTS audit_not_before TIMESTAMP"))
        await conn.execute(text("ALTER TABLE annotation_events ADD COLUMN IF NOT EXISTS visual_verdict VARCHAR"))
        await conn.execute(text("ALTER TABLE annotation_events ADD COLUMN IF NOT EXISTS evidence_tier VARCHAR"))
        await conn.execute(text("ALTER TABLE annotation_events ADD COLUMN IF NOT EXISTS reason_codes JSONB DEFAULT '[]'::jsonb"))
        await conn.execute(text("ALTER TABLE annotation_events ADD COLUMN IF NOT EXISTS requested_evidence JSONB DEFAULT '[]'::jsonb"))
        await conn.execute(text("ALTER TABLE annotation_events ADD COLUMN IF NOT EXISTS is_blind BOOLEAN DEFAULT FALSE"))
        await conn.execute(text("ALTER TABLE dataset_consensus ADD COLUMN IF NOT EXISTS training_role VARCHAR"))
        await conn.execute(text("ALTER TABLE dataset_consensus ADD COLUMN IF NOT EXISTS evidence_tier VARCHAR"))
        await conn.execute(text("ALTER TABLE dataset_consensus ADD COLUMN IF NOT EXISTS audit_status VARCHAR DEFAULT 'pending'"))
        await conn.execute(text("ALTER TABLE dataset_consensus ADD COLUMN IF NOT EXISTS subclass_eligible BOOLEAN DEFAULT FALSE"))
        await conn.execute(text("UPDATE user_subscriptions SET status = CASE WHEN tier IN ('premium', 'admin') THEN 'active' ELSE 'none' END WHERE status IS NULL"))
        await conn.execute(text("UPDATE user_subscriptions SET cancel_at_period_end = FALSE WHERE cancel_at_period_end IS NULL"))
        await conn.execute(text("UPDATE user_subscriptions SET updated_at = NOW() WHERE updated_at IS NULL"))
        await conn.execute(text(
            "UPDATE scans "
            f"SET meteorite_probability = LEAST(meteorite_probability * {NO_CUT_SCORE_FACTOR}, {NO_CUT_MAX_SCORE}) "
            "WHERE interior_image_path IS NULL "
            f"AND meteorite_probability > {INTERIOR_CUT_UNLOCK_THRESHOLD}"
        ))
    yield

app = FastAPI(
    title="App_meteorite Core Server", 
    description="Back-end expert d'identification avec gestion flexible des flux multimédias",
    lifespan=lifespan
)
configured_cors = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
cors_origins = (
    [origin.strip() for origin in configured_cors.split(",") if origin.strip() and origin.strip() != "*"]
    if configured_cors
    else ["null"]
)
cors_origin_regex = os.getenv(
    "CORS_ALLOWED_ORIGIN_REGEX",
    r"https://.*\.claudeusercontent\.com|https://.*\.claude\.site|https://claude\.ai|http://localhost(:\d+)?|http://127\.0\.0\.1(:\d+)?",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins or ["null"],
    allow_origin_regex=cors_origin_regex,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)
app.mount("/storage", StaticFiles(directory=UPLOAD_DIR), name="storage")
app.add_exception_handler(AppProductionException, app_exception_handler)
app.add_exception_handler(RequestValidationError, request_validation_exception_handler)
app.add_exception_handler(StarletteHTTPException, http_exception_handler)

ERROR_RESPONSES = {
    400: {"model": ApiErrorResponse, "description": "Requete invalide"},
    401: {"model": ApiErrorResponse, "description": "Cle API invalide ou manquante"},
    402: {"model": ApiErrorResponse, "description": "Quota epuise"},
    403: {"model": ApiErrorResponse, "description": "Acces interdit"},
    404: {"model": ApiErrorResponse, "description": "Ressource introuvable"},
    409: {"model": ApiErrorResponse, "description": "Conflit metier"},
    413: {"model": ApiErrorResponse, "description": "Fichier trop volumineux"},
    415: {"model": ApiErrorResponse, "description": "Format de fichier non supporte"},
    422: {"model": ApiErrorResponse, "description": "Erreur de validation"},
    503: {"model": ApiErrorResponse, "description": "Service indisponible"},
    500: {"model": ApiErrorResponse, "description": "Erreur interne"},
}

# Global initialization of our orchestration blocks
vision_pipeline = None if SKIP_MODEL_LOAD else VisionPipeline()
fusion_engine = MeteoriteFusionEngine()
business_orchestrator = BusinessOrchestrator()
METEORITE_CLASSES = ["None", "Achondrite", "Carbonee", "Chondrite", "Metallique", "Meteore_Unknown"]
ALLOW_LEGACY_SCAN_UPLOAD = os.getenv("ALLOW_LEGACY_SCAN_UPLOAD", "1") == "1"
CAPTURE_REQUIRED_STEPS = ["front", "side", "back"]
CAPTURE_OPTIONAL_STEPS = ["macro", "interior"]
CAPTURE_ALL_STEPS = CAPTURE_REQUIRED_STEPS + CAPTURE_OPTIONAL_STEPS
CAPTURE_SESSION_TTL_MINUTES = int(os.getenv("CAPTURE_SESSION_TTL_MINUTES", "45"))
CAPTURE_QUALITY_THRESHOLDS = {
    "min_width": int(os.getenv("CAPTURE_MIN_WIDTH", "640")),
    "min_height": int(os.getenv("CAPTURE_MIN_HEIGHT", "640")),
    "min_sharpness": float(os.getenv("CAPTURE_MIN_SHARPNESS", "18")),
    "min_brightness": float(os.getenv("CAPTURE_MIN_BRIGHTNESS", "35")),
    "max_brightness": float(os.getenv("CAPTURE_MAX_BRIGHTNESS", "235")),
    "max_highlight_ratio": float(os.getenv("CAPTURE_MAX_HIGHLIGHT_RATIO", "0.18")),
}
CONTACT_DIGIT_TRANSLATION = str.maketrans({
    "٠": "0", "١": "1", "٢": "2", "٣": "3", "٤": "4",
    "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9",
    "۰": "0", "۱": "1", "۲": "2", "۳": "3", "۴": "4",
    "۵": "5", "۶": "6", "۷": "7", "۸": "8", "۹": "9",
})
IMAGE_CONTACT_PATTERNS = [
    re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE),
    re.compile(r"(whatsapp|wsp|wa\.me|واتساب|واتس)", re.IGNORECASE),
    re.compile(r"(https?://|www\.|\.com|\.ma)", re.IGNORECASE),
    re.compile(r"(?:\+?212|0)(?:[\s().-]?\d){8,}"),
    re.compile(r"(?:\+?\d[\s().-]?){9,}"),
]


def _capture_quality_thresholds_response() -> dict:
    return dict(CAPTURE_QUALITY_THRESHOLDS)


def _now_naive_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def _read_stored_file_bytes(stored_path: str) -> bytes:
    return await storage_provider.get_object(stored_path)


def _compute_laplacian_like_variance(gray: "Any") -> float:
    try:
        import numpy as np
    except Exception:
        return 0.0

    arr = np.asarray(gray, dtype=np.float32)
    if arr.size == 0:
        return 0.0
    lap = (
        -4 * arr[1:-1, 1:-1]
        + arr[:-2, 1:-1]
        + arr[2:, 1:-1]
        + arr[1:-1, :-2]
        + arr[1:-1, 2:]
    )
    return float(np.var(lap)) if lap.size else 0.0


def _image_quality_report(image_bytes: bytes) -> dict:
    issues: list[str] = []
    with Image.open(io.BytesIO(image_bytes)) as raw_image:
        image = ImageOps.exif_transpose(raw_image).convert("RGB")
        width, height = image.size
        gray = image.convert("L")
        try:
            import numpy as np

            gray_arr = np.asarray(gray, dtype=np.float32)
            rgb_arr = np.asarray(image, dtype=np.uint8)
            brightness = float(np.mean(gray_arr))
            highlight_ratio = float(np.mean(gray_arr >= 245))
            shadow_ratio = float(np.mean(gray_arr <= 18))
            saturation_high_ratio = float(np.mean(np.max(rgb_arr, axis=2) >= 248))
        except Exception:
            histogram = gray.histogram()
            total = max(1, sum(histogram))
            brightness = sum(i * count for i, count in enumerate(histogram)) / total
            highlight_ratio = sum(histogram[245:]) / total
            shadow_ratio = sum(histogram[:19]) / total
            saturation_high_ratio = highlight_ratio
        sharpness = _compute_laplacian_like_variance(gray)

    thresholds = CAPTURE_QUALITY_THRESHOLDS
    if width < thresholds["min_width"] or height < thresholds["min_height"]:
        issues.append("LOW_RESOLUTION")
    if sharpness < thresholds["min_sharpness"]:
        issues.append("BLURRY")
    if brightness < thresholds["min_brightness"]:
        issues.append("UNDEREXPOSED")
    if brightness > thresholds["max_brightness"]:
        issues.append("OVEREXPOSED")
    if highlight_ratio > thresholds["max_highlight_ratio"] or saturation_high_ratio > thresholds["max_highlight_ratio"]:
        issues.append("GLARE_OR_HIGHLIGHTS")

    quality_score = max(
        0.0,
        min(
            1.0,
            0.30 * min(width, height) / max(thresholds["min_width"], thresholds["min_height"])
            + 0.35 * min(sharpness / max(1.0, thresholds["min_sharpness"] * 3), 1.0)
            + 0.20 * (1.0 - min(abs(brightness - 128.0) / 128.0, 1.0))
            + 0.15 * (1.0 - min(highlight_ratio / max(0.01, thresholds["max_highlight_ratio"]), 1.0))
        ),
    )
    return {
        "passed": not issues,
        "issues": issues,
        "width": width,
        "height": height,
        "bytes": len(image_bytes),
        "sharpness": round(sharpness, 3),
        "brightness": round(brightness, 3),
        "highlight_ratio": round(highlight_ratio, 5),
        "shadow_ratio": round(shadow_ratio, 5),
        "score": round(quality_score, 4),
        "thresholds": _capture_quality_thresholds_response(),
    }


def _normalize_contact_text(value: str) -> str:
    return (value or "").translate(CONTACT_DIGIT_TRANSLATION)


def _extract_text_from_image(image_bytes: bytes) -> tuple[str, str]:
    try:
        import pytesseract
    except Exception:
        return "", "unavailable"

    try:
        with Image.open(io.BytesIO(image_bytes)) as raw_image:
            image = ImageOps.exif_transpose(raw_image).convert("RGB")
            text = pytesseract.image_to_string(image, lang=os.getenv("TISSINT_OCR_LANG", "ara+fra+eng"))
            return text or "", "pytesseract"
    except Exception:
        return "", "unavailable"


def _contact_guard_report(image_bytes: bytes) -> dict:
    extracted_text, engine = _extract_text_from_image(image_bytes)
    normalized = _normalize_contact_text(extracted_text)
    matched = [
        pattern.pattern
        for pattern in IMAGE_CONTACT_PATTERNS
        if pattern.search(normalized)
    ]
    status_value = "checked" if engine != "unavailable" else "not_available"
    return {
        "passed": not matched,
        "status": status_value,
        "engine": engine,
        "matched_patterns": matched,
        "text_sample": normalized[:120] if normalized else "",
    }


def _capture_step_paths(session: CaptureSessionModel) -> dict:
    metadata = session.capture_metadata or {}
    return dict(metadata.get("step_paths") or {})


def _set_capture_step_path(session: CaptureSessionModel, step: str, stored_path: str) -> None:
    metadata = dict(session.capture_metadata or {})
    step_paths = dict(metadata.get("step_paths") or {})
    step_paths[step] = stored_path
    metadata["step_paths"] = step_paths
    session.capture_metadata = metadata
    session.exterior_images_paths = [
        step_paths[step_name]
        for step_name in CAPTURE_REQUIRED_STEPS + ["macro"]
        if step_paths.get(step_name)
    ]
    if step == "interior":
        session.interior_image_path = stored_path
    session.updated_at = _now_naive_utc()


def _capture_count(session: CaptureSessionModel) -> int:
    step_paths = _capture_step_paths(session)
    return sum(1 for step_name in CAPTURE_REQUIRED_STEPS if step_paths.get(step_name))


def _session_required_paths(session: CaptureSessionModel) -> list[str]:
    step_paths = _capture_step_paths(session)
    return [step_paths[step_name] for step_name in CAPTURE_REQUIRED_STEPS if step_paths.get(step_name)]


def _session_interior_path(session: CaptureSessionModel) -> Optional[str]:
    step_paths = _capture_step_paths(session)
    return step_paths.get("interior") or session.interior_image_path


def _round_score(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 6)


def _detect_client_platform(user_agent: Optional[str]) -> str:
    ua = (user_agent or "").lower()
    if "android" in ua:
        return "android"
    if "iphone" in ua or "ipad" in ua or "ios" in ua:
        return "ios"
    if "mobile" in ua:
        return "mobile_web"
    if ua:
        return "desktop_web"
    return "unknown"


def _upload_file_summary(file: UploadFile, data: bytes, index: int) -> dict:
    width = None
    height = None
    exif_orientation = None
    try:
        with Image.open(io.BytesIO(data)) as image:
            width, height = image.size
            exif_orientation = image.getexif().get(274)
    except Exception:
        pass

    return {
        "index": index,
        "filename": file.filename,
        "content_type": file.content_type,
        "size_bytes": len(data),
        "width": width,
        "height": height,
        "exif_orientation": exif_orientation,
        "sha256_16": hashlib.sha256(data).hexdigest()[:16],
    }


def _vision_output_summary(vision_outputs: dict) -> dict:
    summary = {}
    for area_name in ("exterior", "interior"):
        area = vision_outputs.get(area_name)
        if not area:
            summary[area_name] = None
            continue
        area_summary = {}
        for model_name, model_output in area.items():
            prob_sub = model_output.get("prob_sub") or []
            top_idx = max(range(len(prob_sub)), key=lambda i: prob_sub[i]) if prob_sub else None
            area_summary[model_name] = {
                "prob_bin": _round_score(model_output.get("prob_bin")),
                "top_class": METEORITE_CLASSES[top_idx] if top_idx is not None and top_idx < len(METEORITE_CLASSES) else None,
                "top_prob": _round_score(prob_sub[top_idx]) if top_idx is not None else None,
            }
        summary[area_name] = area_summary

    exterior_per_image = []
    for image_summary in vision_outputs.get("exterior_per_image") or []:
        model_summaries = {}
        for model_name, model_output in (image_summary.get("models") or {}).items():
            model_summaries[model_name] = {
                "prob_bin": _round_score(model_output.get("prob_bin")),
                "top_class": model_output.get("top_class"),
                "top_prob": _round_score(model_output.get("top_prob")),
            }
        exterior_per_image.append({
            "index": image_summary.get("index"),
            "prob_bin": _round_score(image_summary.get("prob_bin")),
            "top_class": image_summary.get("top_class"),
            "top_prob": _round_score(image_summary.get("top_prob")),
            "models": model_summaries,
        })
    if exterior_per_image:
        summary["exterior_per_image"] = exterior_per_image
    return summary


def _print_scan_event(event: str, payload: dict) -> None:
    print(f"[{event}] {json.dumps(payload, ensure_ascii=True, sort_keys=True)}")

@app.get(
    "/health",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    responses={503: {"model": ApiErrorResponse, "description": "Dependance indisponible"}},
)
async def healthcheck():
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database_unavailable: {exc}")

    return {
        "status": "ok",
        "service": "tinssit-backend",
        "database": "ok"
    }

ACCESS_TOKEN_TTL_MINUTES = int(os.getenv("ACCESS_TOKEN_TTL_MINUTES", "30"))
REFRESH_TOKEN_TTL_DAYS = int(os.getenv("REFRESH_TOKEN_TTL_DAYS", "30"))

def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _normalize_email(email: Optional[str]) -> Optional[str]:
    return email.strip().lower() if email and email.strip() else None

def _normalize_phone(phone: str) -> str:
    return phone.strip()

def _quota_response(subscription, user: UserModel | None = None) -> QuotaResponse:
    subscription_state = subscription_payload(subscription)
    role = user.role if user and user.role == "expert" else subscription_state["role"]
    if role == "expert":
        return QuotaResponse(role=role, daily_limit=0, remaining_today=0, resets_at=None)
    if is_unlimited_scan_user(user):
        return QuotaResponse(
            role=role,
            daily_limit=UNLIMITED_SCAN_LIMIT,
            remaining_today=UNLIMITED_SCAN_LIMIT,
            resets_at=None,
        )
    daily_limit = quota_limit_for_tier(role)
    remaining_today = daily_limit if role in {"premium", "admin"} else max(subscription.remaining_tokens, 0)
    return QuotaResponse(
        role=role,
        daily_limit=daily_limit,
        remaining_today=remaining_today,
        resets_at=None,
    )

def _subscription_response(subscription) -> SubscriptionResponse:
    return SubscriptionResponse(**subscription_payload(subscription))

def _auth_user_response(user: UserModel, subscription) -> AuthUserResponse:
    subscription_state = subscription_payload(subscription)
    role = user.role if user.role == "expert" else subscription_state["role"]
    return AuthUserResponse(
        id=user.id,
        first_name=user.first_name,
        last_name=user.last_name,
        phone=user.phone,
        email=user.email,
        role=role,
        premium_expires_at=(
            subscription.subscription_expires_at.isoformat()
            if subscription.subscription_expires_at
            else None
        ),
    )

def _bearer_token(authorization: Optional[str]) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise AppProductionException("UNAUTHORIZED", "Session invalide ou expiree.", 401)
    token = authorization[7:].strip()
    if not token:
        raise AppProductionException("UNAUTHORIZED", "Session invalide ou expiree.", 401)
    return token

async def _create_auth_session(user_id: str, device_id: Optional[str], db: AsyncSession):
    access_token = create_token()
    refresh_token = create_token()
    now = _utc_now()
    session = AuthSessionModel(
        id=str(uuid.uuid4()),
        user_id=user_id,
        device_id=device_id,
        access_token_hash=hash_token(access_token),
        refresh_token_hash=hash_token(refresh_token),
        access_expires_at=now + timedelta(minutes=ACCESS_TOKEN_TTL_MINUTES),
        refresh_expires_at=now + timedelta(days=REFRESH_TOKEN_TTL_DAYS),
    )
    db.add(session)
    await db.commit()
    await db.refresh(session)
    return session, access_token, refresh_token

async def _auth_response(
    user: UserModel,
    session: AuthSessionModel,
    subscription,
    access_token: str,
    refresh_token: str = "",
) -> AuthResponse:
    return AuthResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=session.access_expires_at.isoformat(),
        user=_auth_user_response(user, subscription),
        quota=_quota_response(subscription, user),
    )

async def _current_auth_context(
    authorization: Optional[str],
    db: AsyncSession,
):
    token = _bearer_token(authorization)
    token_hash = hash_token(token)
    now = _utc_now()
    result = await db.execute(
        select(AuthSessionModel).where(
            AuthSessionModel.access_token_hash == token_hash,
            AuthSessionModel.revoked_at.is_(None),
            AuthSessionModel.access_expires_at > now,
        )
    )
    session = result.scalar_one_or_none()
    if not session:
        raise AppProductionException("UNAUTHORIZED", "Session invalide ou expiree.", 401)

    user_result = await db.execute(select(UserModel).where(UserModel.id == session.user_id))
    user = user_result.scalar_one_or_none()
    if not user:
        raise AppProductionException("UNAUTHORIZED", "Session invalide ou expiree.", 401)

    subscription = await get_or_create_subscription(user.id, db)
    return user, session, subscription, token

async def _optional_auth_context(
    authorization: Optional[str],
    db: AsyncSession,
):
    if not authorization:
        return None, None
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    return user, subscription

async def _require_admin_context(
    authorization: Optional[str],
    db: AsyncSession,
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    if user.role != "admin" and subscription.tier != "admin":
        raise AppProductionException("FORBIDDEN", "Acces admin requis.", 403)
    return user, subscription


async def _require_expert_context(
    authorization: Optional[str],
    db: AsyncSession,
    require_review: bool = False,
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    role = user.role
    if role not in {"expert", "admin"} and subscription.tier != "admin":
        raise AppProductionException("FORBIDDEN", "Acces expert requis.", 403)
    return user, subscription

def _validate_mobile_user_id(user_id: str) -> str:
    if len(user_id) < 3 or len(user_id) > 100:
        raise AppProductionException("VALIDATION_ERROR", "Identifiant utilisateur invalide.", 400)
    if not all(char.isalnum() or char in {"_", "-"} for char in user_id):
        raise AppProductionException("VALIDATION_ERROR", "Identifiant utilisateur invalide.", 400)
    return user_id

def _normalize_optional_user_id(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    user_id = value.strip()
    if not user_id:
        return None
    return _validate_mobile_user_id(user_id)

def resolve_user_id(x_user_id: Optional[str] = Header(None)) -> str:
    return _normalize_optional_user_id(x_user_id) or "anonymous"

async def _resolve_mobile_identity(
    db: AsyncSession,
    authorization: Optional[str] = None,
    x_user_id: Optional[str] = None,
    form_user_id: Optional[str] = None,
    require_user: bool = False,
):
    legacy_ids = [
        user_id
        for user_id in (
            _normalize_optional_user_id(x_user_id),
            _normalize_optional_user_id(form_user_id),
        )
        if user_id and user_id != "anonymous"
    ]
    if legacy_ids and any(user_id != legacy_ids[0] for user_id in legacy_ids):
        raise AppProductionException("FORBIDDEN", "Identifiant utilisateur divergent.", 403)

    if authorization:
        user, _session, subscription, _token = await _current_auth_context(authorization, db)
        if legacy_ids and legacy_ids[0] != user.id:
            raise AppProductionException("FORBIDDEN", "Identifiant utilisateur divergent.", 403)
        return user.id, user, subscription

    if require_user and not legacy_ids:
        raise AppProductionException("UNAUTHORIZED", "Session requise.", 401)

    user_id = legacy_ids[0] if legacy_ids else "anonymous"
    subscription = await get_or_create_subscription(user_id, db)
    return user_id, None, subscription

async def _check_scan_quota_for_request(
    db: AsyncSession,
    authorization: Optional[str],
    x_user_id: Optional[str],
    form_user_id: str,
):
    user_id, user, subscription = await _resolve_mobile_identity(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        form_user_id=form_user_id,
        require_user=True,
    )
    refresh_subscription_state(subscription, user)

    if is_unlimited_scan_user(user):
        return user_id, user, subscription

    if subscription.tier in {"premium", "admin"} and subscription_is_active(subscription):
        return user_id, user, subscription

    if subscription.remaining_tokens <= 0:
        raise AppProductionException(
            error_code="QUOTA_EXCEEDED",
            message="Quota de scans epuise. Passez a la version Premium !",
            status_code=402,
        )

    return user_id, user, subscription

@app.post(
    "/api/v1/auth/register",
    response_model=AuthResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def register(
    payload: RegisterInput,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    phone = _normalize_phone(payload.phone)
    email = _normalize_email(payload.email)
    existing_query = select(UserModel).where(UserModel.phone == phone)
    if email:
        existing_query = select(UserModel).where(or_(UserModel.phone == phone, UserModel.email == email))
    existing_result = await db.execute(existing_query)
    if existing_result.scalar_one_or_none():
        raise AppProductionException("CONFLICT", "Un compte existe deja avec ces identifiants.", 409)

    user = UserModel(
        id=str(uuid.uuid4()),
        first_name=payload.first_name.strip(),
        last_name=payload.last_name.strip(),
        phone=phone,
        email=email,
        password_hash=hash_password(payload.password),
        role=payload.desired_role,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    subscription = await get_or_create_subscription(user.id, db)
    subscription.tier = payload.desired_role
    subscription.remaining_tokens = quota_limit_for_tier(payload.desired_role)
    await db.commit()
    await db.refresh(subscription)

    session, access_token, refresh_token = await _create_auth_session(user.id, payload.device_id, db)
    return await _auth_response(user, session, subscription, access_token, refresh_token)


@app.post(
    "/api/v1/auth/login",
    response_model=AuthResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def login(
    payload: LoginInput,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    identifier = payload.phone_or_email.strip()
    email = _normalize_email(identifier)
    result = await db.execute(
        select(UserModel).where(or_(UserModel.phone == identifier, UserModel.email == email))
    )
    user = result.scalar_one_or_none()
    if not user or not verify_password(payload.password, user.password_hash):
        raise AppProductionException("UNAUTHORIZED", "Identifiants invalides.", 401)

    subscription = await get_or_create_subscription(user.id, db)
    await db.commit()
    await db.refresh(user)

    session, access_token, refresh_token = await _create_auth_session(user.id, payload.device_id, db)
    return await _auth_response(user, session, subscription, access_token, refresh_token)


@app.get(
    "/api/v1/auth/me",
    response_model=AuthResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def auth_me(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, session, subscription, access_token = await _current_auth_context(authorization, db)
    return await _auth_response(user, session, subscription, access_token)


@app.post(
    "/api/v1/auth/refresh",
    response_model=AuthResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def refresh_auth(
    payload: RefreshTokenInput,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    now = _utc_now()
    result = await db.execute(
        select(AuthSessionModel).where(
            AuthSessionModel.refresh_token_hash == hash_token(payload.refresh_token),
            AuthSessionModel.revoked_at.is_(None),
            AuthSessionModel.refresh_expires_at > now,
        )
    )
    session = result.scalar_one_or_none()
    if not session:
        raise AppProductionException("UNAUTHORIZED", "Session invalide ou expiree.", 401)

    user_result = await db.execute(select(UserModel).where(UserModel.id == session.user_id))
    user = user_result.scalar_one_or_none()
    if not user:
        raise AppProductionException("UNAUTHORIZED", "Session invalide ou expiree.", 401)

    access_token = create_token()
    refresh_token = create_token()
    session.access_token_hash = hash_token(access_token)
    session.refresh_token_hash = hash_token(refresh_token)
    session.access_expires_at = now + timedelta(minutes=ACCESS_TOKEN_TTL_MINUTES)
    session.refresh_expires_at = now + timedelta(days=REFRESH_TOKEN_TTL_DAYS)
    await db.commit()
    await db.refresh(session)

    subscription = await get_or_create_subscription(user.id, db)
    return await _auth_response(user, session, subscription, access_token, refresh_token)


@app.post(
    "/api/v1/auth/logout",
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def logout(
    payload: LogoutInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    token_hash = hash_token(payload.refresh_token) if payload.refresh_token else None
    if not token_hash and authorization:
        token_hash = hash_token(_bearer_token(authorization))

    if token_hash:
        result = await db.execute(
            select(AuthSessionModel).where(
                or_(
                    AuthSessionModel.refresh_token_hash == token_hash,
                    AuthSessionModel.access_token_hash == token_hash,
                )
            )
        )
        session = result.scalar_one_or_none()
        if session and session.revoked_at is None:
            session.revoked_at = _utc_now()
            await db.commit()

    return {"status": "ok"}

@app.get(
    "/api/v1/quota/me",
    response_model=QuotaResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_quota_me(
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    _user_id, user, subscription = await _resolve_mobile_identity(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
    )
    refresh_subscription_state(subscription, user)
    await db.commit()
    return _quota_response(subscription, user)


@app.post(
    "/api/v1/billing/checkout",
    response_model=CheckoutSessionResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def create_billing_checkout(
    payload: BillingCheckoutInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    refresh_subscription_state(subscription, user)
    checkout = await create_checkout_session(
        user=user,
        subscription=subscription,
        provider=payload.provider,
        plan=payload.plan,
        return_url=payload.return_url,
        db=db,
    )
    await _write_audit_log(
        db,
        actor_user_id=user.id,
        action="billing_checkout_created",
        entity_type="checkout_session",
        entity_id=checkout.id,
        metadata={
            "provider": checkout.provider,
            "plan": checkout.plan,
            "status": checkout.status,
            "amount_dh": checkout.amount_dh,
        },
    )
    await db.commit()
    await db.refresh(checkout)
    return CheckoutSessionResponse(**checkout_payload(checkout))


@app.get(
    "/api/v1/billing/subscription",
    response_model=SubscriptionResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_billing_subscription(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    refresh_subscription_state(subscription, user)
    await db.commit()
    return _subscription_response(subscription)


@app.post(
    "/api/v1/billing/cancel",
    response_model=SubscriptionResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def cancel_billing_subscription(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    await cancel_subscription(user, subscription, db)
    await _write_audit_log(
        db,
        actor_user_id=user.id,
        action="billing_subscription_cancelled",
        entity_type="subscription",
        entity_id=user.id,
        metadata=subscription_payload(subscription),
    )
    await db.commit()
    return _subscription_response(subscription)


@app.get(
    "/api/v1/billing/invoices",
    response_model=List[InvoiceResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def list_billing_invoices(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    result = await db.execute(
        select(InvoiceModel)
        .where(InvoiceModel.user_id == user.id)
        .order_by(InvoiceModel.created_at.desc())
    )
    return [InvoiceResponse(**invoice_payload(invoice)) for invoice in result.scalars().all()]


@app.post(
    "/api/v1/billing/webhooks/{provider}",
    response_model=BillingWebhookResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def handle_billing_webhook(
    provider: str,
    payload: Optional[dict] = Body(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    payload = payload or {}
    provider = normalize_billing_provider(provider)
    event_type = str(payload.get("type") or payload.get("event_type") or "unknown")
    encoded_payload = json.dumps(payload, sort_keys=True, default=str)
    event_id = str(payload.get("event_id") or payload.get("id") or hashlib.sha256(encoded_payload.encode("utf-8")).hexdigest())

    existing_event = await db.execute(
        select(BillingEventModel).where(
            BillingEventModel.provider == provider,
            BillingEventModel.event_id == event_id,
        )
    )
    if existing_event.scalar_one_or_none():
        return BillingWebhookResponse(status="duplicate", processed=False, event_id=event_id)

    checkout_session_id = payload.get("checkout_session_id") or payload.get("session_id")
    user_id = payload.get("user_id")
    checkout = None
    if checkout_session_id:
        checkout_result = await db.execute(
            select(BillingCheckoutSessionModel).where(BillingCheckoutSessionModel.id == checkout_session_id)
        )
        checkout = checkout_result.scalar_one_or_none()
        if checkout:
            user_id = user_id or checkout.user_id

    user = None
    subscription = None
    if user_id:
        user_result = await db.execute(select(UserModel).where(UserModel.id == user_id))
        user = user_result.scalar_one_or_none()
        if user:
            subscription = await get_or_create_subscription(user.id, db)

    if event_type in {"checkout.completed", "invoice.paid", "payment_succeeded", "subscription.activated"}:
        if not user or not subscription:
            raise AppProductionException("VALIDATION_ERROR", "Webhook billing sans utilisateur valide.", 400)
        plan = str(payload.get("plan") or (checkout.plan if checkout else "monthly"))
        if checkout:
            checkout.status = "paid"
            checkout.completed_at = _utc_now()
            existing_invoice = await db.execute(
                select(InvoiceModel).where(InvoiceModel.checkout_session_id == checkout.id)
            )
            if not existing_invoice.scalar_one_or_none():
                create_invoice(user.id, checkout, db)
        await activate_subscription(user, subscription, provider, plan, db)
    elif event_type in {"subscription.cancelled", "customer.subscription.deleted"}:
        if not user or not subscription:
            raise AppProductionException("VALIDATION_ERROR", "Webhook billing sans utilisateur valide.", 400)
        await cancel_subscription(user, subscription, db)
    elif event_type in {"invoice.payment_failed", "payment_failed"}:
        if subscription:
            subscription.status = "past_due"
            subscription.updated_at = _utc_now()

    event = BillingEventModel(
        id=str(uuid.uuid4()),
        provider=provider,
        event_id=event_id,
        event_type=event_type,
        user_id=user.id if user else None,
        checkout_session_id=checkout.id if checkout else None,
        payload=payload,
        processed_at=_utc_now(),
    )
    db.add(event)
    await db.commit()

    return BillingWebhookResponse(
        status="processed",
        processed=True,
        event_id=event_id,
        subscription=_subscription_response(subscription) if subscription else None,
    )


def _blur_coordinates(scan: ScanModel):
    safe_lat = round(scan.latitude, 1) if scan.latitude is not None else None
    safe_lon = round(scan.longitude, 1) if scan.longitude is not None else None
    return safe_lat, safe_lon

def _scan_main_image_uri(scan: ScanModel) -> Optional[str]:
    if scan.exterior_images_paths:
        return storage_provider.public_url(scan.exterior_images_paths[0])
    return None

def _scan_thumbnail_uri(scan: ScanModel) -> Optional[str]:
    return _scan_main_image_uri(scan)

def _scan_interior_image_uri(scan: ScanModel) -> Optional[str]:
    return storage_provider.public_url(scan.interior_image_path)

def _scan_has_interior_cut(scan: ScanModel) -> bool:
    return bool(scan.interior_image_path)

def _scan_user_score(scan: ScanModel) -> float:
    return apply_interior_cut_score_policy(
        scan.meteorite_probability,
        has_interior_cut=_scan_has_interior_cut(scan),
    )

def _listing_image_fields(scan: ScanModel) -> dict:
    main_image_uri = _scan_main_image_uri(scan)
    gallery_images: list[str] = []
    for stored_path in scan.exterior_images_paths or []:
        public_url = storage_provider.public_url(stored_path)
        if public_url and public_url not in gallery_images:
            gallery_images.append(public_url)
    interior_image_uri = _scan_interior_image_uri(scan)
    if interior_image_uri and interior_image_uri not in gallery_images:
        gallery_images.append(interior_image_uri)
    return {
        "main_image_uri": main_image_uri,
        "image_url": main_image_uri,
        "thumbnail_uri": _scan_thumbnail_uri(scan),
        "interior_image_uri": interior_image_uri,
        "gallery_images": gallery_images,
    }

def _marketplace_priority_ordering():
    premium_case = case(
        (
            (ScanModel.interior_image_path.isnot(None))
            & (ScanModel.meteorite_probability > INTERIOR_CUT_UNLOCK_THRESHOLD),
            1,
        ),
        else_=0,
    )
    return premium_case.desc(), ListingModel.created_at.desc()

async def _create_notification(
    db: AsyncSession,
    user_id: str,
    type_value: str,
    title: str,
    body: str,
    action: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> NotificationModel:
    notification = NotificationModel(
        id=str(uuid.uuid4()),
        user_id=user_id,
        type=type_value,
        title=title,
        body=body,
        action=action,
        event_metadata=metadata,
    )
    db.add(notification)
    return notification

async def _send_push_to_user(db: AsyncSession, user_id: str, payload: dict) -> None:
    vapid_private_key = os.getenv("VAPID_PRIVATE_KEY")
    vapid_contact = os.getenv("VAPID_CONTACT", "mailto:contact@tissint.ma")
    if not vapid_private_key:
        return

    try:
        from pywebpush import WebPushException, webpush
    except Exception:
        return

    result = await db.execute(select(PushSubscriptionModel).where(PushSubscriptionModel.user_id == user_id))
    subscriptions = result.scalars().all()
    for subscription in subscriptions:
        try:
            webpush(
                subscription_info={"endpoint": subscription.endpoint, "keys": subscription.keys},
                data=json.dumps(payload),
                vapid_private_key=vapid_private_key,
                vapid_claims={"sub": vapid_contact},
            )
        except WebPushException:
            continue

def _is_rare_candidate(dominant_class: str, confidence: float) -> bool:
    rare_classes = ["Achondrite", "Carbonee", "Martian", "Lunar", "Pallasite", "Iron", "Metallique"]
    return dominant_class in rare_classes and confidence >= 0.85

MARKETPLACE_VISIBLE_STATUSES = {
    "published",
    "institutional_hold_24h",
    "admin_reserved",
    "sold",
}
MARKETPLACE_LOCKED_FOR_SELLER_STATUSES = {
    "admin_reserved",
    "sold",
    "rejected",
    "archived",
    "removed",
}
CONTACT_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
CONTACT_PHONE_RE = re.compile(r"(?:\+?\d[\s().-]?){8,}")
CONTACT_WHATSAPP_RE = re.compile(
    r"(whatsapp|wsp|wa\.me|\u0648\u0627\u062a\u0633\u0627\u0628|\u0648\u0627\u062a\u0633)",
    re.IGNORECASE,
)

def _clean_optional_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None

def _contains_contact_leak(value: Optional[str]) -> bool:
    if not value:
        return False
    return any(
        pattern.search(value)
        for pattern in (CONTACT_EMAIL_RE, CONTACT_PHONE_RE, CONTACT_WHATSAPP_RE)
    )

def _legacy_listing_status(status_value: Optional[str]) -> str:
    legacy = {
        "available": "published",
        "reserved": "admin_reserved",
        "inactive": "archived",
    }
    return legacy.get(status_value or "draft", status_value or "draft")

def _listing_hold_until(listing: ListingModel) -> Optional[datetime]:
    if _legacy_listing_status(listing.status) != "institutional_hold_24h" or not listing.created_at:
        return None
    hold_until = listing.created_at + timedelta(hours=24)
    return hold_until if hold_until > _utc_now() else None

def _effective_listing_status(listing: ListingModel) -> str:
    status_value = _legacy_listing_status(listing.status)
    if status_value == "institutional_hold_24h" and _listing_hold_until(listing) is None:
        return "published"
    return status_value

def _marketplace_status_for_publish(scan: ScanModel, is_rare: bool) -> str:
    return "institutional_hold_24h" if is_rare else "published"

def _listing_price_mode(listing: ListingModel) -> str:
    if listing.price_mode:
        return listing.price_mode
    return "on_request" if listing.price <= 0 else "fixed_total"

def _can_contact_listing(viewer_role: str, status_value: str) -> bool:
    if status_value == "institutional_hold_24h":
        return viewer_role == "admin"
    if status_value in {"published", "admin_reserved"}:
        return viewer_role in {"premium", "admin"}
    return False

def _contact_lock_reason(viewer_role: str, status_value: str) -> Optional[str]:
    if _can_contact_listing(viewer_role, status_value):
        return None
    if status_value == "institutional_hold_24h":
        return "institutional_hold_24h"
    if status_value in {"sold", "rejected", "archived", "removed"}:
        return "listing_unavailable"
    if viewer_role not in {"premium", "admin"}:
        return "premium_required"
    return "contact_locked"

def _seller_masked_name(seller: Optional[UserModel]) -> str:
    if not seller:
        return "Vendeur Tissint"
    first_name = _clean_optional_text(seller.first_name) or "Vendeur"
    last_initial = (_clean_optional_text(seller.last_name) or "")[:1]
    return f"{first_name} {last_initial}.".strip()

def _seller_full_name(seller: Optional[UserModel]) -> Optional[str]:
    if not seller:
        return None
    name = " ".join(
        part for part in [seller.first_name, seller.last_name] if _clean_optional_text(part)
    ).strip()
    return name or None

async def _write_audit_log(
    db: AsyncSession,
    actor_user_id: str,
    action: str,
    entity_type: str,
    entity_id: str,
    metadata: Optional[dict] = None,
) -> AuditLogModel:
    log = AuditLogModel(
        id=str(uuid.uuid4()),
        actor_user_id=actor_user_id,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        event_metadata=metadata,
    )
    db.add(log)
    return log

def _audit_log_response(log: AuditLogModel) -> AuditLogResponse:
    return AuditLogResponse(
        id=log.id,
        actor_user_id=log.actor_user_id,
        action=log.action,
        entity_type=log.entity_type,
        entity_id=log.entity_id,
        metadata=log.event_metadata,
        created_at=log.created_at.isoformat() if log.created_at else "",
    )

def _admin_radar_listing_response(
    listing: ListingModel,
    scan: ScanModel,
    seller: Optional[UserModel],
) -> AdminRadarListingResponse:
    hold_until = _listing_hold_until(listing)
    image_fields = _listing_image_fields(scan)
    has_interior_cut = _scan_has_interior_cut(scan)
    score = _scan_user_score(scan)
    return AdminRadarListingResponse(
        listing_id=listing.id,
        scan_id=scan.id,
        status=_effective_listing_status(listing),
        dominant_class=scan.dominant_class,
        confidence=scan.class_confidence,
        class_confidence=scan.class_confidence,
        meteorite_probability=score,
        fusion_score=score,
        has_interior_cut=has_interior_cut,
        price=listing.price,
        price_mode=_listing_price_mode(listing),
        title=listing.title or scan.dominant_class,
        description=listing.description,
        region=listing.region,
        weight=scan.weight,
        weight_g=scan.weight,
        magnetic=scan.magnetic,
        latitude=scan.latitude,
        longitude=scan.longitude,
        is_rare=_is_rare_candidate(scan.dominant_class, scan.class_confidence),
        hold_until=hold_until.isoformat() if hold_until else None,
        created_at=listing.created_at.isoformat() if listing.created_at else None,
        seller_user_id=seller.id if seller else scan.user_id,
        seller_name=_seller_full_name(seller),
        seller_phone=seller.phone if seller else None,
        seller_email=seller.email if seller else None,
        seller_verified=seller is not None,
        **image_fields,
    )

def _public_listing_item(
    listing: ListingModel,
    scan: ScanModel,
    seller: Optional[UserModel] = None,
    viewer_role: str = "guest",
) -> PublicListingItem:
    safe_lat, safe_lon = _blur_coordinates(scan)
    status_value = _effective_listing_status(listing)
    can_contact = _can_contact_listing(viewer_role, status_value)
    contact_locked_until = _listing_hold_until(listing)
    seller_phone = seller.phone if seller and can_contact else None
    image_fields = _listing_image_fields(scan)
    has_interior_cut = _scan_has_interior_cut(scan)
    score = _scan_user_score(scan)
    return PublicListingItem(
        listing_id=listing.id,
        scan_id=scan.id,
        price=listing.price,
        status=status_value,
        dominant_class=scan.dominant_class,
        confidence=scan.class_confidence,
        class_confidence=scan.class_confidence,
        meteorite_probability=score,
        fusion_score=score,
        has_interior_cut=has_interior_cut,
        weight=scan.weight,
        weight_g=scan.weight,
        blurred_latitude=safe_lat,
        blurred_longitude=safe_lon,
        is_rare=_is_rare_candidate(scan.dominant_class, scan.class_confidence),
        price_mode=_listing_price_mode(listing),
        created_at=listing.created_at.isoformat() if listing.created_at else None,
        title=listing.title or scan.dominant_class,
        description=listing.description,
        region=listing.region,
        seller_masked_name=_seller_masked_name(seller),
        seller_name=_seller_full_name(seller) if can_contact else None,
        seller_phone=seller_phone,
        seller_whatsapp=seller_phone,
        seller_verified=seller is not None,
        can_contact=can_contact,
        contact_lock_reason=_contact_lock_reason(viewer_role, status_value),
        contact_locked_until=contact_locked_until.isoformat() if contact_locked_until else None,
        **image_fields,
    )

def _collection_status_for_scan(scan: ScanModel, listing: Optional[ListingModel] = None) -> str:
    if listing:
        status_value = _effective_listing_status(listing)
        if status_value == "sold":
            return "sold"
        if status_value in {"published", "institutional_hold_24h", "admin_reserved"}:
            return "listed"
    if scan.status_code == "DIAGNOSTIC_SUCCESS_HIGH":
        return "eligible"
    if scan.status_code == "DIAGNOSTIC_HESITANT":
        if scan.interior_image_path:
            return "pending_validation"
        return "needs_cut"
    return "pending_validation"

def _collection_item_response(
    collection: CollectionItemModel,
    scan: ScanModel,
    listing: Optional[ListingModel] = None,
) -> CollectionItemResponse:
    status_value = _collection_status_for_scan(scan, listing)
    if collection.status != status_value:
        collection.status = status_value
    image_fields = _listing_image_fields(scan)
    has_interior_cut = _scan_has_interior_cut(scan)
    score = _scan_user_score(scan)

    return CollectionItemResponse(
        id=collection.id,
        scan_id=scan.id,
        class_name=scan.dominant_class,
        fusion_score=score,
        status=status_value,
        status_code=scan.status_code,
        is_meteorite=scan.is_meteorite,
        class_confidence=scan.class_confidence,
        has_interior_cut=has_interior_cut,
        created_at=collection.created_at.isoformat() if collection.created_at else "",
        weight_g=scan.weight,
        magnetic=scan.magnetic,
        latitude=scan.latitude,
        longitude=scan.longitude,
        region=listing.region if listing else None,
        notes=listing.description if listing else None,
        meteorite_probability=score,
        **image_fields,
    )

async def _sync_collection_status_for_listing(
    db: AsyncSession,
    scan: ScanModel,
    listing: ListingModel,
) -> None:
    result = await db.execute(select(CollectionItemModel).where(CollectionItemModel.scan_id == scan.id))
    for collection in result.scalars().all():
        collection.status = _collection_status_for_scan(scan, listing)


@app.post(
    "/api/v1/scan/capture-sessions",
    response_model=CaptureSessionResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def create_capture_session(
    payload: CaptureSessionCreateInput,
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    actor_user_id, _actor_user, _subscription = await _check_scan_quota_for_request(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        form_user_id=payload.user_id,
    )
    result = await db.execute(select(CaptureSessionModel).where(CaptureSessionModel.client_uuid == payload.client_uuid))
    existing = result.scalar_one_or_none()
    if existing:
        if existing.user_id != actor_user_id:
            raise AppProductionException("FORBIDDEN", "Session de capture divergente.", 403)
        return CaptureSessionResponse(
            session_id=existing.id,
            client_uuid=existing.client_uuid,
            capture_mode=existing.capture_mode,
            expected_steps=existing.expected_steps,
            required_steps=CAPTURE_REQUIRED_STEPS,
            expires_at=existing.expires_at.isoformat(),
            quality_thresholds=_capture_quality_thresholds_response(),
        )

    now = _now_naive_utc()
    session = CaptureSessionModel(
        id=str(uuid.uuid4()),
        client_uuid=payload.client_uuid,
        user_id=actor_user_id,
        status="active",
        capture_mode="mobile_camera",
        expected_steps=CAPTURE_ALL_STEPS,
        capture_metadata={
            "weight": payload.weight,
            "magnetic": payload.magnetic,
            "latitude": payload.latitude,
            "longitude": payload.longitude,
            "step_paths": {},
        },
        quality_report={},
        image_hashes={},
        contact_guard={},
        expires_at=now + timedelta(minutes=CAPTURE_SESSION_TTL_MINUTES),
        created_at=now,
        updated_at=now,
    )
    db.add(session)
    await db.commit()
    return CaptureSessionResponse(
        session_id=session.id,
        client_uuid=session.client_uuid,
        capture_mode=session.capture_mode,
        expected_steps=session.expected_steps,
        required_steps=CAPTURE_REQUIRED_STEPS,
        expires_at=session.expires_at.isoformat(),
        quality_thresholds=_capture_quality_thresholds_response(),
    )


@app.post(
    "/api/v1/scan/capture-sessions/{session_id}/images",
    response_model=CaptureImageResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def upload_capture_session_image(
    session_id: str,
    step: str = Form(...),
    image: UploadFile = File(...),
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    actor_user_id, _user, _subscription = await _resolve_mobile_identity(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        require_user=True,
    )
    step = (step or "").strip().lower()
    if step not in CAPTURE_ALL_STEPS:
        raise AppProductionException("INVALID_CAPTURE_STEP", "Etape de capture invalide.", 400)

    result = await db.execute(select(CaptureSessionModel).where(CaptureSessionModel.id == session_id))
    session = result.scalar_one_or_none()
    if not session or session.user_id != actor_user_id:
        raise AppProductionException("NOT_FOUND", "Session de capture introuvable.", 404)
    if session.status != "active":
        raise AppProductionException("CONFLICT", "Session de capture deja finalisee.", 409)
    if session.expires_at < _now_naive_utc():
        session.status = "expired"
        await db.commit()
        raise AppProductionException("CAPTURE_SESSION_EXPIRED", "Session de capture expiree.", 409)

    await validate_upload_file(image)
    image_bytes = await image.read()
    try:
        quality = _image_quality_report(image_bytes)
    except Exception:
        raise AppProductionException("INVALID_IMAGE", "Image illisible.", 415)
    if not quality.get("passed"):
        raise AppProductionException("PHOTO_QUALITY_REJECTED", "Photo insuffisante. Reprenez une image plus nette et mieux eclairee.", 400)

    contact_guard = _contact_guard_report(image_bytes)
    if not contact_guard.get("passed"):
        raise AppProductionException("CONTACT_IN_IMAGE_DETECTED", "Un numero de telephone, WhatsApp, email ou lien est visible dans l'image.", 400)

    stored_path = await storage_provider.save_image(
        image_bytes,
        category="interior" if step == "interior" else "exterior",
    )
    image_hash = hashlib.sha256(image_bytes).hexdigest()
    quality_report = dict(session.quality_report or {})
    image_hashes = dict(session.image_hashes or {})
    contact_report = dict(session.contact_guard or {})
    quality_report[step] = quality
    image_hashes[step] = image_hash
    contact_report[step] = contact_guard
    session.quality_report = quality_report
    session.image_hashes = image_hashes
    session.contact_guard = contact_report
    _set_capture_step_path(session, step, stored_path)
    await db.commit()

    return CaptureImageResponse(
        session_id=session.id,
        step=step,
        accepted=True,
        quality=quality,
        contact_guard=contact_guard,
        image_hash=image_hash,
        captured_count=_capture_count(session),
        required_count=len(CAPTURE_REQUIRED_STEPS),
        message="Photo acceptee.",
    )


@app.post(
    "/api/v1/scan/capture-sessions/{session_id}/submit",
    response_model=ScanDecisionResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def submit_capture_session(
    session_id: str,
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    user_agent: Optional[str] = Header(None, alias="User-Agent"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
    accept_language: Optional[str] = Header(None, alias="Accept-Language"),
):
    actor_user_id, actor_user, subscription = await _resolve_mobile_identity(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        require_user=True,
    )
    result = await db.execute(select(CaptureSessionModel).where(CaptureSessionModel.id == session_id))
    session = result.scalar_one_or_none()
    if not session or session.user_id != actor_user_id:
        raise AppProductionException("NOT_FOUND", "Session de capture introuvable.", 404)
    if session.expires_at < _now_naive_utc():
        session.status = "expired"
        await db.commit()
        raise AppProductionException("CAPTURE_SESSION_EXPIRED", "Session de capture expiree.", 409)

    actor_user_id, actor_user, subscription = await _check_scan_quota_for_request(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        form_user_id=session.user_id,
    )

    metadata = session.capture_metadata or {}
    existing_result = await db.execute(select(ScanModel).where(ScanModel.client_uuid == session.client_uuid))
    existing_scan = existing_result.scalar_one_or_none()
    if existing_scan:
        displayed_score = _scan_user_score(existing_scan)
        has_interior_cut = bool(existing_scan.interior_image_path)
        actions = business_orchestrator.build_scan_actions(existing_scan.status_code, has_interior_cut=has_interior_cut)
        return {
            "status_code": existing_scan.status_code,
            "is_meteorite": existing_scan.is_meteorite,
            "meteorite_probability": displayed_score,
            "dominant_class": existing_scan.dominant_class,
            "class_confidence": existing_scan.class_confidence,
            "has_interior_cut": has_interior_cut,
            "actions": actions,
            "trigger_radar_admin": False,
            "metadata_applied": {
                "weight_provided": existing_scan.weight is not None,
                "magnetic_status": existing_scan.magnetic,
                "has_coordinates": existing_scan.latitude is not None and existing_scan.longitude is not None,
            },
            "message": business_orchestrator.build_message(
                status_code=existing_scan.status_code,
                dominant_class=existing_scan.dominant_class,
                meteorite_probability=displayed_score,
                actions=actions,
                language=accept_language,
                has_interior_cut=has_interior_cut,
            ),
            "scan_id": existing_scan.id,
            "is_sync_retry": True,
            "capture_verified": existing_scan.capture_verified,
            "capture_mode": existing_scan.capture_mode,
            "quality_report": existing_scan.quality_report,
            "contact_guard": existing_scan.contact_guard,
        }

    exterior_paths = _session_required_paths(session)
    if len(exterior_paths) < len(CAPTURE_REQUIRED_STEPS):
        raise AppProductionException(
            "MISSING_EXTERNAL_PHOTOS",
            f"Action obligatoire : vous devez fournir {len(CAPTURE_REQUIRED_STEPS)} photos exterieures validees.",
            400,
        )

    if vision_pipeline is None:
        raise AppProductionException("SERVICE_UNAVAILABLE", "Pipeline IA indisponible.", 503)

    list_exterior_bytes = [await _read_stored_file_bytes(path) for path in exterior_paths]
    interior_path = _session_interior_path(session)
    interior_bytes = await _read_stored_file_bytes(interior_path) if interior_path else None

    try:
        vision_results = await anyio.to_thread.run_sync(
            vision_pipeline.process_full_scan,
            list_exterior_bytes,
            interior_bytes,
        )
        fusion_results = fusion_engine.fuse_outputs(
            vision_outputs=vision_results,
            weight=metadata.get("weight"),
            magnetic=metadata.get("magnetic"),
            latitude=metadata.get("latitude"),
            longitude=metadata.get("longitude"),
        )
        final_decision = business_orchestrator.evaluate_decision(
            fusion_results,
            language=accept_language,
            has_interior_cut=bool(interior_path),
        )
        _print_scan_event(
            "CaptureScanResult",
            {
                "client_uuid": session.client_uuid,
                "session_id": session.id,
                "user_id": session.user_id,
                "client_platform": _detect_client_platform(user_agent),
                "status_code": final_decision["status_code"],
                "meteorite_probability": _round_score(final_decision["meteorite_probability"]),
                "dominant_class": final_decision["dominant_class"],
                "class_confidence": _round_score(final_decision["class_confidence"]),
                "vision": _vision_output_summary(vision_results),
            },
        )
    except Exception as e:
        raise AppProductionException("INTERNAL_PROCESSING_ERROR", f"Erreur de traitement IA: {str(e)}", 500)

    scan_id = str(uuid.uuid4())
    new_scan = ScanModel(
        id=scan_id,
        client_uuid=session.client_uuid,
        user_id=session.user_id,
        status_code=final_decision["status_code"],
        is_meteorite=final_decision["is_meteorite"],
        meteorite_probability=final_decision["meteorite_probability"],
        dominant_class=final_decision["dominant_class"],
        class_confidence=final_decision["class_confidence"],
        weight=metadata.get("weight"),
        magnetic=metadata.get("magnetic"),
        latitude=metadata.get("latitude"),
        longitude=metadata.get("longitude"),
        raw_vision_outputs=vision_results,
        exterior_images_paths=exterior_paths,
        interior_image_path=interior_path,
        capture_session_id=session.id,
        capture_mode=session.capture_mode,
        capture_verified=True,
        quality_report=session.quality_report,
        image_hashes=session.image_hashes,
        contact_guard=session.contact_guard,
    )
    session.status = "submitted"
    session.updated_at = _now_naive_utc()
    db.add(new_scan)
    await _create_notification(
        db,
        user_id=session.user_id,
        type_value="scan_ready",
        title="نتيجة المسح جاهزة",
        body=f"{final_decision['dominant_class']} - {final_decision['meteorite_probability'] * 100:.1f}/100",
        action="scanResult",
        metadata={"scan_id": scan_id, "capture_session_id": session.id},
    )
    await db.commit()

    await decrement_quota(subscription.user_id, db)
    refreshed_subscription = await get_or_create_subscription(subscription.user_id, db)
    if (
        not is_unlimited_scan_user(actor_user)
        and refreshed_subscription.tier == "free"
        and refreshed_subscription.remaining_tokens <= 1
    ):
        await _create_notification(
            db,
            user_id=session.user_id,
            type_value="quota_warning",
            title="Quota bientot epuise",
            body=f"Il vous reste {refreshed_subscription.remaining_tokens} scan gratuit.",
            action="premium",
        )
        await db.commit()

    final_decision["scan_id"] = scan_id
    final_decision["capture_verified"] = True
    final_decision["capture_mode"] = session.capture_mode
    final_decision["quality_report"] = session.quality_report
    final_decision["contact_guard"] = session.contact_guard
    await _send_push_to_user(
        db,
        session.user_id,
        {
            "title": "نتيجة المسح جاهزة",
            "body": f"نقاط: {final_decision['meteorite_probability'] * 100:.1f}/100 - {final_decision['dominant_class']}",
            "data": {"action": "scanResult", "scan_id": scan_id},
        },
    )
    return final_decision

@app.post(
    "/api/v1/scan/exterior",
    response_model=ScanDecisionResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def scan_exterior(
    client_uuid: str = Form(...),
    user_id: str = Form(...),
    files_exterior: List[UploadFile] = File(...),
    file_interior: Optional[UploadFile] = File(None),
    weight: Optional[float] = Form(None),
    magnetic: Optional[bool] = Form(None),
    latitude: Optional[float] = Form(None),
    longitude: Optional[float] = Form(None),
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    user_agent: Optional[str] = Header(None, alias="User-Agent"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
    accept_language: Optional[str] = Header(None, alias="Accept-Language"),
):
    actor_user_id, actor_user, subscription = await _check_scan_quota_for_request(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        form_user_id=user_id,
    )
    if not ALLOW_LEGACY_SCAN_UPLOAD:
        raise AppProductionException(
            "LEGACY_UPLOAD_DISABLED",
            "Le scan par upload est desactive en production. Utilisez la capture camera Tissint.",
            403,
        )
    try:
        metadata = ScanMetadataInput(
            client_uuid=client_uuid,
            user_id=actor_user_id,
            weight=weight,
            magnetic=magnetic,
            latitude=latitude,
            longitude=longitude
        )
    except Exception as e:
        raise AppProductionException("VALIDATION_ERROR", str(e), 400)

    # Vérification d'idempotence (Sync hors-ligne / Retry device)
    result = await db.execute(select(ScanModel).where(ScanModel.client_uuid == metadata.client_uuid))
    existing_scan = result.scalar_one_or_none()
    
    if existing_scan:
        if existing_scan.user_id != metadata.user_id:
            raise AppProductionException("FORBIDDEN", "Identifiant utilisateur divergent.", 403)
        print(f"🔄 [Idempotence] Scan existant récupéré pour client_uuid: {metadata.client_uuid}")
        has_interior_cut = bool(existing_scan.interior_image_path)
        displayed_score = _scan_user_score(existing_scan)
        actions = business_orchestrator.build_scan_actions(
            existing_scan.status_code,
            has_interior_cut=has_interior_cut,
        )
        _print_scan_event(
            "ScanResult",
            {
                "client_uuid": metadata.client_uuid,
                "scan_id": existing_scan.id,
                "user_id": metadata.user_id,
                "is_sync_retry": True,
                "client_platform": _detect_client_platform(user_agent),
                "status_code": existing_scan.status_code,
                "meteorite_probability": _round_score(displayed_score),
                "dominant_class": existing_scan.dominant_class,
                "class_confidence": _round_score(existing_scan.class_confidence),
                "metadata": {
                    "weight": existing_scan.weight,
                    "magnetic": existing_scan.magnetic,
                    "latitude_provided": existing_scan.latitude is not None,
                    "longitude_provided": existing_scan.longitude is not None,
                },
            },
        )
        return {
            "status_code": existing_scan.status_code,
            "is_meteorite": existing_scan.is_meteorite,
            "meteorite_probability": displayed_score,
            "dominant_class": existing_scan.dominant_class,
            "class_confidence": existing_scan.class_confidence,
            "has_interior_cut": has_interior_cut,
            "actions": actions,
            "trigger_radar_admin": False,
            "metadata_applied": {
                "weight_provided": existing_scan.weight is not None,
                "magnetic_status": existing_scan.magnetic,
                "has_coordinates": existing_scan.latitude is not None and existing_scan.longitude is not None
            },
            "message": business_orchestrator.build_message(
                status_code=existing_scan.status_code,
                dominant_class=existing_scan.dominant_class,
                meteorite_probability=displayed_score,
                actions=actions,
                language=accept_language,
                has_interior_cut=has_interior_cut,
            ),
            "scan_id": existing_scan.id,
            "is_sync_retry": True
        }

    if len(files_exterior) < 3:
        raise AppProductionException(
            "MISSING_EXTERNAL_PHOTOS", 
            f"Action obligatoire : Vous devez fournir au moins 3 photos extérieures. Reçu : {len(files_exterior)}",
            400
        )
        
    print(f"📸 [OK] {len(files_exterior)} images extérieures reçues.")
    
    # 1. Extraction et Sauvegarde Asynchrone des images extérieures
    list_exterior_bytes = []
    exterior_paths = []
    exterior_file_summaries = []
    for index, f in enumerate(files_exterior):
        await validate_upload_file(f)
        data = await f.read()
        list_exterior_bytes.append(data)
        exterior_file_summaries.append(_upload_file_summary(f, data, index))
        path = await storage_provider.save_image(data, category="exterior")
        exterior_paths.append(path)
        
    # Extraction et Sauvegarde image intérieure si présente
    interior_bytes = None
    interior_path = None
    interior_file_summary = None
    if file_interior:
        print("💎 [Anticipation] Une photo de coupe interne a été fournie dès le départ !")
        await validate_upload_file(file_interior)
        interior_bytes = await file_interior.read()
        interior_file_summary = _upload_file_summary(file_interior, interior_bytes, 0)
        interior_path = await storage_provider.save_image(interior_bytes, category="interior")
    else:
        print("🔍 Analyse basée uniquement sur les caractéristiques extérieures.")

    _print_scan_event(
        "ScanInput",
        {
            "client_uuid": metadata.client_uuid,
            "user_id": metadata.user_id,
            "client_platform": _detect_client_platform(user_agent),
            "user_agent": (user_agent or "")[:180],
            "accept_language": accept_language,
            "exterior_count": len(list_exterior_bytes),
            "exterior_files": exterior_file_summaries,
            "interior_file": interior_file_summary,
            "metadata": {
                "weight": metadata.weight,
                "magnetic": metadata.magnetic,
                "latitude_provided": metadata.latitude is not None,
                "longitude_provided": metadata.longitude is not None,
            },
        },
    )

    if vision_pipeline is None:
        raise AppProductionException("SERVICE_UNAVAILABLE", "Pipeline IA indisponible.", 503)

    try:
        # 2. Pipeline de Vision (Inférence des modèles sur Thread)
        vision_results = await anyio.to_thread.run_sync(
            vision_pipeline.process_full_scan,
            list_exterior_bytes, 
            interior_bytes
        )
        
        # 3. Moteur de Fusion et Métadonnées
        fusion_results = fusion_engine.fuse_outputs(
            vision_outputs=vision_results,
            weight=metadata.weight,
            magnetic=metadata.magnetic,
            latitude=metadata.latitude,
            longitude=metadata.longitude
        )
        
        # 4. Orchestrateur Métier (Décision)
        final_decision = business_orchestrator.evaluate_decision(
            fusion_results,
            language=accept_language,
            has_interior_cut=bool(interior_path),
        )
        _print_scan_event(
            "ScanResult",
            {
                "client_uuid": metadata.client_uuid,
                "user_id": metadata.user_id,
                "is_sync_retry": False,
                "client_platform": _detect_client_platform(user_agent),
                "status_code": final_decision["status_code"],
                "meteorite_probability": _round_score(final_decision["meteorite_probability"]),
                "dominant_class": final_decision["dominant_class"],
                "class_confidence": _round_score(final_decision["class_confidence"]),
                "vision": _vision_output_summary(vision_results),
                "metadata_applied": fusion_results.get("metadata_applied"),
            },
        )

    except Exception as e:
        raise AppProductionException("INTERNAL_PROCESSING_ERROR", f"Erreur de traitement IA: {str(e)}", 500)

    # 5. Persistance Asynchrone en BDD
    scan_id = str(uuid.uuid4())
    new_scan = ScanModel(
        id=scan_id,
        client_uuid=metadata.client_uuid,
        user_id=metadata.user_id,
        status_code=final_decision["status_code"],
        is_meteorite=final_decision["is_meteorite"],
        meteorite_probability=final_decision["meteorite_probability"],
        dominant_class=final_decision["dominant_class"],
        class_confidence=final_decision["class_confidence"],
        weight=metadata.weight,
        magnetic=metadata.magnetic,
        latitude=metadata.latitude,
        longitude=metadata.longitude,
        raw_vision_outputs=vision_results,
        exterior_images_paths=exterior_paths,
        interior_image_path=interior_path,
        capture_mode="legacy_upload",
        capture_verified=False,
        quality_report=None,
        image_hashes={
            "exterior": [summary.get("sha256_16") for summary in exterior_file_summaries],
            "interior": interior_file_summary.get("sha256_16") if interior_file_summary else None,
        },
        contact_guard={"status": "unchecked", "passed": True},
    )
    
    db.add(new_scan)
    await _create_notification(
        db,
        user_id=metadata.user_id,
        type_value="scan_ready",
        title="نتيجة المسح جاهزة",
        body=f"{final_decision['dominant_class']} - {final_decision['meteorite_probability'] * 100:.1f}/100",
        action="scanResult",
        metadata={"scan_id": scan_id},
    )
    await db.commit()

    # Déduction du quota pour le scan d'IA (uniquement flux nominal, pas en cas d'idempotence)
    await decrement_quota(subscription.user_id, db)
    refreshed_subscription = await get_or_create_subscription(subscription.user_id, db)
    if (
        not is_unlimited_scan_user(actor_user)
        and refreshed_subscription.tier == "free"
        and refreshed_subscription.remaining_tokens <= 1
    ):
        await _create_notification(
            db,
            user_id=metadata.user_id,
            type_value="quota_warning",
            title="Quota bientôt épuisé",
            body=f"Il vous reste {refreshed_subscription.remaining_tokens} scan gratuit.",
            action="premium",
        )
        await db.commit()

    # Ajout du scan_id et des infos à la réponse
    final_decision["scan_id"] = scan_id
    await _send_push_to_user(
        db,
        metadata.user_id,
        {
            "title": "نتيجة المسح جاهزة",
            "body": f"نقاط: {final_decision['meteorite_probability'] * 100:.1f}/100 - {final_decision['dominant_class']}",
            "data": {"action": "scanResult", "scan_id": scan_id},
        },
    )

    return final_decision

@app.patch(
    "/api/v1/scan/{scan_id}/interior",
    response_model=ScanDecisionResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def scan_interior_update(
    scan_id: str,
    file_interior: UploadFile = File(...),
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
    accept_language: Optional[str] = Header(None, alias="Accept-Language"),
):
    # 1. Récupération asynchrone du scan de la BDD
    actor_user_id, _user, _subscription = await _resolve_mobile_identity(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        require_user=True,
    )
    result = await db.execute(select(ScanModel).where(ScanModel.id == scan_id))
    scan = result.scalar_one_or_none()
    
    if not scan:
        raise AppProductionException("NOT_FOUND", "Scan introuvable.", 404)

    if scan.user_id != actor_user_id:
        raise AppProductionException("NOT_FOUND", "Scan introuvable.", 404)
        
    if scan.interior_image_path:
        raise AppProductionException("CONFLICT", "Une image de coupe existe déjà pour ce scan.", 409)

    # 2. Lecture et Sauvegarde de la nouvelle image de coupe asynchrone
    await validate_upload_file(file_interior)
    interior_bytes = await file_interior.read()
    interior_path = await storage_provider.save_image(interior_bytes, category="interior")

    if vision_pipeline is None:
        raise AppProductionException("SERVICE_UNAVAILABLE", "Pipeline IA indisponible.", 503)

    try:
        # 3. Inférence vision sur la coupe intérieure
        vision_results = dict(scan.raw_vision_outputs or {})
        
        # Inférence asynchrone d'une seule image via le pipeline (Thread offloading)
        new_interior_vision = await anyio.to_thread.run_sync(
            vision_pipeline.predict_image_parallel, 
            interior_bytes
        )
        vision_results["interior"] = new_interior_vision

    except Exception as e:
        raise AppProductionException("INTERNAL_PROCESSING_ERROR", f"Erreur de traitement IA: {str(e)}", 500)

    # 4. Refusion complète avec la nouvelle coupe et les métadonnées existantes
    fusion_results = fusion_engine.fuse_outputs(
        vision_outputs=vision_results,
        weight=scan.weight,
        magnetic=scan.magnetic
    )

    final_decision = business_orchestrator.evaluate_decision(
        fusion_results,
        language=accept_language,
        has_interior_cut=True,
    )

    # 5. Mise à jour du document en BDD
    # On force manuellement le json pour la mise a jour
    scan.raw_vision_outputs = vision_results
    scan.interior_image_path = interior_path
    scan.status_code = final_decision["status_code"]
    scan.is_meteorite = final_decision["is_meteorite"]
    scan.meteorite_probability = final_decision["meteorite_probability"]
    scan.dominant_class = final_decision["dominant_class"]
    scan.class_confidence = final_decision["class_confidence"]

    await _create_notification(
        db,
        user_id=scan.user_id,
        type_value="scan_ready",
        title="نتيجة المسح جاهزة",
        body=f"{final_decision['dominant_class']} - {final_decision['meteorite_probability'] * 100:.1f}/100",
        action="scanResult",
        metadata={"scan_id": scan_id},
    )
    await db.commit()
    
    final_decision["scan_id"] = scan_id
    await _send_push_to_user(
        db,
        scan.user_id,
        {
            "title": "نتيجة المسح جاهزة",
            "body": f"نقاط: {final_decision['meteorite_probability'] * 100:.1f}/100 - {final_decision['dominant_class']}",
            "data": {"action": "scanResult", "scan_id": scan_id},
        },
    )
    
    return final_decision

@app.get(
    "/api/v1/collection",
    response_model=List[CollectionItemResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def list_collection(
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user_id, _user, _subscription = await _resolve_mobile_identity(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
    )
    query = (
        select(CollectionItemModel, ScanModel, ListingModel)
        .join(ScanModel, CollectionItemModel.scan_id == ScanModel.id)
        .outerjoin(ListingModel, ListingModel.scan_id == ScanModel.id)
        .where(CollectionItemModel.user_id == user_id)
        .order_by(CollectionItemModel.created_at.desc())
    )
    result = await db.execute(query)
    rows = result.all()

    items = [_collection_item_response(collection, scan, listing) for collection, scan, listing in rows]
    await db.commit()
    return items


@app.post(
    "/api/v1/collection/{scan_id}",
    response_model=CollectionItemResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def add_scan_to_collection(
    scan_id: str,
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user_id, _user, _subscription = await _resolve_mobile_identity(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        require_user=True,
    )
    result = await db.execute(select(ScanModel).where(ScanModel.id == scan_id))
    scan = result.scalar_one_or_none()

    if not scan:
        raise AppProductionException("NOT_FOUND", "Scan introuvable.", 404)

    owner_id = user_id
    if scan.user_id != owner_id:
        raise AppProductionException("NOT_FOUND", "Scan introuvable.", 404)

    if scan.status_code not in {"DIAGNOSTIC_SUCCESS_HIGH", "DIAGNOSTIC_HESITANT"}:
        raise AppProductionException("CONFLICT", "Ce scan n'est pas eligible a la collection.", 409)

    existing_result = await db.execute(
        select(CollectionItemModel).where(
            CollectionItemModel.user_id == owner_id,
            CollectionItemModel.scan_id == scan_id,
        )
    )
    collection = existing_result.scalar_one_or_none()

    if not collection:
        collection = CollectionItemModel(
            id=str(uuid.uuid4()),
            user_id=owner_id,
            scan_id=scan_id,
            status=_collection_status_for_scan(scan),
        )
        db.add(collection)
        await db.commit()
        await db.refresh(collection)

    listing_result = await db.execute(select(ListingModel).where(ListingModel.scan_id == scan_id))
    listing = listing_result.scalar_one_or_none()
    item = _collection_item_response(collection, scan, listing)
    await db.commit()
    return item


@app.get(
    "/api/v1/collection/{scan_id}",
    response_model=CollectionItemResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_collection_item(
    scan_id: str,
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user_id, _user, _subscription = await _resolve_mobile_identity(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        require_user=True,
    )
    query = (
        select(CollectionItemModel, ScanModel, ListingModel)
        .join(ScanModel, CollectionItemModel.scan_id == ScanModel.id)
        .outerjoin(ListingModel, ListingModel.scan_id == ScanModel.id)
        .where(CollectionItemModel.user_id == user_id, CollectionItemModel.scan_id == scan_id)
    )
    result = await db.execute(query)
    row = result.first()

    if not row:
        raise AppProductionException("NOT_FOUND", "Pierre introuvable dans la collection.", 404)

    collection, scan, listing = row
    item = _collection_item_response(collection, scan, listing)
    await db.commit()
    return item

@app.delete(
    "/api/v1/collection/{scan_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def delete_collection_item(
    scan_id: str,
    authorization: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user_id, _user, _subscription = await _resolve_mobile_identity(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        require_user=True,
    )
    result = await db.execute(
        select(CollectionItemModel).where(
            CollectionItemModel.user_id == user_id,
            CollectionItemModel.scan_id == scan_id,
        )
    )
    collection = result.scalar_one_or_none()
    if collection:
        await db.delete(collection)
        await db.commit()
    return OkResponse(ok=True)

@app.post(
    "/api/v1/marketplace/publish/{scan_id}",
    response_model=MarketplaceListingResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def publish_to_marketplace(
    scan_id: str,
    background_tasks: BackgroundTasks,
    payload: Optional[PublishListingInput] = Body(None),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Route de mise en vente sur le Marketplace ou validation finale.
    Sécurise la vie privée via floutage des coordonnées.
    """
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    payload = payload or PublishListingInput()

    result = await db.execute(select(ScanModel).where(ScanModel.id == scan_id))
    scan = result.scalar_one_or_none()

    if not scan:
        raise AppProductionException("NOT_FOUND", "Scan introuvable.", 404)
    if scan.user_id != user.id:
        raise AppProductionException("NOT_FOUND", "Scan introuvable.", 404)
    if scan.status_code != "DIAGNOSTIC_SUCCESS_HIGH":
        raise AppProductionException("CONFLICT", "Ce scan n'est pas eligible au marketplace.", 409)

    title = _clean_optional_text(payload.title)
    description = _clean_optional_text(payload.description)
    region = _clean_optional_text(payload.region)
    if _contains_contact_leak(title) or _contains_contact_leak(description):
        raise AppProductionException(
            "CONTACT_LEAK_DETECTED",
            "La description contient des coordonnees directes.",
            400,
        )
    final_weight = payload.weight_g if payload.weight_g is not None else scan.weight
    if (
        title is None
        or description is None
        or region is None
        or payload.price is None
        or payload.price <= 0
        or final_weight is None
        or final_weight <= 0
    ):
        raise AppProductionException(
            "VALIDATION_ERROR",
            "Titre, description, region, prix et poids sont obligatoires pour publier.",
            400,
        )
    if payload.weight_g is not None:
        scan.weight = payload.weight_g

    # Extraction des valeurs de notre BDD
    dominant_class = scan.dominant_class
    confidence = scan.class_confidence
    user_id = scan.user_id

    # Le Déclencheur Strict pour le bot Telegram
    is_rare = _is_rare_candidate(dominant_class, confidence)
    target_status = _marketplace_status_for_publish(scan, is_rare)

    listing_price = payload.price
    listing_result = await db.execute(select(ListingModel).where(ListingModel.scan_id == scan_id))
    listing = listing_result.scalar_one_or_none()
    previous_status = _legacy_listing_status(listing.status) if listing else None

    if listing:
        if previous_status in MARKETPLACE_LOCKED_FOR_SELLER_STATUSES:
            raise AppProductionException("CONFLICT", "Cette annonce ne peut plus etre modifiee.", 409)
        listing.status = target_status
        listing.price = listing_price
        listing.title = title
        listing.description = description
        listing.region = region
        listing.price_mode = payload.price_mode
    else:
        listing = ListingModel(
            id=str(uuid.uuid4()),
            scan_id=scan_id,
            price=listing_price,
            status=target_status,
            title=title,
            description=description,
            price_mode=payload.price_mode,
            region=region,
        )
        db.add(listing)

    if is_rare and previous_status != target_status:
        background_tasks.add_task(
            send_telegram_radar_alert,
            scan_id=scan_id,
            stone_class=dominant_class,
            confidence=confidence,
            user_id=user_id
        )

    collection_result = await db.execute(
        select(CollectionItemModel).where(
            CollectionItemModel.user_id == user.id,
            CollectionItemModel.scan_id == scan_id,
        )
    )
    collection = collection_result.scalar_one_or_none()
    if collection:
        collection.status = _collection_status_for_scan(scan, listing)

    await db.commit()
    await db.refresh(listing)

    # Security: Anonymisation & Floutage (Arrondi d'une décimale pour une précision régionale protectrice d'environ ~11km)
    safe_lat, safe_lon = _blur_coordinates(scan)
    contact_locked_until = _listing_hold_until(listing)
    image_fields = _listing_image_fields(scan)
    has_interior_cut = _scan_has_interior_cut(scan)
    score = _scan_user_score(scan)

    return MarketplaceListingResponse(
        status=listing.status,
        message="Requête de mise en vente traitée. Données géospatiales anonymisées.",
        listing_id=listing.id,
        scan_id=scan_id,
        is_rare_candidate=is_rare,
        dominant_class=dominant_class,
        confidence=confidence,
        class_confidence=confidence,
        meteorite_probability=score,
        fusion_score=score,
        has_interior_cut=has_interior_cut,
        price=listing.price,
        price_mode=_listing_price_mode(listing),
        title=listing.title,
        description=listing.description,
        region=listing.region,
        weight=scan.weight,
        weight_g=scan.weight,
        magnetic=scan.magnetic,
        blurred_latitude=safe_lat,
        blurred_longitude=safe_lon,
        contact_locked_until=contact_locked_until.isoformat() if contact_locked_until else None,
        **image_fields,
    )


@app.get(
    "/api/v1/marketplace/listings",
    response_model=List[PublicListingItem],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_marketplace_listings(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Récupère toutes les annonces disponibles sur le Marketplace.
    Les coordonnées géospatiales sont anonymisées à 1 décimale (~ 11km).
    """
    _viewer_user, viewer_subscription = await _optional_auth_context(authorization, db)
    viewer_role = viewer_subscription.tier if viewer_subscription else "guest"

    # Requires an inner join with the scans table to retrieve dominant classes, weight, lat/long etc
    query = (
        select(ListingModel, ScanModel, UserModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .outerjoin(UserModel, UserModel.id == ScanModel.user_id)
        .where(
            ListingModel.status.in_(
                list(MARKETPLACE_VISIBLE_STATUSES | {"available", "reserved"})
            )
        )
        .order_by(*_marketplace_priority_ordering())
    )
    result = await db.execute(query)
    rows = result.all()

    listings = [
        _public_listing_item(listing, scan, seller, viewer_role)
        for listing, scan, seller in rows
        if _effective_listing_status(listing) in MARKETPLACE_VISIBLE_STATUSES
    ]
        
    return listings


@app.get(
    "/api/v1/marketplace/listings/{listing_id}",
    response_model=PublicListingItem,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_marketplace_listing_detail(
    listing_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    _viewer_user, viewer_subscription = await _optional_auth_context(authorization, db)
    viewer_role = viewer_subscription.tier if viewer_subscription else "guest"
    query = (
        select(ListingModel, ScanModel, UserModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .outerjoin(UserModel, UserModel.id == ScanModel.user_id)
        .where(ListingModel.id == listing_id)
    )
    result = await db.execute(query)
    row = result.first()

    if not row:
        raise AppProductionException("NOT_FOUND", "Annonce introuvable.", 404)

    listing, scan, seller = row
    if _effective_listing_status(listing) not in MARKETPLACE_VISIBLE_STATUSES and viewer_role != "admin":
        raise AppProductionException("NOT_FOUND", "Annonce introuvable.", 404)
    return _public_listing_item(listing, scan, seller, viewer_role)


async def _marketplace_listing_row(
    listing_id: str,
    db: AsyncSession,
):
    query = (
        select(ListingModel, ScanModel, UserModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .outerjoin(UserModel, UserModel.id == ScanModel.user_id)
        .where(ListingModel.id == listing_id)
    )
    result = await db.execute(query)
    row = result.first()
    if not row:
        raise AppProductionException("NOT_FOUND", "Annonce introuvable.", 404)
    return row


def _is_admin_actor(user: UserModel, subscription) -> bool:
    return user.role == "admin" or subscription.tier == "admin"


def _ensure_listing_actor(user: UserModel, subscription, scan: ScanModel) -> bool:
    is_admin = _is_admin_actor(user, subscription)
    if not is_admin and scan.user_id != user.id:
        raise AppProductionException("FORBIDDEN", "Action non autorisee sur cette annonce.", 403)
    return is_admin


def _ensure_listing_can_mutate(listing: ListingModel, is_admin: bool, action: str) -> str:
    current_status = _effective_listing_status(listing)
    if current_status == "removed":
        raise AppProductionException("CONFLICT", "Cette annonce est deja retiree.", 409)
    if current_status in {"archived", "rejected"}:
        raise AppProductionException("CONFLICT", "Cette annonce ne peut plus etre modifiee.", 409)
    if current_status == "sold" and action != "sold":
        raise AppProductionException("CONFLICT", "Cette annonce est deja vendue.", 409)
    if current_status == "admin_reserved" and not is_admin:
        raise AppProductionException("CONFLICT", "Cette annonce est reservee par l'administration.", 409)
    return current_status


@app.patch(
    "/api/v1/marketplace/listings/{listing_id}",
    response_model=PublicListingItem,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def update_marketplace_listing(
    listing_id: str,
    payload: UpdateListingInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    listing, scan, seller = await _marketplace_listing_row(listing_id, db)
    is_admin = _ensure_listing_actor(user, subscription, scan)
    previous_status = _ensure_listing_can_mutate(listing, is_admin, "update")
    fields_set = getattr(payload, "model_fields_set", None)
    if fields_set is None:
        fields_set = set(payload.dict(exclude_unset=True).keys())

    title = _clean_optional_text(payload.title) if "title" in fields_set else listing.title
    description = _clean_optional_text(payload.description) if "description" in fields_set else listing.description
    if _contains_contact_leak(title) or _contains_contact_leak(description):
        raise AppProductionException(
            "CONTACT_LEAK_DETECTED",
            "La description contient des coordonnees directes.",
            400,
        )

    if "price" in fields_set and payload.price is not None:
        listing.price = payload.price
    if "title" in fields_set:
        listing.title = title
    if "description" in fields_set:
        listing.description = description
    if "price_mode" in fields_set and payload.price_mode is not None:
        listing.price_mode = payload.price_mode
    if "region" in fields_set:
        listing.region = _clean_optional_text(payload.region)
    if "weight_g" in fields_set and payload.weight_g is not None:
        scan.weight = payload.weight_g

    await _sync_collection_status_for_listing(db, scan, listing)
    await _write_audit_log(
        db,
        actor_user_id=user.id,
        action="marketplace_update_listing",
        entity_type="listing",
        entity_id=listing.id,
        metadata={"scan_id": scan.id, "previous_status": previous_status},
    )
    await db.commit()
    await db.refresh(listing)
    await db.refresh(scan)
    return _public_listing_item(listing, scan, seller, subscription.tier)


@app.post(
    "/api/v1/marketplace/listings/{listing_id}/sold",
    response_model=PublicListingItem,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def mark_marketplace_listing_sold(
    listing_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    listing, scan, seller = await _marketplace_listing_row(listing_id, db)
    is_admin = _ensure_listing_actor(user, subscription, scan)
    previous_status = _ensure_listing_can_mutate(listing, is_admin, "sold")

    listing.status = "sold"
    await _sync_collection_status_for_listing(db, scan, listing)
    await _write_audit_log(
        db,
        actor_user_id=user.id,
        action="marketplace_mark_listing_sold",
        entity_type="listing",
        entity_id=listing.id,
        metadata={"scan_id": scan.id, "previous_status": previous_status, "new_status": "sold"},
    )
    await db.commit()
    await db.refresh(listing)
    return _public_listing_item(listing, scan, seller, subscription.tier)


@app.delete(
    "/api/v1/marketplace/listings/{listing_id}",
    response_model=PublicListingItem,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def remove_marketplace_listing(
    listing_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    listing, scan, seller = await _marketplace_listing_row(listing_id, db)
    is_admin = _ensure_listing_actor(user, subscription, scan)
    previous_status = _effective_listing_status(listing)
    if previous_status != "removed":
        _ensure_listing_can_mutate(listing, is_admin, "remove")
        listing.status = "removed"
        await _sync_collection_status_for_listing(db, scan, listing)
        await _write_audit_log(
            db,
            actor_user_id=user.id,
            action="marketplace_remove_listing",
            entity_type="listing",
            entity_id=listing.id,
            metadata={"scan_id": scan.id, "previous_status": previous_status, "new_status": "removed"},
        )
        await db.commit()
        await db.refresh(listing)
    return _public_listing_item(listing, scan, seller, subscription.tier)


@app.get(
    "/api/v1/admin/radar",
    response_model=List[AdminRadarListingResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def list_admin_radar(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_admin_context(authorization, db)
    query = (
        select(ListingModel, ScanModel, UserModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .outerjoin(UserModel, UserModel.id == ScanModel.user_id)
        .where(
            ListingModel.status.in_(
                [
                    "institutional_hold_24h",
                    "admin_reserved",
                    "published",
                    "rejected",
                ]
            )
        )
        .order_by(ListingModel.created_at.desc())
    )
    result = await db.execute(query)
    rows = result.all()
    return [
        _admin_radar_listing_response(listing, scan, seller)
        for listing, scan, seller in rows
        if _is_rare_candidate(scan.dominant_class, scan.class_confidence)
    ]


async def _admin_listing_row(
    listing_id: str,
    db: AsyncSession,
):
    query = (
        select(ListingModel, ScanModel, UserModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .outerjoin(UserModel, UserModel.id == ScanModel.user_id)
        .where(ListingModel.id == listing_id)
    )
    result = await db.execute(query)
    row = result.first()
    if not row:
        raise AppProductionException("NOT_FOUND", "Annonce introuvable.", 404)
    listing, scan, seller = row
    if not _is_rare_candidate(scan.dominant_class, scan.class_confidence):
        raise AppProductionException("CONFLICT", "Cette annonce n'est pas une alerte radar.", 409)
    return listing, scan, seller


async def _apply_admin_listing_action(
    listing_id: str,
    target_status: str,
    action: str,
    success_message: str,
    payload: Optional[AdminListingActionInput],
    authorization: Optional[str],
    db: AsyncSession,
) -> AdminActionResponse:
    admin_user, _subscription = await _require_admin_context(authorization, db)
    listing, scan, seller = await _admin_listing_row(listing_id, db)
    previous_status = _effective_listing_status(listing)

    if previous_status in {"sold", "archived"}:
        raise AppProductionException("CONFLICT", "Cette annonce ne peut plus etre modifiee.", 409)
    if action == "admin_reject_listing" and previous_status == "rejected":
        raise AppProductionException("CONFLICT", "Cette annonce est deja rejetee.", 409)

    listing.status = target_status
    await _sync_collection_status_for_listing(db, scan, listing)
    await _write_audit_log(
        db,
        actor_user_id=admin_user.id,
        action=action,
        entity_type="listing",
        entity_id=listing.id,
        metadata={
            "scan_id": scan.id,
            "previous_status": previous_status,
            "new_status": target_status,
            "reason": payload.reason if payload else None,
        },
    )
    await db.commit()
    await db.refresh(listing)

    return AdminActionResponse(
        status=listing.status,
        message=success_message,
        listing=_admin_radar_listing_response(listing, scan, seller),
    )


@app.post(
    "/api/v1/admin/radar/{listing_id}/reserve",
    response_model=AdminActionResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def reserve_admin_radar_listing(
    listing_id: str,
    payload: Optional[AdminListingActionInput] = Body(None),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    return await _apply_admin_listing_action(
        listing_id=listing_id,
        target_status="admin_reserved",
        action="admin_reserve_listing",
        success_message="Annonce reservee pour revue admin.",
        payload=payload,
        authorization=authorization,
        db=db,
    )


@app.post(
    "/api/v1/admin/radar/{listing_id}/release",
    response_model=AdminActionResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def release_admin_radar_listing(
    listing_id: str,
    payload: Optional[AdminListingActionInput] = Body(None),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    return await _apply_admin_listing_action(
        listing_id=listing_id,
        target_status="published",
        action="admin_release_listing",
        success_message="Annonce publiee depuis le radar admin.",
        payload=payload,
        authorization=authorization,
        db=db,
    )


@app.post(
    "/api/v1/admin/radar/{listing_id}/reject",
    response_model=AdminActionResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def reject_admin_radar_listing(
    listing_id: str,
    payload: Optional[AdminListingActionInput] = Body(None),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    return await _apply_admin_listing_action(
        listing_id=listing_id,
        target_status="rejected",
        action="admin_reject_listing",
        success_message="Annonce rejetee par le radar admin.",
        payload=payload,
        authorization=authorization,
        db=db,
    )


@app.get(
    "/api/v1/admin/audit",
    response_model=List[AuditLogResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def list_admin_audit_logs(
    authorization: Optional[str] = Header(None),
    limit: int = Query(50, ge=1, le=200),
    entity_type: Optional[str] = Query(None, max_length=80),
    entity_id: Optional[str] = Query(None, max_length=120),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_admin_context(authorization, db)
    query = select(AuditLogModel)
    if entity_type:
        query = query.where(AuditLogModel.entity_type == entity_type)
    if entity_id:
        query = query.where(AuditLogModel.entity_id == entity_id)
    query = query.order_by(AuditLogModel.created_at.desc()).limit(limit)
    result = await db.execute(query)
    return [_audit_log_response(log) for log in result.scalars().all()]


@app.post(
    "/api/v1/messages",
    response_model=UiMessageResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def create_ui_message(
    payload: SendMessageInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    text_content = payload.text.strip()
    if not text_content:
        raise AppProductionException("VALIDATION_ERROR", "Message vide.", 400)

    thread = None
    listing = None
    scan = None
    receiver_id = None

    if payload.thread_id:
        thread_result = await db.execute(select(MessageThreadModel).where(MessageThreadModel.id == payload.thread_id))
        thread = thread_result.scalar_one_or_none()
        if not thread or user.id not in {thread.buyer_id, thread.seller_id}:
            raise AppProductionException("NOT_FOUND", "Conversation introuvable.", 404)
        receiver_id = thread.seller_id if user.id == thread.buyer_id else thread.buyer_id
        listing_result = await db.execute(
            select(ListingModel, ScanModel)
            .join(ScanModel, ListingModel.scan_id == ScanModel.id)
            .where(ListingModel.id == thread.listing_id)
        )
        listing_row = listing_result.first()
        if not listing_row:
            raise AppProductionException("NOT_FOUND", "Annonce introuvable.", 404)
        listing, scan = listing_row
    elif payload.listing_id:
        listing_result = await db.execute(
            select(ListingModel, ScanModel)
            .join(ScanModel, ListingModel.scan_id == ScanModel.id)
            .where(ListingModel.id == payload.listing_id)
        )
        listing_row = listing_result.first()
        if not listing_row:
            raise AppProductionException("NOT_FOUND", "Annonce introuvable.", 404)
        listing, scan = listing_row
        if scan.user_id == user.id:
            raise AppProductionException("CONFLICT", "Le vendeur ne peut pas demarrer une conversation acheteur.", 409)
        receiver_id = scan.user_id
        thread_result = await db.execute(
            select(MessageThreadModel).where(
                MessageThreadModel.listing_id == listing.id,
                MessageThreadModel.buyer_id == user.id,
                MessageThreadModel.seller_id == scan.user_id,
            )
        )
        thread = thread_result.scalar_one_or_none()
        if not thread:
            thread = MessageThreadModel(
                id=str(uuid.uuid4()),
                listing_id=listing.id,
                buyer_id=user.id,
                seller_id=scan.user_id,
                unread_for_buyer=0,
                unread_for_seller=0,
            )
            db.add(thread)
    else:
        raise AppProductionException("VALIDATION_ERROR", "listing_id ou thread_id est requis.", 400)

    now = _utc_now()
    thread.updated_at = now
    if receiver_id == thread.buyer_id:
        thread.unread_for_buyer += 1
    else:
        thread.unread_for_seller += 1

    message = MessageModel(
        id=str(uuid.uuid4()),
        conversation_id=thread.id,
        sender_id=user.id,
        receiver_id=receiver_id,
        text_content=text_content,
        timestamp=now,
    )
    db.add(message)
    await _create_notification(
        db,
        user_id=receiver_id,
        type_value="message",
        title="Nouveau message",
        body=text_content[:180],
        action="messages",
        metadata={"thread_id": thread.id, "listing_id": listing.id if listing else thread.listing_id},
    )
    await db.commit()
    await db.refresh(message)
    await _send_push_to_user(
        db,
        receiver_id,
        {"title": "Nouveau message", "body": text_content[:180], "data": {"action": "messages", "thread_id": thread.id}},
    )

    return UiMessageResponse(
        id=message.id,
        thread_id=thread.id,
        from_me=True,
        text=message.text_content,
        created_at=message.timestamp.isoformat() if message.timestamp else "",
    )


@app.get(
    "/api/v1/messages",
    response_model=List[MessageThreadResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def list_ui_message_threads(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    result = await db.execute(
        select(MessageThreadModel)
        .where(or_(MessageThreadModel.buyer_id == user.id, MessageThreadModel.seller_id == user.id))
        .order_by(MessageThreadModel.updated_at.desc())
    )
    threads = result.scalars().all()

    responses = []
    for thread in threads:
        listing_result = await db.execute(
            select(ListingModel, ScanModel)
            .join(ScanModel, ListingModel.scan_id == ScanModel.id)
            .where(ListingModel.id == thread.listing_id)
        )
        listing_row = listing_result.first()
        if not listing_row:
            continue
        listing, scan = listing_row
        peer_id = thread.seller_id if user.id == thread.buyer_id else thread.buyer_id
        peer_result = await db.execute(select(UserModel).where(UserModel.id == peer_id))
        peer = peer_result.scalar_one_or_none()
        last_result = await db.execute(
            select(MessageModel)
            .where(MessageModel.conversation_id == thread.id)
            .order_by(MessageModel.timestamp.desc())
            .limit(1)
        )
        last_message = last_result.scalar_one_or_none()
        unread = thread.unread_for_buyer if user.id == thread.buyer_id else thread.unread_for_seller
        responses.append(
            MessageThreadResponse(
                id=thread.id,
                listing_id=thread.listing_id,
                listing_title=listing.title or scan.dominant_class,
                listing_image_uri=_scan_main_image_uri(scan),
                peer_name=_seller_full_name(peer) or _seller_masked_name(peer),
                peer_verified=peer is not None,
                last_message=last_message.text_content if last_message else None,
                last_at=last_message.timestamp.isoformat() if last_message and last_message.timestamp else None,
                unread=unread,
            )
        )
    return responses


@app.get(
    "/api/v1/messages/{thread_id}",
    response_model=List[UiMessageResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_ui_message_thread(
    thread_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    thread_result = await db.execute(select(MessageThreadModel).where(MessageThreadModel.id == thread_id))
    thread = thread_result.scalar_one_or_none()
    if not thread or user.id not in {thread.buyer_id, thread.seller_id}:
        raise AppProductionException("NOT_FOUND", "Conversation introuvable.", 404)

    if user.id == thread.buyer_id:
        thread.unread_for_buyer = 0
    else:
        thread.unread_for_seller = 0

    result = await db.execute(
        select(MessageModel)
        .where(MessageModel.conversation_id == thread.id)
        .order_by(MessageModel.timestamp.asc())
    )
    messages = result.scalars().all()
    await db.commit()
    return [
        UiMessageResponse(
            id=message.id,
            thread_id=thread.id,
            from_me=message.sender_id == user.id,
            text=message.text_content,
            created_at=message.timestamp.isoformat() if message.timestamp else "",
        )
        for message in messages
    ]


@app.get(
    "/api/v1/favorites",
    response_model=List[PublicListingItem],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def list_favorites(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    viewer_role = subscription.tier
    result = await db.execute(
        select(FavoriteModel, ListingModel, ScanModel, UserModel)
        .join(ListingModel, FavoriteModel.listing_id == ListingModel.id)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .outerjoin(UserModel, UserModel.id == ScanModel.user_id)
        .where(FavoriteModel.user_id == user.id)
        .order_by(FavoriteModel.created_at.desc())
    )
    return [
        _public_listing_item(listing, scan, seller, viewer_role)
        for _favorite, listing, scan, seller in result.all()
        if _effective_listing_status(listing) in MARKETPLACE_VISIBLE_STATUSES
    ]


@app.post(
    "/api/v1/favorites/{listing_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def add_favorite(
    listing_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    listing_result = await db.execute(select(ListingModel).where(ListingModel.id == listing_id))
    if not listing_result.scalar_one_or_none():
        raise AppProductionException("NOT_FOUND", "Annonce introuvable.", 404)
    favorite_result = await db.execute(
        select(FavoriteModel).where(FavoriteModel.user_id == user.id, FavoriteModel.listing_id == listing_id)
    )
    if not favorite_result.scalar_one_or_none():
        db.add(FavoriteModel(id=str(uuid.uuid4()), user_id=user.id, listing_id=listing_id))
        await db.commit()
    return OkResponse(ok=True)


@app.delete(
    "/api/v1/favorites/{listing_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def delete_favorite(
    listing_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    favorite_result = await db.execute(
        select(FavoriteModel).where(FavoriteModel.user_id == user.id, FavoriteModel.listing_id == listing_id)
    )
    favorite = favorite_result.scalar_one_or_none()
    if favorite:
        await db.delete(favorite)
        await db.commit()
    return OkResponse(ok=True)


@app.get(
    "/api/v1/notifications",
    response_model=List[NotificationResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def list_notifications(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    result = await db.execute(
        select(NotificationModel)
        .where(NotificationModel.user_id == user.id)
        .order_by(NotificationModel.created_at.desc())
        .limit(100)
    )
    return [
        NotificationResponse(
            id=notification.id,
            type=notification.type,
            title=notification.title,
            body=notification.body,
            read=notification.read,
            created_at=notification.created_at.isoformat() if notification.created_at else "",
            action=notification.action,
        )
        for notification in result.scalars().all()
    ]


@app.patch(
    "/api/v1/notifications/{notification_id}/read",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def mark_notification_read(
    notification_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    result = await db.execute(
        select(NotificationModel).where(NotificationModel.id == notification_id, NotificationModel.user_id == user.id)
    )
    notification = result.scalar_one_or_none()
    if not notification:
        raise AppProductionException("NOT_FOUND", "Notification introuvable.", 404)
    notification.read = True
    await db.commit()
    return OkResponse(ok=True)


@app.post(
    "/api/v1/notifications/read-all",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def mark_all_notifications_read(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    result = await db.execute(select(NotificationModel).where(NotificationModel.user_id == user.id))
    for notification in result.scalars().all():
        notification.read = True
    await db.commit()
    return OkResponse(ok=True)


@app.post(
    "/api/v1/notifications/push-subscribe",
    response_model=PushSubscribeResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def push_subscribe(
    payload: PushSubscribeInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    result = await db.execute(select(PushSubscriptionModel).where(PushSubscriptionModel.endpoint == payload.endpoint))
    subscription = result.scalar_one_or_none()
    if subscription:
        subscription.user_id = user.id
        subscription.keys = payload.keys.model_dump()
        subscription.updated_at = _utc_now()
    else:
        db.add(
            PushSubscriptionModel(
                id=str(uuid.uuid4()),
                user_id=user.id,
                endpoint=payload.endpoint,
                keys=payload.keys.model_dump(),
            )
        )
    await db.commit()
    return PushSubscribeResponse(subscribed=True)


@app.post(
    "/api/v1/ratings",
    response_model=RatingResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def rate_seller(
    payload: RatingInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    row_result = await db.execute(
        select(ListingModel, ScanModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .where(ListingModel.id == payload.listing_id)
    )
    row = row_result.first()
    if not row:
        raise AppProductionException("NOT_FOUND", "Annonce introuvable.", 404)
    listing, scan = row
    if scan.user_id != payload.seller_id:
        raise AppProductionException("VALIDATION_ERROR", "Vendeur invalide pour cette annonce.", 400)
    if user.id == payload.seller_id:
        raise AppProductionException("CONFLICT", "Un vendeur ne peut pas se noter lui-meme.", 409)

    existing_result = await db.execute(
        select(SellerRatingModel).where(SellerRatingModel.listing_id == listing.id, SellerRatingModel.buyer_id == user.id)
    )
    rating = existing_result.scalar_one_or_none()
    if rating:
        rating.stars = payload.stars
        rating.comment = payload.comment
    else:
        rating = SellerRatingModel(
            id=str(uuid.uuid4()),
            listing_id=listing.id,
            seller_id=payload.seller_id,
            buyer_id=user.id,
            stars=payload.stars,
            comment=payload.comment,
        )
        db.add(rating)
    await db.commit()
    await db.refresh(rating)
    return RatingResponse(id=rating.id, ok=True)


@app.get(
    "/api/v1/sellers/{seller_id_or_name}",
    response_model=SellerProfileResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_seller_profile(
    seller_id_or_name: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    _viewer_user, viewer_subscription = await _optional_auth_context(authorization, db)
    viewer_role = viewer_subscription.tier if viewer_subscription else "guest"
    like_value = f"%{seller_id_or_name}%"
    seller_result = await db.execute(
        select(UserModel).where(
            or_(
                UserModel.id == seller_id_or_name,
                UserModel.phone == seller_id_or_name,
                UserModel.email == seller_id_or_name,
                UserModel.first_name.ilike(like_value),
                UserModel.last_name.ilike(like_value),
            )
        ).limit(1)
    )
    seller = seller_result.scalars().first()
    if not seller:
        raise AppProductionException("NOT_FOUND", "Vendeur introuvable.", 404)

    ratings_result = await db.execute(select(SellerRatingModel).where(SellerRatingModel.seller_id == seller.id))
    ratings = ratings_result.scalars().all()
    average_rating = round(sum(rating.stars for rating in ratings) / len(ratings), 2) if ratings else 0.0

    listings_result = await db.execute(
        select(ListingModel, ScanModel, UserModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .outerjoin(UserModel, UserModel.id == ScanModel.user_id)
        .where(ScanModel.user_id == seller.id, ListingModel.status.in_(list(MARKETPLACE_VISIBLE_STATUSES)))
        .order_by(*_marketplace_priority_ordering())
    )
    listings = [
        _public_listing_item(listing, scan, listing_seller, viewer_role)
        for listing, scan, listing_seller in listings_result.all()
    ]
    return SellerProfileResponse(
        id=seller.id,
        name=_seller_full_name(seller),
        average_rating=average_rating,
        total_ratings=len(ratings),
        listings=listings,
    )


async def _wallet_account_for_user(user_id: str, db: AsyncSession) -> WalletAccountModel:
    result = await db.execute(select(WalletAccountModel).where(WalletAccountModel.user_id == user_id))
    account = result.scalar_one_or_none()
    if not account:
        account = WalletAccountModel(user_id=user_id, balance=0.0, currency="MAD")
        db.add(account)
        await db.flush()
    return account


@app.get(
    "/api/v1/wallet",
    response_model=WalletResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_wallet(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    account = await _wallet_account_for_user(user.id, db)
    tx_result = await db.execute(
        select(WalletTransactionModel)
        .where(WalletTransactionModel.user_id == user.id)
        .order_by(WalletTransactionModel.created_at.desc())
        .limit(100)
    )
    transactions = [
        WalletTransactionResponse(
            id=tx.id,
            type=tx.type,
            amount=tx.amount,
            fee=tx.fee,
            net=tx.net,
            desc=tx.desc,
            created_at=tx.created_at.isoformat() if tx.created_at else "",
            status=tx.status,
        )
        for tx in tx_result.scalars().all()
    ]
    await db.commit()
    return WalletResponse(balance=account.balance, currency=account.currency, transactions=transactions)


@app.post(
    "/api/v1/wallet/withdraw",
    response_model=WithdrawResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def withdraw_wallet(
    payload: WithdrawInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, _subscription, _token = await _current_auth_context(authorization, db)
    account = await _wallet_account_for_user(user.id, db)
    if payload.amount > account.balance:
        raise AppProductionException("INSUFFICIENT_FUNDS", "Solde insuffisant.", 409)
    fee = round(payload.amount * 0.02, 2)
    net = round(payload.amount - fee, 2)
    account.balance = round(account.balance - payload.amount, 2)
    account.updated_at = _utc_now()
    withdrawal = WithdrawalRequestModel(
        id=str(uuid.uuid4()),
        user_id=user.id,
        amount=payload.amount,
        iban=payload.iban,
        status="processing",
        estimated_days=2,
    )
    db.add(withdrawal)
    db.add(
        WalletTransactionModel(
            id=str(uuid.uuid4()),
            user_id=user.id,
            type="withdrawal",
            amount=payload.amount,
            fee=fee,
            net=net,
            desc="Demande de retrait",
            status="pending",
        )
    )
    await db.commit()
    return WithdrawResponse(request_id=withdrawal.id, status=withdrawal.status, estimated_days=withdrawal.estimated_days)


@app.get(
    "/api/v1/marketplace/my-listings",
    response_model=List[PublicListingItem],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_my_marketplace_listings(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _session, subscription, _token = await _current_auth_context(authorization, db)
    result = await db.execute(
        select(ListingModel, ScanModel, UserModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .outerjoin(UserModel, UserModel.id == ScanModel.user_id)
        .where(ScanModel.user_id == user.id)
        .order_by(*_marketplace_priority_ordering())
    )
    return [_public_listing_item(listing, scan, seller, subscription.tier) for listing, scan, seller in result.all()]


@app.post(
    "/api/v1/marketplace/search",
    response_model=List[PublicListingItem],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def search_marketplace(
    payload: MarketplaceSearchInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    _viewer_user, viewer_subscription = await _optional_auth_context(authorization, db)
    viewer_role = viewer_subscription.tier if viewer_subscription else "guest"
    query = (
        select(ListingModel, ScanModel, UserModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .outerjoin(UserModel, UserModel.id == ScanModel.user_id)
        .where(ListingModel.status.in_(list(MARKETPLACE_VISIBLE_STATUSES)))
    )
    if payload.query:
        like_value = f"%{payload.query.strip()}%"
        query = query.where(
            or_(
                ListingModel.title.ilike(like_value),
                ListingModel.description.ilike(like_value),
                ScanModel.dominant_class.ilike(like_value),
            )
        )
    if payload.region:
        query = query.where(ListingModel.region.ilike(f"%{payload.region.strip()}%"))
    if payload.classification:
        query = query.where(ScanModel.dominant_class.ilike(f"%{payload.classification.strip()}%"))
    if payload.price_min is not None:
        query = query.where(ListingModel.price >= payload.price_min)
    if payload.price_max is not None:
        query = query.where(ListingModel.price <= payload.price_max)

    result = await db.execute(query.order_by(*_marketplace_priority_ordering()))
    return [
        _public_listing_item(listing, scan, seller, viewer_role)
        for listing, scan, seller in result.all()
        if _effective_listing_status(listing) in MARKETPLACE_VISIBLE_STATUSES
    ]


@app.get(
    "/api/v1/marketplace/stats",
    response_model=MarketplaceStatsResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_marketplace_stats(
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    result = await db.execute(
        select(ListingModel, ScanModel)
        .join(ScanModel, ListingModel.scan_id == ScanModel.id)
        .where(ListingModel.status.in_(list(MARKETPLACE_VISIBLE_STATUSES | {"rejected"})))
    )
    rows = result.all()
    total_listings = len(rows)
    sold_rows = [(listing, scan) for listing, scan in rows if _legacy_listing_status(listing.status) == "sold"]
    priced_rows = [(listing, scan) for listing, scan in rows if listing.price > 0]
    avg_price = round(sum(listing.price for listing, _scan in priced_rows) / len(priced_rows), 2) if priced_rows else 0.0

    by_class: dict[str, list[float]] = {}
    by_region: dict[str, int] = {}
    for listing, scan in rows:
        by_class.setdefault(scan.dominant_class, []).append(listing.price)
        region = listing.region or "unknown"
        by_region[region] = by_region.get(region, 0) + 1

    trending = [
        {
            "classification": class_name,
            "change_percent": 0.0,
            "avg_price": round(sum(prices) / len(prices), 2) if prices else 0.0,
        }
        for class_name, prices in sorted(by_class.items(), key=lambda item: len(item[1]), reverse=True)[:5]
    ]
    volume_by_region = [
        {
            "region": region,
            "count": count,
            "pct": round(count / total_listings, 4) if total_listings else 0.0,
        }
        for region, count in sorted(by_region.items(), key=lambda item: item[1], reverse=True)[:8]
    ]
    price_history = {
        class_name: [round(sum(prices) / len(prices), 2) if prices else 0.0 for _month in range(12)]
        for class_name, prices in by_class.items()
    }
    return MarketplaceStatsResponse(
        total_listings=total_listings,
        total_sales=len(sold_rows),
        avg_price_dh=avg_price,
        trending=trending,
        volume_by_region=volume_by_region,
        price_history=price_history,
        months=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
    )


@app.post(
    "/api/v1/marketplace/chat/send",
    response_model=MessageResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def send_chat_message(
    payload: CreateMessageInput,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Envoie un message via le Marketplace.
    """
    message_id = str(uuid.uuid4())
    new_message = MessageModel(
        id=message_id,
        conversation_id=payload.conversation_id,
        sender_id=payload.sender_id,
        receiver_id=payload.receiver_id,
        text_content=payload.text_content
    )
    db.add(new_message)
    await db.commit()
    await db.refresh(new_message)
    
    return MessageResponse(
        id=new_message.id,
        conversation_id=new_message.conversation_id,
        sender_id=new_message.sender_id,
        receiver_id=new_message.receiver_id,
        text_content=new_message.text_content,
        timestamp=new_message.timestamp.isoformat()
    )


@app.get(
    "/api/v1/marketplace/chat/history/{conversation_id}",
    response_model=List[MessageResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_chat_history(
    conversation_id: str,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Récupère l'historique des messages d'une conversation liée au Marketplace.
    """
    query = select(MessageModel).where(MessageModel.conversation_id == conversation_id).order_by(MessageModel.timestamp.asc())
    result = await db.execute(query)
    messages = result.scalars().all()
    
    return [
        MessageResponse(
            id=msg.id,
            conversation_id=msg.conversation_id,
            sender_id=msg.sender_id,
            receiver_id=msg.receiver_id,
            text_content=msg.text_content,
            timestamp=msg.timestamp.isoformat()
        )
        for msg in messages
    ]


# ---------------------------------------------------------------------------
# Expert dataset workspace
# ---------------------------------------------------------------------------

DATASET_ALLOWED_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/tiff",
    "image/heic",
    "image/heif",
}
DATASET_MAX_FILE_SIZE_BYTES = int(os.getenv("DATASET_MAX_FILE_SIZE_BYTES", str(20 * 1024 * 1024)))
DATASET_LEASE_MINUTES = int(os.getenv("DATASET_LEASE_MINUTES", "15"))
V2_AUDIT_DELAY_HOURS = int(os.getenv("EXPERT_V2_AUDIT_DELAY_HOURS", "72"))


def _dataset_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _dataset_response(batch: DatasetBatchModel) -> ExpertDatasetResponse:
    return ExpertDatasetResponse(
        id=batch.id,
        name=batch.name,
        description=batch.description,
        status=batch.status,
        taxonomy_version=batch.taxonomy_version,
        annotation_policy_version=batch.annotation_policy_version,
        statistics=batch.statistics or {},
        created_at=batch.created_at.isoformat() if batch.created_at else "",
        updated_at=batch.updated_at.isoformat() if batch.updated_at else "",
    )


def _is_v2_dataset(batch: DatasetBatchModel) -> bool:
    return batch.taxonomy_version == TAXONOMY_V2_VERSION


def _is_v2_item(item: DatasetItemModel) -> bool:
    return (item.item_metadata or {}).get("dataset_taxonomy_version") == TAXONOMY_V2_VERSION


def _v2_item_metadata(item: DatasetItemModel) -> dict:
    return {
        "source_type": item.source_type,
        "source_reference": item.source_reference,
        "rights_status": item.rights_status,
        "evidence_tier": item.evidence_tier,
        "view_type": item.view_type,
        "historical_label": item.historical_label,
        "duplicate_group_id": item.duplicate_group_id,
        "blind_annotation": True,
    }


def _normalize_dataset_image(data: bytes) -> tuple[bytes, bytes, dict]:
    return normalize_image_assets(data)


async def _dataset_object_url(object_key: Optional[str]) -> Optional[str]:
    if not object_key:
        return None
    return await storage_provider.create_presigned_get(object_key)


def _second_review_sample(item: DatasetItemModel) -> bool:
    seed = item.sha256 or item.id
    return int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8], 16) % 100 < 10


def _v2_audit_sample(item: DatasetItemModel) -> bool:
    seed = item.sha256 or item.id
    return int(hashlib.sha256(f"audit-v2:{seed}".encode("utf-8")).hexdigest()[:8], 16) % 100 < 15


def _queue_reason(item: DatasetItemModel) -> str:
    if item.status == "audit_pending":
        return "audit_sample"
    if item.status == "needs_review":
        return "human_review"
    if (item.quality_report or {}).get("issues"):
        return "quality_flag"
    model_scores = [
        float(model.get("meteorite_probability"))
        for model in ((item.raw_prediction or {}).get("models") or {}).values()
        if isinstance(model, dict) and model.get("meteorite_probability") is not None
    ]
    if len(model_scores) >= 2 and max(model_scores) - min(model_scores) >= 0.15:
        return "model_disagreement"
    probability = float((item.raw_prediction or {}).get("meteorite_probability") or 0.0)
    if 0.45 <= probability < 0.85:
        return "borderline_score"
    if _second_review_sample(item):
        return "calibration_sample"
    return "standard"


def _queue_priority(item: DatasetItemModel) -> int:
    return {
        "audit_sample": 0,
        "human_review": 0,
        "model_disagreement": 10,
        "borderline_score": 20,
        "quality_flag": 30,
        "calibration_sample": 40,
        "standard": 50,
    }[_queue_reason(item)]


async def _dataset_item_response(
    item: DatasetItemModel,
    consensus: Optional[DatasetConsensusModel] = None,
) -> ExpertQueueItemResponse:
    is_v2 = _is_v2_item(item)
    reveal_prediction = not is_v2 or (
        bool((item.item_metadata or {}).get("prediction_revealed"))
        and item.status not in {"audit_pending", "audit_in_progress"}
    )
    prediction = ExpertModelPrediction(**(item.raw_prediction or {})) if item.raw_prediction and reveal_prediction else None
    metadata = {
        **(item.item_metadata or {}),
        **(_v2_item_metadata(item) if is_v2 else {}),
    }
    return ExpertQueueItemResponse(
        item_id=item.id,
        dataset_id=item.batch_id,
        status=item.status,
        image_url=await _dataset_object_url(item.normalized_object_key or item.original_object_key),
        thumbnail_url=await _dataset_object_url(item.thumbnail_object_key),
        original_filename=item.original_filename,
        specimen_id=item.specimen_id,
        content_type=item.content_type,
        quality_report=item.quality_report,
        metadata=metadata,
        prediction=prediction,
        lease_expires_at=item.lease_expires_at.isoformat() if item.lease_expires_at else None,
        import_id=item.import_id,
        queue_reason=_queue_reason(item),
        review_stage=("audit" if item.status in {"audit_pending", "audit_in_progress"} else "secondary" if item.status == "needs_review" else "primary"),
        consensus_status=consensus.status if consensus else None,
        training_eligible=bool(consensus.training_eligible) if consensus else False,
    )


async def _import_response(import_row: DatasetImportModel, db: AsyncSession) -> ExpertImportResponse:
    items_result = await db.execute(
        select(DatasetItemModel).where(DatasetItemModel.import_id == import_row.id)
    )
    items = items_result.scalars().all()
    counts = Counter(item.status for item in items)
    statistics = {
        **(import_row.statistics or {}),
        "uploaded": len(items),
        "ready": counts.get("pending_annotation", 0),
        "processing": counts.get("inference_pending", 0) + counts.get("processing_inference", 0),
        "duplicates": counts.get("skipped", 0),
        "errors": counts.get("error", 0),
        "status_counts": dict(counts),
    }
    return ExpertImportResponse(
        id=import_row.id,
        dataset_id=import_row.batch_id,
        name=import_row.name,
        status=import_row.status,
        source_attested=import_row.source_attested,
        source_type=import_row.source_type,
        source_reference=import_row.source_reference,
        rights_status=import_row.rights_status,
        evidence_tier=import_row.evidence_tier,
        view_type=import_row.view_type,
        statistics=statistics,
        created_at=import_row.created_at.isoformat() if import_row.created_at else "",
        updated_at=import_row.updated_at.isoformat() if import_row.updated_at else "",
    )


async def _refresh_import_status(import_id: Optional[str], db: AsyncSession) -> None:
    if not import_id:
        return
    import_row = await db.get(DatasetImportModel, import_id)
    if not import_row:
        return
    result = await db.execute(
        select(DatasetItemModel.status).where(DatasetItemModel.import_id == import_id)
    )
    statuses = list(result.scalars().all())
    if statuses and not any(status in {"inference_pending", "processing_inference", "imported"} for status in statuses):
        import_row.status = "ready"
    elif statuses:
        import_row.status = "processing"
    import_row.updated_at = _dataset_now()


async def _require_dataset_admin(authorization: Optional[str], db: AsyncSession):
    user, subscription = await _require_expert_context(authorization, db)
    if user.role != "admin" and subscription.tier != "admin":
        raise AppProductionException("FORBIDDEN", "Acces administrateur dataset requis.", 403)
    return user, subscription


async def _infer_dataset_item(item_id: str) -> None:
    if vision_pipeline is None:
        return
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(DatasetItemModel)
            .where(
                DatasetItemModel.id == item_id,
                DatasetItemModel.status.in_({"inference_pending", "imported"}),
            )
            .with_for_update(skip_locked=True)
        )
        item = result.scalar_one_or_none()
        if not item:
            return
        item.status = "processing_inference"
        item.updated_at = _dataset_now()
        await db.commit()
        try:
            original_bytes = await storage_provider.get_object(item.original_object_key)
            image_sha = item.sha256 or sha256_hex(original_bytes)
            duplicate_result = await db.execute(
                select(DatasetItemModel).where(
                    DatasetItemModel.batch_id == item.batch_id,
                    DatasetItemModel.sha256 == image_sha,
                    DatasetItemModel.id != item.id,
                )
            )
            duplicate = duplicate_result.scalar_one_or_none()
            if duplicate:
                item.sha256 = image_sha
                item.status = "skipped"
                item.item_metadata = {
                    **(item.item_metadata or {}),
                    "duplicate_of": duplicate.id,
                }
                item.updated_at = _dataset_now()
                await _refresh_import_status(item.import_id, db)
                await db.commit()
                return
            image_phash = item.perceptual_hash or perceptual_hash(original_bytes)
            perceptual_duplicate_result = await db.execute(
                select(DatasetItemModel).where(
                    DatasetItemModel.batch_id == item.batch_id,
                    DatasetItemModel.perceptual_hash == image_phash,
                    DatasetItemModel.id != item.id,
                ).limit(1)
            )
            perceptual_duplicate = perceptual_duplicate_result.scalar_one_or_none()
            item.perceptual_hash = image_phash
            if perceptual_duplicate:
                item.item_metadata = {
                    **(item.item_metadata or {}),
                    "perceptual_duplicate_of": perceptual_duplicate.id,
                }
            if not item.normalized_object_key:
                normalized, thumbnail, quality = _normalize_dataset_image(original_bytes)
                item.sha256 = image_sha
                normalized_key = f"datasets/{item.batch_id}/normalized/{item.id}.jpg"
                thumbnail_key = f"datasets/{item.batch_id}/thumbnails/{item.id}.jpg"
                item.normalized_object_key = await storage_provider.save_object(
                    normalized, normalized_key, "image/jpeg"
                )
                item.thumbnail_object_key = await storage_provider.save_object(
                    thumbnail, thumbnail_key, "image/jpeg"
                )
                item.quality_report = quality
                image_bytes = normalized
            else:
                image_bytes = await storage_provider.get_object(item.normalized_object_key)
            raw_models = await anyio.to_thread.run_sync(
                vision_pipeline.predict_image_parallel,
                image_bytes,
            )
            item.raw_prediction = build_single_image_prediction(raw_models)
            item.model_version = MODEL_VERSION
            item.status = "pending_annotation"
            item.item_metadata = {
                **(item.item_metadata or {}),
                "queue_priority": _queue_priority(item),
            }
            item.updated_at = _dataset_now()
            await _refresh_import_status(item.import_id, db)
            await db.commit()
        except Exception as exc:
            item.status = "inference_pending"
            item.item_metadata = {
                **(item.item_metadata or {}),
                "inference_error": str(exc)[:500],
            }
            item.updated_at = _dataset_now()
            await _refresh_import_status(item.import_id, db)
            await db.commit()


async def _dataset_item_for_id(item_id: str, db: AsyncSession) -> DatasetItemModel:
    result = await db.execute(select(DatasetItemModel).where(DatasetItemModel.id == item_id))
    item = result.scalar_one_or_none()
    if not item:
        raise AppProductionException("NOT_FOUND", "Image dataset introuvable.", 404)
    return item


async def _create_v2_audit(
    *,
    batch: DatasetBatchModel,
    user: UserModel,
    db: AsyncSession,
) -> ExpertAuditResponse:
    """Audit the v2 evidence chain instead of applying legacy accuracy thresholds."""
    rows_result = await db.execute(
        select(DatasetItemModel, DatasetConsensusModel)
        .outerjoin(DatasetConsensusModel, DatasetConsensusModel.dataset_item_id == DatasetItemModel.id)
        .where(DatasetItemModel.batch_id == batch.id)
    )
    rows = rows_result.all()
    events_result = await db.execute(
        select(AnnotationEventModel)
        .join(DatasetItemModel, AnnotationEventModel.dataset_item_id == DatasetItemModel.id)
        .where(DatasetItemModel.batch_id == batch.id)
        .order_by(AnnotationEventModel.created_at.asc())
    )
    events_by_item: dict[str, list[AnnotationEventModel]] = {}
    for event in events_result.scalars().all():
        events_by_item.setdefault(event.dataset_item_id, []).append(event)
    status_counts = Counter(item.status for item, _consensus in rows)
    verdict_counts = Counter((consensus.final_label if consensus else None) or "unlabeled" for _item, consensus in rows)
    role_counts = Counter((consensus.training_role if consensus else None) or "unresolved" for _item, consensus in rows)
    source_counts = Counter(item.source_type for item, _consensus in rows)
    evidence_counts = Counter(
        (consensus.evidence_tier if consensus and consensus.evidence_tier else item.evidence_tier)
        for item, consensus in rows
    )
    quality_counts = Counter(
        "passed" if (item.quality_report or {}).get("passed", True) else "flagged"
        for item, _consensus in rows
    )
    duplicate_groups: dict[str, list[str]] = {}
    confusion = Counter()
    errors: list[dict] = []
    audited, agreements = 0, 0
    for item, consensus in rows:
        group_id = item.duplicate_group_id or item.specimen_id or item.perceptual_hash or item.id
        duplicate_groups.setdefault(group_id, []).append(item.id)
        human = consensus.final_label if consensus else None
        probability = (item.raw_prediction or {}).get("meteorite_probability")
        if human in {"meteorite_candidate", "terrestrial"} and isinstance(probability, (float, int)):
            model_label = "meteorite_candidate" if float(probability) >= 0.5 else "terrestrial"
            confusion[f"{human}__vs__{model_label}"] += 1
        item_events = [event for event in events_by_item.get(item.id, []) if event.is_blind and event.action in {"label", "review"}]
        if len(item_events) >= 2:
            audited += 1
            first, second = item_events[0], item_events[-1]
            first_signature = (first.visual_verdict or first.top_label, first.meteorite_subclass, first.terrestrial_family)
            second_signature = (second.visual_verdict or second.top_label, second.meteorite_subclass, second.terrestrial_family)
            agreements += int(first_signature == second_signature)
        if consensus and consensus.audit_status == "conflict":
            errors.append({"item_id": item.id, "error_type": "human_audit_conflict", "group_id": group_id})
        if (item.item_metadata or {}).get("historical_label_conflict"):
            errors.append({"item_id": item.id, "error_type": "historical_label_conflict", "group_id": group_id})
    duplicate_sets = [
        {"group_id": group_id, "image_ids": image_ids}
        for group_id, image_ids in duplicate_groups.items() if len(image_ids) > 1
    ]
    imports_result = await db.execute(select(DatasetImportModel).where(DatasetImportModel.batch_id == batch.id))
    import_errors = sum(int((row.statistics or {}).get("errors", 0)) for row in imports_result.scalars().all())
    summary = {
        "contract": VISION_V2_DATASET_CONTRACT_VERSION,
        "total": len(rows),
        "status_counts": dict(status_counts),
        "verdict_counts": dict(verdict_counts),
        "training_roles": dict(role_counts),
        "sources": dict(source_counts),
        "evidence_tiers": dict(evidence_counts),
        "quality": dict(quality_counts),
        "duplicate_groups": len(duplicate_sets),
        "import_errors": import_errors,
        "human_audit": {
            "audited": audited,
            "agreement": agreements,
            "agreement_rate": round(agreements / audited, 6) if audited else None,
            "conflicts": sum(1 for _item, consensus in rows if consensus and consensus.audit_status == "conflict"),
        },
        "trio_vs_human_confusion": dict(confusion),
    }
    recommendations = []
    if role_counts.get("gold", 0) + role_counts.get("hard_negatives", 0) == 0:
        recommendations.append("Aucune référence auditée n'est prête pour validation/test; ne lancez pas de fine-tuning.")
    if errors:
        recommendations.append("Résolvez les conflits humains et historiques avant un snapshot de référence.")
    if import_errors:
        recommendations.append("Résolvez les erreurs d'import ou d'image corrompue avant export.")
    audit_id = str(uuid.uuid4())
    report_key = f"datasets/{batch.id}/reports/{audit_id}/summary.json"
    errors_key = f"datasets/{batch.id}/reports/{audit_id}/errors.json"
    await storage_provider.save_object(json.dumps({"summary": summary, "duplicates": duplicate_sets}, ensure_ascii=False, indent=2).encode("utf-8"), report_key, "application/json")
    await storage_provider.save_object(json.dumps(errors, ensure_ascii=False, indent=2).encode("utf-8"), errors_key, "application/json")
    audit = AuditRunModel(
        id=audit_id, batch_id=batch.id, created_by=user.id, status="completed", model_version="trio-v2-data-contract",
        summary=summary, recommendations=recommendations, report_object_key=report_key, errors_object_key=errors_key,
        created_at=_dataset_now(), completed_at=_dataset_now(),
    )
    db.add(audit)
    await _write_audit_log(
        db, actor_user_id=user.id, action="expert_v2_dataset_audited", entity_type="dataset_audit", entity_id=audit.id,
        metadata={"dataset_id": batch.id, "summary": summary},
    )
    await db.commit()
    return ExpertAuditResponse(
        id=audit.id, dataset_id=batch.id, status=audit.status, model_version=audit.model_version,
        summary=summary, recommendations=recommendations, report_url=await _dataset_object_url(report_key),
        errors_url=await _dataset_object_url(errors_key), created_at=audit.created_at.isoformat(), completed_at=audit.completed_at.isoformat(),
        taxonomy_version=batch.taxonomy_version, annotation_policy_version=batch.annotation_policy_version, dataset_version=batch.id,
    )


@app.post(
    "/api/v1/admin/expert-accounts",
    response_model=ExpertAccountResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def admin_create_expert_account(
    payload: ExpertAccountCreateInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    admin, _subscription = await _require_admin_context(authorization, db)
    phone = payload.phone.strip()
    email = payload.email.strip() if payload.email else None
    duplicate_query = select(UserModel).where(
        or_(UserModel.phone == phone, UserModel.email == email if email else False)
    )
    duplicate_result = await db.execute(duplicate_query)
    if duplicate_result.scalar_one_or_none():
        raise AppProductionException("CONFLICT", "Un compte utilise déjà ce téléphone ou cet email.", 409)
    user = UserModel(
        id=str(uuid.uuid4()),
        first_name=payload.first_name.strip(),
        last_name=payload.last_name.strip(),
        phone=phone,
        email=email,
        password_hash=hash_password(payload.password),
        role="expert",
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    subscription = await get_or_create_subscription(user.id, db)
    subscription.tier = "free"
    subscription.remaining_tokens = 0
    await db.commit()
    await _write_audit_log(
        db,
        actor_user_id=admin.id,
        action="admin_create_expert_account",
        entity_type="user",
        entity_id=user.id,
        metadata={"permissions": ["dataset.read", "dataset.annotate"]},
    )
    await db.commit()
    return ExpertAccountResponse(
        user=AuthUserResponse(
            id=user.id,
            first_name=user.first_name,
            last_name=user.last_name,
            phone=user.phone,
            email=user.email,
            role="expert",
        ),
    )


@app.get(
    "/api/v1/admin/expert-users",
    response_model=List[ExpertUserResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def admin_list_expert_users(
    query: Optional[str] = Query(None, max_length=120),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_dataset_admin(authorization, db)
    statement = (
        select(UserModel)
        .where(UserModel.role != "admin")
        .order_by(UserModel.created_at.desc())
        .limit(50)
    )
    if query:
        pattern = f"%{query.strip()}%"
        statement = statement.where(
            or_(
                UserModel.first_name.ilike(pattern),
                UserModel.last_name.ilike(pattern),
                UserModel.email.ilike(pattern),
                UserModel.phone.ilike(pattern),
            )
        )
    result = await db.execute(statement)
    return [
        ExpertUserResponse(
            id=user.id,
            first_name=user.first_name,
            last_name=user.last_name,
            phone=user.phone,
            email=user.email,
            role=user.role,
        )
        for user in result.scalars().all()
    ]


@app.patch(
    "/api/v1/admin/expert-users/{user_id}/role",
    response_model=ExpertUserResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def admin_update_expert_user_role(
    user_id: str,
    payload: ExpertRoleUpdateInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    admin, _subscription = await _require_dataset_admin(authorization, db)
    user = await db.get(UserModel, user_id)
    if not user:
        raise AppProductionException("NOT_FOUND", "Utilisateur introuvable.", 404)
    if user.role == "admin":
        raise AppProductionException("FORBIDDEN", "Le rôle administrateur ne peut pas être modifié ici.", 403)
    previous_role = user.role
    user.role = payload.role
    subscription = await get_or_create_subscription(user.id, db)
    if payload.role == "expert":
        subscription.tier = "free"
        subscription.status = "none"
        subscription.remaining_tokens = 0
    elif payload.role == "premium":
        subscription.tier = "premium"
        subscription.status = "active"
    else:
        subscription.tier = "free"
        subscription.status = "none"
    await _write_audit_log(
        db,
        actor_user_id=admin.id,
        action="admin_update_expert_role",
        entity_type="user",
        entity_id=user.id,
        metadata={"from": previous_role, "to": payload.role},
    )
    await db.commit()
    return ExpertUserResponse(
        id=user.id,
        first_name=user.first_name,
        last_name=user.last_name,
        phone=user.phone,
        email=user.email,
        role=user.role,
    )


@app.post(
    "/api/v1/expert/datasets",
    response_model=ExpertDatasetResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def expert_create_dataset(
    payload: ExpertDatasetCreateInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_dataset_admin(authorization, db)
    if payload.taxonomy_version not in {TAXONOMY_VERSION, TAXONOMY_V2_VERSION}:
        raise AppProductionException("VALIDATION_ERROR", "Version de taxonomie non prise en charge.", 400)
    expected_policy = ANNOTATION_POLICY_V2_VERSION if payload.taxonomy_version == TAXONOMY_V2_VERSION else ANNOTATION_POLICY_VERSION
    if payload.annotation_policy_version != expected_policy:
        raise AppProductionException("VALIDATION_ERROR", "La taxonomie et la politique d'annotation doivent correspondre.", 400)
    now = _dataset_now()
    batch = DatasetBatchModel(
        id=str(uuid.uuid4()),
        name=payload.name.strip(),
        description=payload.description,
        status="active",
        taxonomy_version=payload.taxonomy_version,
        annotation_policy_version=payload.annotation_policy_version,
        created_by=user.id,
        statistics={},
        created_at=now,
        updated_at=now,
    )
    db.add(batch)
    await db.commit()
    await db.refresh(batch)
    return _dataset_response(batch)


@app.get(
    "/api/v1/expert/datasets",
    response_model=List[ExpertDatasetResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_list_datasets(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    result = await db.execute(select(DatasetBatchModel).order_by(DatasetBatchModel.created_at.desc()))
    return [_dataset_response(batch) for batch in result.scalars().all()]


@app.get(
    "/api/v1/expert/datasets/{dataset_id}",
    response_model=ExpertDatasetResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_get_dataset(
    dataset_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    result = await db.execute(select(DatasetBatchModel).where(DatasetBatchModel.id == dataset_id))
    batch = result.scalar_one_or_none()
    if not batch:
        raise AppProductionException("NOT_FOUND", "Dataset introuvable.", 404)
    return _dataset_response(batch)


@app.post(
    "/api/v1/expert/datasets/{dataset_id}/imports",
    response_model=ExpertImportResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def expert_create_import(
    dataset_id: str,
    payload: ExpertImportCreateInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_dataset_admin(authorization, db)
    batch = await db.get(DatasetBatchModel, dataset_id)
    if not batch:
        raise AppProductionException("NOT_FOUND", "Dataset introuvable.", 404)
    if not payload.source_attested:
        raise AppProductionException(
            "VALIDATION_ERROR",
            "L'attestation de droit d'utilisation des images est obligatoire.",
            400,
        )
    if _is_v2_dataset(batch):
        if payload.source_type not in V2_SOURCE_TYPES:
            raise AppProductionException("VALIDATION_ERROR", "Type de source v2 invalide.", 400)
        if payload.evidence_tier not in V2_EVIDENCE_TIERS:
            raise AppProductionException("VALIDATION_ERROR", "Niveau de preuve v2 invalide.", 400)
        if payload.view_type not in V2_VIEW_TYPES:
            raise AppProductionException("VALIDATION_ERROR", "Type de vue v2 invalide.", 400)
    now = _dataset_now()
    import_row = DatasetImportModel(
        id=str(uuid.uuid4()),
        batch_id=dataset_id,
        name=payload.name.strip(),
        source_attested=True,
        source_type=payload.source_type,
        source_reference=payload.source_reference.strip() if payload.source_reference else None,
        rights_status=payload.rights_status,
        evidence_tier=payload.evidence_tier,
        view_type=payload.view_type,
        import_metadata={"dataset_taxonomy_version": batch.taxonomy_version},
        status="uploading",
        created_by=user.id,
        statistics={},
        created_at=now,
        updated_at=now,
    )
    db.add(import_row)
    await _write_audit_log(
        db,
        actor_user_id=user.id,
        action="expert_import_created",
        entity_type="dataset_import",
        entity_id=import_row.id,
        metadata={
            "dataset_id": dataset_id,
            "name": import_row.name,
            "source_type": import_row.source_type,
            "evidence_tier": import_row.evidence_tier,
        },
    )
    await db.commit()
    return await _import_response(import_row, db)


@app.get(
    "/api/v1/expert/datasets/{dataset_id}/imports",
    response_model=List[ExpertImportResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_list_imports(
    dataset_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    result = await db.execute(
        select(DatasetImportModel)
        .where(DatasetImportModel.batch_id == dataset_id)
        .order_by(DatasetImportModel.created_at.desc())
    )
    return [await _import_response(row, db) for row in result.scalars().all()]


@app.patch(
    "/api/v1/expert/imports/{import_id}/metadata",
    response_model=ExpertImportResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_update_import_metadata(
    import_id: str,
    payload: ExpertImportMetadataUpdateInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_dataset_admin(authorization, db)
    import_row = await db.get(DatasetImportModel, import_id)
    if not import_row:
        raise AppProductionException("NOT_FOUND", "Lot d'import introuvable.", 404)
    batch = await db.get(DatasetBatchModel, import_row.batch_id)
    if not batch or not _is_v2_dataset(batch):
        raise AppProductionException("VALIDATION_ERROR", "Seuls les lots taxonomy-v2 peuvent être corrigés.", 400)
    if (
        payload.source_type not in V2_SOURCE_TYPES
        or payload.evidence_tier not in V2_EVIDENCE_TIERS
        or payload.view_type not in V2_VIEW_TYPES
    ):
        raise AppProductionException("VALIDATION_ERROR", "Métadonnées v2 d'import invalides.", 400)

    items_result = await db.execute(
        select(DatasetItemModel).where(DatasetItemModel.import_id == import_id)
    )
    items = items_result.scalars().all()
    if items:
        annotation_result = await db.execute(
            select(AnnotationEventModel.id)
            .where(AnnotationEventModel.dataset_item_id.in_([item.id for item in items]))
            .limit(1)
        )
        if annotation_result.scalar_one_or_none():
            raise AppProductionException(
                "CONFLICT",
                "Les métadonnées ne peuvent plus être modifiées après la première annotation.",
                409,
            )

    previous = {
        "source_type": import_row.source_type,
        "source_reference": import_row.source_reference,
        "rights_status": import_row.rights_status,
        "evidence_tier": import_row.evidence_tier,
        "view_type": import_row.view_type,
    }
    import_row.source_type = payload.source_type
    import_row.source_reference = payload.source_reference.strip() if payload.source_reference else None
    import_row.rights_status = payload.rights_status
    import_row.evidence_tier = payload.evidence_tier
    import_row.view_type = payload.view_type
    now = _dataset_now()
    import_row.updated_at = now
    for item in items:
        item.source_type = import_row.source_type
        item.source_reference = import_row.source_reference
        item.rights_status = import_row.rights_status
        item.evidence_tier = import_row.evidence_tier
        item.view_type = import_row.view_type
        item.item_metadata = {
            **(item.item_metadata or {}),
            "source_type": import_row.source_type,
            "source_reference": import_row.source_reference,
            "origin": import_row.source_type,
            "capture_type": import_row.view_type,
        }
        item.updated_at = now
    await _write_audit_log(
        db,
        actor_user_id=user.id,
        action="expert_import_metadata_corrected",
        entity_type="dataset_import",
        entity_id=import_row.id,
        metadata={"dataset_id": import_row.batch_id, "previous": previous, "next": {
            "source_type": import_row.source_type,
            "source_reference": import_row.source_reference,
            "rights_status": import_row.rights_status,
            "evidence_tier": import_row.evidence_tier,
            "view_type": import_row.view_type,
        }, "affected_items": len(items)},
    )
    await db.commit()
    return await _import_response(import_row, db)


@app.post(
    "/api/v1/expert/imports/{import_id}/complete",
    response_model=ExpertImportResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_complete_import(
    import_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_dataset_admin(authorization, db)
    import_row = await db.get(DatasetImportModel, import_id)
    if not import_row:
        raise AppProductionException("NOT_FOUND", "Lot d'import introuvable.", 404)
    import_row.status = "processing"
    import_row.updated_at = _dataset_now()
    await _write_audit_log(
        db,
        actor_user_id=user.id,
        action="expert_import_completed",
        entity_type="dataset_import",
        entity_id=import_row.id,
        metadata={"dataset_id": import_row.batch_id},
    )
    await db.commit()
    return await _import_response(import_row, db)


@app.post(
    "/api/v1/expert/datasets/{dataset_id}/images",
    response_model=ExpertQueueItemResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def expert_upload_dataset_image(
    dataset_id: str,
    background_tasks: BackgroundTasks,
    image: UploadFile = File(...),
    specimen_id: Optional[str] = Form(None),
    source_type: Optional[str] = Form(None),
    origin: Optional[str] = Form(None),
    capture_type: Optional[str] = Form(None),
    has_interior_cut: Optional[str] = Form(None),
    import_id: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_dataset_admin(authorization, db)
    batch_result = await db.execute(select(DatasetBatchModel).where(DatasetBatchModel.id == dataset_id))
    batch = batch_result.scalar_one_or_none()
    if not batch:
        raise AppProductionException("NOT_FOUND", "Dataset introuvable.", 404)
    import_row = None
    if import_id:
        import_row = await db.get(DatasetImportModel, import_id)
        if not import_row or import_row.batch_id != dataset_id:
            raise AppProductionException("VALIDATION_ERROR", "Lot d'import dataset invalide.", 400)
        if not import_row.source_attested:
            raise AppProductionException("FORBIDDEN", "L'import doit être attesté avant envoi.", 403)
    if image.content_type not in DATASET_ALLOWED_CONTENT_TYPES:
        raise AppProductionException("INVALID_FILE_FORMAT", "Format image non supporté.", 415)
    data = await image.read()
    if len(data) > DATASET_MAX_FILE_SIZE_BYTES:
        raise AppProductionException("FILE_TOO_LARGE", "Image dataset trop volumineuse.", 413)
    try:
        normalized, thumbnail, quality = _normalize_dataset_image(data)
        image_sha = sha256_hex(data)
        image_phash = perceptual_hash(data)
    except Exception as exc:
        raise AppProductionException("INVALID_IMAGE", f"Image illisible: {exc}", 415)

    existing = await db.execute(
        select(DatasetItemModel).where(
            DatasetItemModel.batch_id == dataset_id,
            DatasetItemModel.sha256 == image_sha,
        )
    )
    if existing.scalar_one_or_none():
        if import_row:
            import_row.statistics = {
                **(import_row.statistics or {}),
                "duplicate_uploads": int((import_row.statistics or {}).get("duplicate_uploads", 0)) + 1,
            }
            import_row.updated_at = _dataset_now()
            await db.commit()
        raise AppProductionException("CONFLICT", "Cette image existe déjà dans le dataset.", 409)

    item_id = str(uuid.uuid4())
    raw_key = f"datasets/{dataset_id}/raw/{image_sha}/original"
    normalized_key = f"datasets/{dataset_id}/normalized/{item_id}.jpg"
    thumbnail_key = f"datasets/{dataset_id}/thumbnails/{item_id}.jpg"
    raw_key = await storage_provider.save_object(data, raw_key, image.content_type)
    normalized_key = await storage_provider.save_object(normalized, normalized_key, "image/jpeg")
    thumbnail_key = await storage_provider.save_object(thumbnail, thumbnail_key, "image/jpeg")

    metadata = {
        "source_type": source_type or "unknown",
        "origin": origin or "unknown",
        "capture_type": capture_type or "unknown",
        "has_interior_cut": has_interior_cut or "unknown",
        "upload_filename": image.filename,
        "dataset_taxonomy_version": batch.taxonomy_version,
    }
    now = _dataset_now()
    item = DatasetItemModel(
        id=item_id,
        batch_id=dataset_id,
        import_id=import_id or None,
        specimen_id=specimen_id or None,
        original_filename=image.filename,
        content_type=image.content_type,
        original_object_key=raw_key,
        normalized_object_key=normalized_key,
        thumbnail_object_key=thumbnail_key,
        sha256=image_sha,
        perceptual_hash=image_phash,
        duplicate_group_id=specimen_id or image_phash,
        source_type=(import_row.source_type if import_row else source_type or "other"),
        source_reference=import_row.source_reference if import_row else origin,
        rights_status=import_row.rights_status if import_row else "attested",
        evidence_tier=import_row.evidence_tier if import_row else "unresolved",
        view_type=import_row.view_type if import_row else (capture_type or "unknown"),
        status="inference_pending",
        quality_report=quality,
        item_metadata=metadata,
        created_at=now,
        updated_at=now,
    )
    db.add(item)
    if import_row:
        import_row.updated_at = now
    await db.commit()
    await db.refresh(item)
    if vision_pipeline is not None:
        background_tasks.add_task(_infer_dataset_item, item.id)
    return await _dataset_item_response(item)


@app.post(
    "/api/v1/expert/datasets/{dataset_id}/presign-upload",
    response_model=ExpertPresignUploadResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_presign_upload(
    dataset_id: str,
    payload: ExpertPresignUploadInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_dataset_admin(authorization, db)
    if os.getenv("STORAGE_BACKEND", "local").strip().lower() != "s3":
        raise AppProductionException(
            "SERVICE_UNAVAILABLE",
            "Les uploads présignés nécessitent STORAGE_BACKEND=s3.",
            503,
        )
    result = await db.execute(select(DatasetBatchModel).where(DatasetBatchModel.id == dataset_id))
    if not result.scalar_one_or_none():
        raise AppProductionException("NOT_FOUND", "Dataset introuvable.", 404)
    if payload.import_id:
        import_row = await db.get(DatasetImportModel, payload.import_id)
        if not import_row or import_row.batch_id != dataset_id or not import_row.source_attested:
            raise AppProductionException("VALIDATION_ERROR", "Lot d'import dataset invalide.", 400)
    uploads = []
    for file_spec in payload.files:
        filename = os.path.basename(str(file_spec.get("filename") or "image.jpg"))
        content_type = str(file_spec.get("content_type") or "image/jpeg")
        if content_type not in DATASET_ALLOWED_CONTENT_TYPES:
            raise AppProductionException("INVALID_FILE_FORMAT", "Format image non supporté.", 415)
        object_key = f"datasets/{dataset_id}/uploads/{uuid.uuid4()}-{filename}"
        upload_url = await storage_provider.create_presigned_put(object_key, content_type)
        uploads.append(ExpertPresignedUpload(filename=filename, object_key=object_key, upload_url=upload_url))
    return ExpertPresignUploadResponse(uploads=uploads)


@app.post(
    "/api/v1/expert/datasets/{dataset_id}/finalize-import",
    response_model=ExpertDatasetResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_finalize_import(
    dataset_id: str,
    payload: ExpertFinalizeImportInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_dataset_admin(authorization, db)
    result = await db.execute(select(DatasetBatchModel).where(DatasetBatchModel.id == dataset_id))
    batch = result.scalar_one_or_none()
    if not batch:
        raise AppProductionException("NOT_FOUND", "Dataset introuvable.", 404)
    import_row = None
    if payload.import_id:
        import_row = await db.get(DatasetImportModel, payload.import_id)
        if not import_row or import_row.batch_id != dataset_id or not import_row.source_attested:
            raise AppProductionException("VALIDATION_ERROR", "Lot d'import dataset invalide.", 400)
    now = _dataset_now()
    created = 0
    for spec in payload.items:
        object_key = str(spec.get("object_key") or "").strip()
        if not object_key:
            continue
        expected_prefix = f"datasets/{dataset_id}/uploads/"
        if not object_key.startswith(expected_prefix):
            raise AppProductionException("VALIDATION_ERROR", "Clé d’import dataset invalide.", 400)
        content_type = str(spec.get("content_type") or "image/jpeg")
        if content_type not in DATASET_ALLOWED_CONTENT_TYPES:
            raise AppProductionException("INVALID_FILE_FORMAT", "Format image non supporté.", 415)
        source_type = str(spec.get("source_type") or (import_row.source_type if import_row else "other"))
        view_type = str(spec.get("view_type") or (import_row.view_type if import_row else "unknown"))
        evidence_tier = str(spec.get("evidence_tier") or (import_row.evidence_tier if import_row else "unresolved"))
        if _is_v2_dataset(batch) and (
            source_type not in V2_SOURCE_TYPES
            or view_type not in V2_VIEW_TYPES
            or evidence_tier not in V2_EVIDENCE_TIERS
        ):
            raise AppProductionException("VALIDATION_ERROR", "Métadonnées v2 d'import invalides.", 400)
        item = DatasetItemModel(
            id=str(uuid.uuid4()),
            batch_id=dataset_id,
            import_id=payload.import_id or None,
            specimen_id=spec.get("specimen_id") or None,
            original_filename=os.path.basename(str(spec.get("filename") or object_key)),
            content_type=content_type,
            original_object_key=object_key,
            status="inference_pending",
            duplicate_group_id=spec.get("specimen_id") or None,
            source_type=source_type,
            source_reference=spec.get("source_reference") or (import_row.source_reference if import_row else None),
            rights_status=str(spec.get("rights_status") or (import_row.rights_status if import_row else "attested")),
            evidence_tier=evidence_tier,
            view_type=view_type,
            historical_label=spec.get("historical_label"),
            item_metadata={
                "source_type": spec.get("source_type") or "unknown",
                "origin": spec.get("origin") or "unknown",
                "capture_type": spec.get("capture_type") or "unknown",
                "has_interior_cut": spec.get("has_interior_cut") or "unknown",
                "dataset_taxonomy_version": batch.taxonomy_version,
            },
            created_at=now,
            updated_at=now,
        )
        db.add(item)
        created += 1
    batch.statistics = {**(batch.statistics or {}), "imported_items": created}
    if import_row:
        import_row.status = "processing"
        import_row.updated_at = now
    batch.updated_at = now
    await db.commit()
    await db.refresh(batch)
    return _dataset_response(batch)


@app.get(
    "/api/v1/expert/datasets/{dataset_id}/stats",
    response_model=ExpertDatasetStatsResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_dataset_stats(
    dataset_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    batch_result = await db.execute(select(DatasetBatchModel).where(DatasetBatchModel.id == dataset_id))
    if not batch_result.scalar_one_or_none():
        raise AppProductionException("NOT_FOUND", "Dataset introuvable.", 404)
    result = await db.execute(select(DatasetItemModel).where(DatasetItemModel.batch_id == dataset_id))
    items = result.scalars().all()
    counts = Counter(item.status for item in items)
    quality_counts = Counter(
        "passed" if (item.quality_report or {}).get("passed") else "flagged"
        for item in items
        if item.quality_report is not None
    )
    consensus_result = await db.execute(
        select(DatasetConsensusModel).join(
            DatasetItemModel,
            DatasetConsensusModel.dataset_item_id == DatasetItemModel.id,
        ).where(DatasetItemModel.batch_id == dataset_id)
    )
    label_counts = Counter(row.final_label or "unlabeled" for row in consensus_result.scalars().all())
    audit_result = await db.execute(
        select(AuditRunModel.id)
        .where(AuditRunModel.batch_id == dataset_id)
        .order_by(AuditRunModel.created_at.desc())
        .limit(1)
    )
    return ExpertDatasetStatsResponse(
        dataset_id=dataset_id,
        counts=dict(counts),
        label_counts=dict(label_counts),
        quality_counts=dict(quality_counts),
        last_audit_id=audit_result.scalar_one_or_none(),
        dataset_version=dataset_id,
    )


@app.get(
    "/api/v1/expert/queue/next",
    response_model=Optional[ExpertQueueItemResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_queue_next(
    dataset_id: Optional[str] = Query(None),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_expert_context(authorization, db)
    now = _dataset_now()
    available_statuses = {"pending_annotation", "needs_review", "audit_pending", "inference_ready"}
    already_reviewed = (
        select(AnnotationEventModel.id)
        .where(
            AnnotationEventModel.dataset_item_id == DatasetItemModel.id,
            AnnotationEventModel.expert_id == user.id,
            AnnotationEventModel.action.in_(["label", "review"]),
        )
        .exists()
    )
    queue_priority = func.coalesce(
        DatasetItemModel.item_metadata["queue_priority"].astext.cast(Integer),
        99,
    )
    query = select(DatasetItemModel).where(
        DatasetItemModel.status.in_(available_statuses),
        or_(DatasetItemModel.lease_expires_at.is_(None), DatasetItemModel.lease_expires_at < now),
        or_(DatasetItemModel.status.notin_(["needs_review"]), ~already_reviewed),
        or_(DatasetItemModel.audit_not_before.is_(None), DatasetItemModel.audit_not_before <= now),
    )
    if dataset_id:
        query = query.where(DatasetItemModel.batch_id == dataset_id)
    query = query.order_by(
        case((DatasetItemModel.status == "needs_review", 0), else_=queue_priority),
        DatasetItemModel.created_at.asc(),
    ).limit(1).with_for_update(skip_locked=True)
    result = await db.execute(query)
    item = result.scalar_one_or_none()
    if not item:
        return None
    item.status = "audit_in_progress" if item.status == "audit_pending" else "in_progress"
    item.lease_user_id = user.id
    item.lease_expires_at = now + timedelta(minutes=DATASET_LEASE_MINUTES)
    item.updated_at = now
    await db.commit()
    await db.refresh(item)
    return await _dataset_item_response(item)


@app.get(
    "/api/v1/expert/items/{item_id}",
    response_model=ExpertQueueItemResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_get_item(
    item_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    item = await _dataset_item_for_id(item_id, db)
    return await _dataset_item_response(item)


async def _annotate_v2_item(
    *,
    item: DatasetItemModel,
    payload: ExpertAnnotationInput,
    user: UserModel,
    db: AsyncSession,
    now: datetime,
) -> ExpertAnnotationResponse:
    """One-expert blind annotation plus a delayed, blinded consistency audit."""
    audit_stage = item.status == "audit_in_progress"
    verdict = "unusable" if payload.action == "unusable" else payload.top_label
    evidence_tier = payload.evidence_tier or item.evidence_tier or "unresolved"
    if payload.action in {"label", "review"}:
        try:
            validate_v2_annotation(
                verdict,
                payload.meteorite_subclass,
                payload.terrestrial_family,
                evidence_tier,
            )
        except ValueError as exc:
            raise AppProductionException("VALIDATION_ERROR", str(exc), 400)
        if payload.confidence is None or payload.confidence == "not_assessed":
            raise AppProductionException("VALIDATION_ERROR", "La confiance de l'expert est obligatoire.", 400)

    event = AnnotationEventModel(
        id=str(uuid.uuid4()),
        dataset_item_id=item.id,
        expert_id=user.id,
        client_uuid=payload.client_uuid,
        action=payload.action,
        top_label=verdict,
        meteorite_subclass=payload.meteorite_subclass,
        terrestrial_family=payload.terrestrial_family,
        confidence=payload.confidence,
        visual_verdict=verdict,
        evidence_tier=evidence_tier,
        reason_codes=payload.reason_codes,
        requested_evidence=payload.requested_evidence,
        is_blind=True,
        comment=payload.comment,
        annotation_metadata={
            **payload.metadata,
            "specimen_id": payload.specimen_id,
            "review_stage": "audit" if audit_stage else "primary",
            "trio_prediction_visible": False,
        },
        policy_version=ANNOTATION_POLICY_V2_VERSION,
        created_at=now,
    )
    db.add(event)
    if payload.specimen_id is not None:
        item.specimen_id = payload.specimen_id or None
    if payload.metadata:
        item.item_metadata = {**(item.item_metadata or {}), **payload.metadata}
    item.item_metadata = {**(item.item_metadata or {}), "prediction_revealed": True}

    consensus = await db.get(DatasetConsensusModel, item.id)
    if not consensus:
        consensus = DatasetConsensusModel(dataset_item_id=item.id, status="pending")
        db.add(consensus)

    if payload.action == "skip":
        item.status = "skipped"
        consensus.status = "skipped"
        consensus.audit_status = "not_required"
        consensus.training_eligible = False
        consensus.training_role = "unresolved"
        review_required = False
    elif payload.action == "unusable":
        consensus.final_label = "unusable"
        consensus.final_confidence = "not_assessed"
        consensus.evidence_tier = evidence_tier
        consensus.status = "consensus_validated"
        consensus.audit_status = "not_required"
        consensus.training_role = "unusable"
        consensus.training_eligible = False
        consensus.subclass_eligible = False
        consensus.finalized_by = user.id
        consensus.finalized_at = now
        item.status = "consensus_validated"
        review_required = False
    elif audit_stage:
        primary_result = await db.execute(
            select(AnnotationEventModel)
            .where(
                AnnotationEventModel.dataset_item_id == item.id,
                AnnotationEventModel.id != event.id,
                AnnotationEventModel.action.in_(["label", "review"]),
            )
            .order_by(AnnotationEventModel.created_at.asc())
            .limit(1)
        )
        primary = primary_result.scalar_one_or_none()
        same_decision = bool(primary and (
            primary.visual_verdict or primary.top_label,
            primary.meteorite_subclass,
            primary.terrestrial_family,
        ) == (verdict, payload.meteorite_subclass, payload.terrestrial_family))
        if same_decision and payload.confidence == "high" and primary.confidence == "high":
            final_evidence = (
                "catalog_verified"
                if item.evidence_tier == "catalog_verified"
                else "field_expert_audited"
            )
            consensus.final_label = verdict
            consensus.meteorite_subclass = payload.meteorite_subclass
            consensus.terrestrial_family = payload.terrestrial_family
            consensus.final_confidence = "high"
            consensus.evidence_tier = final_evidence
            consensus.audit_status = "audited"
            consensus.status = "consensus_validated"
            consensus.finalized_by = user.id
            consensus.finalized_at = now
            consensus.subclass_eligible = bool(
                verdict == "meteorite_candidate"
                and payload.meteorite_subclass
                and final_evidence in {"catalog_verified", "field_expert_audited"}
            )
            consensus.training_role = v2_training_role(
                verdict=verdict,
                evidence_tier=final_evidence,
                confidence="high",
                quality_passed=bool((item.quality_report or {}).get("passed", True)),
                audit_status="audited",
                source_type=item.source_type,
            )
            consensus.training_eligible = consensus.training_role in {
                "gold", "field_strong", "weak_labels", "hard_negatives",
            }
            item.status = "consensus_validated"
            review_required = False
        else:
            consensus.status = "needs_review"
            consensus.audit_status = "conflict"
            consensus.training_eligible = False
            consensus.training_role = "unresolved"
            consensus.subclass_eligible = False
            item.status = "needs_review"
            review_required = True
    else:
        requires_audit = bool(
            payload.action == "review"
            or verdict in {"meteorite_candidate", "uncertain"}
            or (item.item_metadata or {}).get("historical_label_conflict")
            or _v2_audit_sample(item)
        )
        consensus.final_label = verdict
        consensus.meteorite_subclass = payload.meteorite_subclass
        consensus.terrestrial_family = payload.terrestrial_family
        consensus.final_confidence = payload.confidence
        consensus.evidence_tier = evidence_tier
        consensus.subclass_eligible = False
        if requires_audit:
            consensus.status = "audit_pending"
            consensus.audit_status = "pending"
            consensus.training_eligible = False
            consensus.training_role = "unresolved"
            item.status = "audit_pending"
            item.audit_not_before = now + timedelta(hours=V2_AUDIT_DELAY_HOURS)
            review_required = True
        else:
            consensus.status = "consensus_validated"
            consensus.audit_status = "not_required"
            consensus.training_role = v2_training_role(
                verdict=verdict,
                evidence_tier=evidence_tier,
                confidence=payload.confidence,
                quality_passed=bool((item.quality_report or {}).get("passed", True)),
                audit_status="not_required",
                source_type=item.source_type,
            )
            consensus.training_eligible = consensus.training_role == "weak_labels"
            consensus.finalized_by = user.id
            consensus.finalized_at = now
            item.status = "consensus_validated"
            review_required = False

    count_result = await db.execute(
        select(func.count(AnnotationEventModel.id)).where(AnnotationEventModel.dataset_item_id == item.id)
    )
    consensus.annotation_count = int(count_result.scalar_one() or 0)
    consensus.review_required = review_required
    consensus.updated_at = now
    item.lease_user_id = None
    item.lease_expires_at = None
    item.updated_at = now
    await db.commit()
    await db.refresh(item)
    return ExpertAnnotationResponse(
        item=await _dataset_item_response(item, consensus),
        annotation_id=event.id,
        consensus_status=consensus.status,
        review_required=review_required,
        next_item_available=True,
    )


@app.post(
    "/api/v1/expert/items/{item_id}/annotation",
    response_model=ExpertAnnotationResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_annotate_item(
    item_id: str,
    payload: ExpertAnnotationInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_expert_context(authorization, db)
    item = await _dataset_item_for_id(item_id, db)
    now = _dataset_now()
    if item.lease_user_id not in {None, user.id} and item.lease_expires_at and item.lease_expires_at > now:
        raise AppProductionException("CONFLICT", "Cette image est verrouillée par un autre expert.", 409)
    duplicate_result = await db.execute(
        select(AnnotationEventModel).where(AnnotationEventModel.client_uuid == payload.client_uuid)
    )
    duplicate = duplicate_result.scalar_one_or_none()
    if duplicate:
        return ExpertAnnotationResponse(
            item=await _dataset_item_response(item),
            annotation_id=duplicate.id,
            consensus_status="duplicate",
            review_required=True,
            next_item_available=True,
        )

    if _is_v2_item(item):
        return await _annotate_v2_item(item=item, payload=payload, user=user, db=db, now=now)

    if payload.action in {"label", "review"}:
        try:
            validate_annotation(payload.top_label, payload.meteorite_subclass, payload.terrestrial_family)
        except ValueError as exc:
            raise AppProductionException("VALIDATION_ERROR", str(exc), 400)
        if payload.confidence is None:
            raise AppProductionException("VALIDATION_ERROR", "La confiance de l'expert est obligatoire.", 400)

    event = AnnotationEventModel(
        id=str(uuid.uuid4()),
        dataset_item_id=item.id,
        expert_id=user.id,
        client_uuid=payload.client_uuid,
        action=payload.action,
        top_label=payload.top_label,
        meteorite_subclass=payload.meteorite_subclass,
        terrestrial_family=payload.terrestrial_family,
        confidence=payload.confidence,
        comment=payload.comment,
        annotation_metadata={
            **payload.metadata,
            "specimen_id": payload.specimen_id,
        },
        policy_version=ANNOTATION_POLICY_VERSION,
        created_at=now,
    )
    db.add(event)

    if payload.specimen_id is not None:
        item.specimen_id = payload.specimen_id or None
    if payload.metadata:
        item.item_metadata = {**(item.item_metadata or {}), **payload.metadata}

    consensus_result = await db.execute(
        select(DatasetConsensusModel).where(DatasetConsensusModel.dataset_item_id == item.id)
    )
    consensus = consensus_result.scalar_one_or_none()
    if not consensus:
        consensus = DatasetConsensusModel(dataset_item_id=item.id, status="pending")
        db.add(consensus)

    annotation_result = await db.execute(
        select(AnnotationEventModel).where(
            AnnotationEventModel.dataset_item_id == item.id,
            AnnotationEventModel.action.in_(["label", "review"]),
        )
    )
    annotations = annotation_result.scalars().all()
    if not any(annotation.id == event.id for annotation in annotations):
        annotations.append(event)
    review_required = (
        payload.action in {"skip", "review"}
        or payload.top_label in {"meteorite", "uncertain"}
        or payload.confidence != "high"
        or bool((item.quality_report or {}).get("issues"))
        or _second_review_sample(item)
    )
    consensus.training_eligible = False
    consensus.final_confidence = None
    if len(annotations) >= 2:
        signatures = Counter(
            (
                annotation.top_label,
                annotation.meteorite_subclass,
                annotation.terrestrial_family,
            )
            for annotation in annotations
            if annotation.confidence == "high"
        )
        winning_signature, votes = signatures.most_common(1)[0] if signatures else (None, 0)
        if winning_signature and votes >= 2:
            final_label, meteorite_subclass, terrestrial_family = winning_signature
            consensus.final_label = final_label
            consensus.meteorite_subclass = meteorite_subclass
            consensus.terrestrial_family = terrestrial_family
            consensus.status = "consensus_validated"
            consensus.finalized_by = user.id
            consensus.finalized_at = now
            consensus.final_confidence = "high"
            consensus.training_eligible = bool(
                final_label not in {"uncertain", "unusable"}
                and not (item.quality_report or {}).get("issues")
            )
            review_required = False
        else:
            consensus.status = "needs_review"
            consensus.final_label = None
            consensus.meteorite_subclass = None
            consensus.terrestrial_family = None
            review_required = True
    elif not review_required and payload.action == "label" and payload.confidence == "high":
        consensus.final_label = payload.top_label
        consensus.meteorite_subclass = payload.meteorite_subclass
        consensus.terrestrial_family = payload.terrestrial_family
        consensus.status = "consensus_validated"
        consensus.finalized_by = user.id
        consensus.finalized_at = now
        consensus.final_confidence = "high"
        consensus.training_eligible = bool(
            payload.top_label not in {"uncertain", "unusable"}
            and not (item.quality_report or {}).get("issues")
        )
    elif review_required:
        consensus.status = "needs_review"
        consensus.final_label = None
        consensus.meteorite_subclass = None
        consensus.terrestrial_family = None

    if payload.action == "skip":
        item.status = "skipped"
        consensus.status = "skipped"
        consensus.training_eligible = False
    elif payload.action == "unusable":
        item.status = "unusable"
        consensus.final_label = "unusable"
        consensus.status = "consensus_validated"
        consensus.finalized_by = user.id
        consensus.finalized_at = now
        consensus.final_confidence = "not_assessed"
        consensus.training_eligible = False
    elif consensus.status == "consensus_validated":
        item.status = "consensus_validated"
    else:
        item.status = "needs_review" if review_required else "annotated"

    consensus.review_required = review_required
    consensus.annotation_count = len(annotations)
    consensus.updated_at = now
    item.lease_user_id = None
    item.lease_expires_at = None
    item.updated_at = now
    await db.commit()
    await db.refresh(item)
    return ExpertAnnotationResponse(
        item=await _dataset_item_response(item),
        annotation_id=event.id,
        consensus_status=consensus.status,
        review_required=review_required,
        next_item_available=True,
    )


@app.post(
    "/api/v1/expert/items/{item_id}/adjudicate",
    response_model=ExpertAnnotationResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_adjudicate_item(
    item_id: str,
    payload: ExpertAdjudicationInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_dataset_admin(authorization, db)
    item = await _dataset_item_for_id(item_id, db)
    duplicate_result = await db.execute(
        select(AnnotationEventModel).where(AnnotationEventModel.client_uuid == payload.client_uuid)
    )
    duplicate = duplicate_result.scalar_one_or_none()
    if duplicate:
        if duplicate.dataset_item_id != item.id:
            raise AppProductionException("CONFLICT", "Cette décision a déjà été utilisée pour une autre image.", 409)
        consensus_result = await db.execute(
            select(DatasetConsensusModel).where(DatasetConsensusModel.dataset_item_id == item.id)
        )
        consensus = consensus_result.scalar_one_or_none()
        return ExpertAnnotationResponse(
            item=await _dataset_item_response(item, consensus),
            annotation_id=duplicate.id,
            consensus_status=consensus.status if consensus else "duplicate",
            review_required=consensus.review_required if consensus else True,
            next_item_available=True,
        )
    if _is_v2_item(item):
        try:
            validate_v2_annotation(
                payload.top_label,
                payload.meteorite_subclass,
                payload.terrestrial_family,
                item.evidence_tier or "unresolved",
            )
        except ValueError as exc:
            raise AppProductionException("VALIDATION_ERROR", str(exc), 400)
        now = _dataset_now()
        consensus = await db.get(DatasetConsensusModel, item.id)
        if not consensus:
            consensus = DatasetConsensusModel(dataset_item_id=item.id, status="pending")
            db.add(consensus)
        final_evidence = "catalog_verified" if item.evidence_tier == "catalog_verified" else "field_expert_audited"
        event = AnnotationEventModel(
            id=str(uuid.uuid4()), dataset_item_id=item.id, expert_id=user.id,
            client_uuid=payload.client_uuid, action="adjudicate", top_label=payload.top_label,
            meteorite_subclass=payload.meteorite_subclass, terrestrial_family=payload.terrestrial_family,
            confidence="high", visual_verdict=payload.top_label, evidence_tier=final_evidence,
            reason_codes=["admin_arbitration"], requested_evidence=[], is_blind=False,
            comment=payload.comment,
            annotation_metadata={**payload.metadata, "specimen_id": payload.specimen_id, "adjudicated": True},
            policy_version=ANNOTATION_POLICY_V2_VERSION, created_at=now,
        )
        db.add(event)
        consensus.final_label = payload.top_label
        consensus.meteorite_subclass = payload.meteorite_subclass
        consensus.terrestrial_family = payload.terrestrial_family
        consensus.final_confidence = "high"
        consensus.evidence_tier = final_evidence
        consensus.audit_status = "arbitrated"
        consensus.status = "consensus_validated"
        consensus.review_required = False
        consensus.training_role = v2_training_role(
            verdict=payload.top_label, evidence_tier=final_evidence, confidence="high",
            quality_passed=bool((item.quality_report or {}).get("passed", True)),
            audit_status="audited", source_type=item.source_type,
        )
        consensus.training_eligible = consensus.training_role in {"gold", "field_strong", "weak_labels", "hard_negatives"}
        consensus.subclass_eligible = bool(
            payload.top_label == "meteorite_candidate" and payload.meteorite_subclass
            and final_evidence in {"catalog_verified", "field_expert_audited"}
        )
        consensus.finalized_by = user.id
        consensus.finalized_at = now
        consensus.updated_at = now
        item.status = "consensus_validated"
        item.lease_user_id = None
        item.lease_expires_at = None
        item.updated_at = now
        await _write_audit_log(
            db, actor_user_id=user.id, action="expert_v2_annotation_arbitrated",
            entity_type="dataset_item", entity_id=item.id,
            metadata={"dataset_id": item.batch_id, "final_label": payload.top_label},
        )
        await db.commit()
        await db.refresh(item)
        return ExpertAnnotationResponse(
            item=await _dataset_item_response(item, consensus), annotation_id=event.id,
            consensus_status=consensus.status, review_required=False, next_item_available=True,
        )
    try:
        validate_annotation(payload.top_label, payload.meteorite_subclass, payload.terrestrial_family)
    except ValueError as exc:
        raise AppProductionException("VALIDATION_ERROR", str(exc), 400)
    now = _dataset_now()
    consensus_result = await db.execute(
        select(DatasetConsensusModel).where(DatasetConsensusModel.dataset_item_id == item.id)
    )
    consensus = consensus_result.scalar_one_or_none()
    if not consensus:
        consensus = DatasetConsensusModel(dataset_item_id=item.id, status="pending")
        db.add(consensus)
    event = AnnotationEventModel(
        id=str(uuid.uuid4()),
        dataset_item_id=item.id,
        expert_id=user.id,
        client_uuid=payload.client_uuid,
        action="adjudicate",
        top_label=payload.top_label,
        meteorite_subclass=payload.meteorite_subclass,
        terrestrial_family=payload.terrestrial_family,
        confidence="high",
        comment=payload.comment,
        annotation_metadata={**payload.metadata, "specimen_id": payload.specimen_id, "adjudicated": True},
        policy_version=ANNOTATION_POLICY_VERSION,
        created_at=now,
    )
    db.add(event)
    if payload.specimen_id is not None:
        item.specimen_id = payload.specimen_id or None
    if payload.metadata:
        item.item_metadata = {**(item.item_metadata or {}), **payload.metadata}
    consensus.final_label = payload.top_label
    consensus.meteorite_subclass = payload.meteorite_subclass
    consensus.terrestrial_family = payload.terrestrial_family
    consensus.final_confidence = "high"
    consensus.status = "consensus_validated"
    consensus.review_required = False
    consensus.training_eligible = bool(
        payload.top_label not in {"uncertain", "unusable"}
        and not (item.quality_report or {}).get("issues")
    )
    consensus.finalized_by = user.id
    consensus.finalized_at = now
    consensus.updated_at = now
    item.status = "consensus_validated"
    item.lease_user_id = None
    item.lease_expires_at = None
    item.updated_at = now
    await _write_audit_log(
        db,
        actor_user_id=user.id,
        action="expert_annotation_adjudicated",
        entity_type="dataset_item",
        entity_id=item.id,
        metadata={"dataset_id": item.batch_id, "final_label": payload.top_label},
    )
    await db.commit()
    await db.refresh(item)
    return ExpertAnnotationResponse(
        item=await _dataset_item_response(item, consensus),
        annotation_id=event.id,
        consensus_status=consensus.status,
        review_required=False,
        next_item_available=True,
    )


@app.post(
    "/api/v1/expert/items/{item_id}/release",
    response_model=ExpertQueueItemResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_release_item(
    item_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_expert_context(authorization, db)
    item = await _dataset_item_for_id(item_id, db)
    if item.lease_user_id not in {None, user.id} and user.role != "admin":
        raise AppProductionException("FORBIDDEN", "Cette image appartient à un autre expert.", 403)
    item.lease_user_id = None
    item.lease_expires_at = None
    if item.status == "audit_in_progress":
        item.status = "audit_pending"
    elif item.status == "in_progress":
        item.status = "needs_review" if item.raw_prediction else "inference_pending"
    item.updated_at = _dataset_now()
    await db.commit()
    await db.refresh(item)
    return await _dataset_item_response(item)


@app.post(
    "/api/v1/expert/audits",
    response_model=ExpertAuditResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def expert_create_audit(
    payload: ExpertAuditCreateInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_dataset_admin(authorization, db)
    result = await db.execute(select(DatasetBatchModel).where(DatasetBatchModel.id == payload.dataset_id))
    batch = result.scalar_one_or_none()
    if not batch:
        raise AppProductionException("NOT_FOUND", "Dataset introuvable.", 404)
    if _is_v2_dataset(batch):
        return await _create_v2_audit(batch=batch, user=user, db=db)
    rows_result = await db.execute(
        select(DatasetItemModel, DatasetConsensusModel)
        .join(DatasetConsensusModel, DatasetConsensusModel.dataset_item_id == DatasetItemModel.id)
        .where(
            DatasetItemModel.batch_id == payload.dataset_id,
            DatasetConsensusModel.status == "consensus_validated",
            DatasetItemModel.raw_prediction.is_not(None),
        )
    )
    rows = rows_result.all()
    summary, errors, recommendations = build_audit(rows)
    pending_review_result = await db.execute(
        select(func.count())
        .select_from(DatasetConsensusModel)
        .join(DatasetItemModel, DatasetConsensusModel.dataset_item_id == DatasetItemModel.id)
        .where(
            DatasetItemModel.batch_id == payload.dataset_id,
            DatasetConsensusModel.status == "needs_review",
        )
    )
    reviewed_rows = [consensus for _item, consensus in rows if consensus.annotation_count >= 2]
    summary["human_review"] = {
        "validated": sum(1 for _item, consensus in rows if consensus.status == "consensus_validated"),
        "training_eligible": sum(1 for _item, consensus in rows if consensus.training_eligible),
        "double_reviewed": len(reviewed_rows),
        "agreement_rate": round(
            sum(1 for consensus in reviewed_rows if consensus.status == "consensus_validated") / len(reviewed_rows),
            6,
        ) if reviewed_rows else None,
        "needs_adjudication": int(pending_review_result.scalar_one() or 0),
    }
    audit_id = str(uuid.uuid4())
    report_key = f"datasets/{payload.dataset_id}/reports/{audit_id}/summary.html"
    errors_key = f"datasets/{payload.dataset_id}/reports/{audit_id}/errors.csv"
    report_html = render_audit_html(summary, recommendations).encode("utf-8")
    error_buffer = io.StringIO()
    writer = csv.DictWriter(error_buffer, fieldnames=["item_id", "error_type", "score", "actual"])
    writer.writeheader()
    writer.writerows(errors)
    await storage_provider.save_object(report_html, report_key, "text/html")
    await storage_provider.save_object(error_buffer.getvalue().encode("utf-8"), errors_key, "text/csv")
    now = _dataset_now()
    audit = AuditRunModel(
        id=audit_id,
        batch_id=payload.dataset_id,
        created_by=user.id,
        status="completed",
        model_version=payload.model_version,
        summary=summary,
        recommendations=recommendations,
        report_object_key=report_key,
        errors_object_key=errors_key,
        created_at=now,
        completed_at=now,
    )
    db.add(audit)
    await db.commit()
    return ExpertAuditResponse(
        id=audit.id,
        dataset_id=audit.batch_id,
        status=audit.status,
        model_version=audit.model_version,
        summary=audit.summary or {},
        recommendations=audit.recommendations or [],
        report_url=await _dataset_object_url(audit.report_object_key),
        errors_url=await _dataset_object_url(audit.errors_object_key),
        created_at=audit.created_at.isoformat(),
        completed_at=audit.completed_at.isoformat() if audit.completed_at else None,
    )


@app.get(
    "/api/v1/expert/audits",
    response_model=List[ExpertAuditResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_list_audits(
    dataset_id: Optional[str] = Query(None),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    query = select(AuditRunModel).order_by(AuditRunModel.created_at.desc())
    if dataset_id:
        query = query.where(AuditRunModel.batch_id == dataset_id)
    result = await db.execute(query)
    return [
        ExpertAuditResponse(
            id=audit.id,
            dataset_id=audit.batch_id,
            status=audit.status,
            model_version=audit.model_version,
            summary=audit.summary or {},
            recommendations=audit.recommendations or [],
            report_url=await _dataset_object_url(audit.report_object_key),
            errors_url=await _dataset_object_url(audit.errors_object_key),
            created_at=audit.created_at.isoformat(),
            completed_at=audit.completed_at.isoformat() if audit.completed_at else None,
        )
        for audit in result.scalars().all()
    ]


@app.get(
    "/api/v1/expert/audits/{audit_id}",
    response_model=ExpertAuditResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_get_audit(
    audit_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    result = await db.execute(select(AuditRunModel).where(AuditRunModel.id == audit_id))
    audit = result.scalar_one_or_none()
    if not audit:
        raise AppProductionException("NOT_FOUND", "Audit introuvable.", 404)
    return ExpertAuditResponse(
        id=audit.id,
        dataset_id=audit.batch_id,
        status=audit.status,
        model_version=audit.model_version,
        summary=audit.summary or {},
        recommendations=audit.recommendations or [],
        report_url=await _dataset_object_url(audit.report_object_key),
        errors_url=await _dataset_object_url(audit.errors_object_key),
        created_at=audit.created_at.isoformat(),
        completed_at=audit.completed_at.isoformat() if audit.completed_at else None,
    )


@app.get(
    "/api/v1/expert/audits/{audit_id}/download",
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_download_audit(
    audit_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    result = await db.execute(select(AuditRunModel).where(AuditRunModel.id == audit_id))
    audit = result.scalar_one_or_none()
    if not audit:
        raise AppProductionException("NOT_FOUND", "Audit introuvable.", 404)
    return {
        "audit_id": audit.id,
        "summary_url": await _dataset_object_url(audit.report_object_key),
        "errors_url": await _dataset_object_url(audit.errors_object_key),
        "model_version": audit.model_version,
        "taxonomy_version": TAXONOMY_VERSION,
        "dataset_version": audit.batch_id,
    }


async def _create_v2_export_snapshot(
    *,
    batch: DatasetBatchModel,
    version: str,
    user: UserModel,
    db: AsyncSession,
) -> ExpertExportResponse:
    """Create a self-contained, immutable v2 training snapshot outside Git."""
    rows_result = await db.execute(
        select(DatasetItemModel, DatasetConsensusModel)
        .outerjoin(DatasetConsensusModel, DatasetConsensusModel.dataset_item_id == DatasetItemModel.id)
        .where(DatasetItemModel.batch_id == batch.id)
        .order_by(DatasetItemModel.created_at.asc())
    )
    rows = rows_result.all()
    if not rows:
        raise AppProductionException("CONFLICT", "Le dataset v2 ne contient aucune image.", 409)
    events_result = await db.execute(
        select(AnnotationEventModel)
        .join(DatasetItemModel, AnnotationEventModel.dataset_item_id == DatasetItemModel.id)
        .where(DatasetItemModel.batch_id == batch.id)
        .order_by(AnnotationEventModel.created_at.asc())
    )
    events_by_item: dict[str, list[AnnotationEventModel]] = {}
    for event in events_result.scalars().all():
        events_by_item.setdefault(event.dataset_item_id, []).append(event)

    entries: list[dict] = []
    specimens: dict[str, dict] = {}
    role_counts: Counter = Counter()
    duplicate_groups: dict[str, list[str]] = {}
    group_labels: dict[str, str] = {}
    for item, consensus in rows:
        role = (consensus.training_role if consensus else None) or (
            "unusable" if item.status == "unusable" else "unresolved"
        )
        if role not in V2_TRAINING_ROLES:
            role = "unresolved"
        group_id = item.duplicate_group_id or item.specimen_id or item.perceptual_hash or item.id
        verdict = consensus.final_label if consensus else None
        split_eligible = role in {"gold", "hard_negatives"} and verdict in {"meteorite_candidate", "terrestrial"}
        if split_eligible:
            group_labels.setdefault(group_id, "candidate" if verdict == "meteorite_candidate" else "terrestrial")
        entry = {
            "image_id": item.id,
            "specimen_id": item.specimen_id,
            "group_id": group_id,
            "original_filename": item.original_filename,
            "source_sha256": item.sha256,
            "normalized_sha256": None,
            "source_type": item.source_type,
            "source_reference": item.source_reference,
            "rights_status": item.rights_status,
            "view_type": item.view_type,
            "quality_report": item.quality_report or {},
            "human_verdict": verdict,
            "meteorite_subclass": consensus.meteorite_subclass if consensus else None,
            "terrestrial_family": consensus.terrestrial_family if consensus else None,
            "human_confidence": consensus.final_confidence if consensus else None,
            "evidence_tier": consensus.evidence_tier if consensus and consensus.evidence_tier else item.evidence_tier,
            "audit_status": consensus.audit_status if consensus else "pending",
            "consensus_status": consensus.status if consensus else "unlabeled",
            "training_role": role,
            "subclass_eligible": bool(consensus.subclass_eligible) if consensus else False,
            "historical_label": item.historical_label,
            "vision_trio_raw": item.raw_prediction or {},
            "vision_trio_model_version": item.model_version,
            "training_tasks": {
                "image_quality": role not in {"unresolved", "unusable"},
                "candidate_binary": role in {"gold", "field_strong", "weak_labels", "hard_negatives"},
                "terrestrial_family": bool(consensus and consensus.terrestrial_family and role in {"gold", "field_strong", "hard_negatives"}),
                "meteorite_subclass": bool(consensus and consensus.subclass_eligible),
            },
        }
        entries.append(entry)
        role_counts[role] += 1
        duplicate_groups.setdefault(group_id, []).append(item.id)
        specimen_key = item.specimen_id or group_id
        specimen = specimens.setdefault(specimen_key, {
            "specimen_id": item.specimen_id,
            "group_id": group_id,
            "image_ids": [],
            "source_types": set(),
            "views": set(),
        })
        specimen["image_ids"].append(item.id)
        specimen["source_types"].add(item.source_type)
        specimen["views"].add(item.view_type)

    group_splits = v2_stratified_group_split(group_labels)
    split_rows: dict[str, list[dict]] = {"train": [], "validation": [], "test": []}
    for entry in entries:
        role = entry["training_role"]
        if role in {"gold", "hard_negatives"} and entry["group_id"] in group_splits:
            split = group_splits[entry["group_id"]]
            entry["split"] = split
            entry["image_path"] = f"images/{split}/{entry['image_id']}.jpg"
            split_rows[split].append(entry)
        elif role in {"field_strong", "weak_labels"}:
            entry["split"] = "train_only"
            entry["image_path"] = f"images/train_only/{role}/{entry['image_id']}.jpg"
        else:
            entry["split"] = "excluded"
            entry["image_path"] = f"images/excluded/{role}/{entry['image_id']}.jpg"

    annotations = []
    for item_id, item_events in events_by_item.items():
        for event in item_events:
            annotations.append({
                "annotation_id": event.id,
                "image_id": item_id,
                "author_id": event.expert_id,
                "created_at": event.created_at.isoformat() if event.created_at else None,
                "action": event.action,
                "visual_verdict": event.visual_verdict or event.top_label,
                "meteorite_subclass": event.meteorite_subclass,
                "terrestrial_family": event.terrestrial_family,
                "confidence": event.confidence,
                "evidence_tier": event.evidence_tier,
                "reason_codes": event.reason_codes or [],
                "requested_evidence": event.requested_evidence or [],
                "is_blind": event.is_blind,
                "policy_version": event.policy_version,
            })

    for specimen in specimens.values():
        specimen["source_types"] = sorted(specimen["source_types"])
        specimen["views"] = sorted(specimen["views"])
    split_counts = {split: len(rows) for split, rows in split_rows.items()}
    supervised_count = sum(split_counts.values())
    duplicate_report = {
        "groups_with_multiple_images": [
            {"group_id": group_id, "image_ids": item_ids}
            for group_id, item_ids in duplicate_groups.items() if len(item_ids) > 1
        ],
        "policy": "All images in a specimen, exact-duplicate, or perceptual-duplicate group share one split.",
    }
    preparation_report = {
        "dataset_contract": VISION_V2_DATASET_CONTRACT_VERSION,
        "taxonomy_version": TAXONOMY_V2_VERSION,
        "annotation_policy_version": ANNOTATION_POLICY_V2_VERSION,
        "total_images": len(entries),
        "roles": dict(role_counts),
        "supervised_splits": split_counts,
        "validation_test_reference_only": True,
        "weak_labels_train_only": True,
        "subclass_minimums": {"train_verified_specimens": 50, "test_groups": 10},
    }
    model_contract = {
        "version": VISION_V2_RELEASE_CONTRACT_VERSION,
        "dataset_contract": VISION_V2_DATASET_CONTRACT_VERSION,
        "models": ["dinov2_vits14", "swinv2_tiny", "convnextv2_tiny"],
        "tasks": ["image_quality", "meteorite_candidate_binary", "terrestrial_family", "meteorite_subclass"],
        "runtime": {
            "required_views": 3,
            "folder_score": "median_calibrated_probability",
            "force_uncertain": ["low_quality", "out_of_distribution", "trio_disagreement", "view_conflict", "insufficient_views"],
        },
        "publication_gate": {
            "test_false_positive_rate_max": 0.01,
            "test_false_positive_upper_95_max": 0.01,
            "minimum_coherent_views": 2,
            "disclaimer": "Tri visuel Tissint, non certifie par laboratoire.",
        },
    }
    readiness = {
        "supervised_images": supervised_count,
        "frozen_test_images": split_counts["test"],
        "release_ready": False,
        "reason": "A release requires a separately evaluated model release.json; this snapshot alone cannot authorise publication.",
    }
    statistics = {
        "total": len(entries), "supervised_total": supervised_count,
        **split_counts, **{f"role_{key}": value for key, value in role_counts.items()},
        "split_policy": "stratified-grouped-deterministic-70-15-15",
        "contract": VISION_V2_DATASET_CONTRACT_VERSION,
    }

    archive_buffer = io.BytesIO()
    checksums: list[str] = []
    export_errors: list[dict] = []
    with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item, entry in zip((row[0] for row in rows), entries):
            try:
                image_bytes = await storage_provider.get_object(item.normalized_object_key or item.original_object_key)
                normalized_hash = hashlib.sha256(image_bytes).hexdigest()
                entry["normalized_sha256"] = normalized_hash
                archive.writestr(entry["image_path"], image_bytes)
                checksums.append(f"{normalized_hash}  {entry['image_path']}")
            except Exception as exc:
                entry["export_error"] = str(exc)[:300]
                export_errors.append({"image_id": entry["image_id"], "error": entry["export_error"]})
        archive.writestr("manifests/images.jsonl", "\n".join(json.dumps(row, ensure_ascii=False) for row in entries) + "\n")
        archive.writestr("manifests/specimens.jsonl", "\n".join(json.dumps(row, ensure_ascii=False) for row in specimens.values()) + "\n")
        archive.writestr("manifests/annotations.jsonl", "\n".join(json.dumps(row, ensure_ascii=False) for row in annotations) + "\n")
        for split, split_entries in split_rows.items():
            archive.writestr(f"splits/{split}.jsonl", "\n".join(json.dumps(row, ensure_ascii=False) for row in split_entries) + "\n")
        csv_buffer = io.StringIO()
        fieldnames = ["image_id", "specimen_id", "group_id", "image_path", "split", "training_role", "human_verdict", "meteorite_subclass", "terrestrial_family", "human_confidence", "evidence_tier", "audit_status", "source_sha256", "normalized_sha256"]
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(entries)
        archive.writestr("manifests/images.csv", csv_buffer.getvalue())
        archive.writestr("taxonomy.json", json.dumps({
            "version": TAXONOMY_V2_VERSION, "primary_verdicts": sorted(V2_PRIMARY_VERDICTS),
            "evidence_tiers": sorted(V2_EVIDENCE_TIERS), "meteorite_subclasses": sorted(METEORITE_SUBCLASSES),
            "terrestrial_families": sorted(TERRESTRIAL_FAMILIES),
        }, ensure_ascii=False, indent=2))
        archive.writestr("model-contract.json", json.dumps(model_contract, ensure_ascii=False, indent=2))
        archive.writestr("checksums.sha256", "\n".join(checksums) + "\n")
        archive.writestr("reports/preparation.json", json.dumps(preparation_report, ensure_ascii=False, indent=2))
        archive.writestr("reports/duplicates.json", json.dumps(duplicate_report, ensure_ascii=False, indent=2))
        archive.writestr("reports/export-errors.json", json.dumps(export_errors, ensure_ascii=False, indent=2))
        archive.writestr("reports/release-readiness.json", json.dumps(readiness, ensure_ascii=False, indent=2))
        archive.writestr("dataset-card.md", "\n".join([
            f"# {batch.name}", "", f"Snapshot: trio-finetune-v2-{version}",
            f"Created at: {_dataset_now().isoformat()} UTC", f"Created by: {user.id}",
            "", "This dataset supports visual triage, not laboratory certification.",
            "Validation and test contain only audited catalogue references and verified hard negatives.",
            "Weak Facebook labels are training-only and can never enter validation, test, or subclass supervision.",
            "Publication is disabled until a model release demonstrates FPR <= 1% and a 95% upper bound <= 1% on the frozen test set.",
        ]))
    archive_hash = hashlib.sha256(archive_buffer.getvalue()).hexdigest()
    statistics["archive_sha256"] = archive_hash
    export_id = str(uuid.uuid4())
    base_key = f"datasets/{batch.id}/exports/trio-finetune-v2-{version}"
    manifest_key = f"{base_key}/manifests/images.jsonl"
    await storage_provider.save_object(
        ("\n".join(json.dumps(row, ensure_ascii=False) for row in entries) + "\n").encode("utf-8"),
        manifest_key, "application/jsonl",
    )
    archive_key = f"{base_key}/trio-finetune-v2-{version}.zip"
    await storage_provider.save_object(archive_buffer.getvalue(), archive_key, "application/zip")
    export = DatasetExportModel(
        id=export_id, batch_id=batch.id, version=f"trio-finetune-v2-{version}", status="completed",
        created_by=user.id, manifest_object_key=manifest_key, archive_object_key=archive_key,
        statistics=statistics, created_at=_dataset_now(),
    )
    db.add(export)
    await _write_audit_log(
        db, actor_user_id=user.id, action="expert_v2_dataset_snapshot_created",
        entity_type="dataset_export", entity_id=export.id,
        metadata={"dataset_id": batch.id, "version": export.version, "archive_sha256": archive_hash, "statistics": statistics},
    )
    await db.commit()
    return ExpertExportResponse(
        id=export.id, dataset_id=batch.id, version=export.version, status=export.status,
        statistics=statistics, manifest_url=await _dataset_object_url(manifest_key),
        download_url=await _dataset_object_url(archive_key), created_at=export.created_at.isoformat(),
    )


@app.post(
    "/api/v1/expert/exports",
    response_model=ExpertExportResponse,
    status_code=status.HTTP_201_CREATED,
    responses=ERROR_RESPONSES,
)
async def expert_create_export(
    payload: ExpertExportCreateInput,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    user, _subscription = await _require_dataset_admin(authorization, db)
    batch_result = await db.execute(select(DatasetBatchModel).where(DatasetBatchModel.id == payload.dataset_id))
    batch = batch_result.scalar_one_or_none()
    if not batch:
        raise AppProductionException("NOT_FOUND", "Dataset introuvable.", 404)
    if _is_v2_dataset(batch):
        version = payload.version or _dataset_now().strftime("%Y%m%d%H%M%S")
        return await _create_v2_export_snapshot(batch=batch, version=version, user=user, db=db)
    result = await db.execute(
        select(DatasetItemModel, DatasetConsensusModel)
        .join(DatasetConsensusModel, DatasetConsensusModel.dataset_item_id == DatasetItemModel.id)
        .where(
            DatasetItemModel.batch_id == payload.dataset_id,
            DatasetConsensusModel.status == "consensus_validated",
        )
        .order_by(DatasetItemModel.created_at.asc())
    )
    rows = result.all()
    if not rows:
        raise AppProductionException("CONFLICT", "Aucune annotation validée à exporter.", 409)

    version = payload.version or f"v{_dataset_now().strftime('%Y%m%d%H%M%S')}"
    manifests: dict[str, list[dict]] = {"train": [], "validation": [], "test": []}
    excluded: dict[str, list[dict]] = {
        "hard_cases": [],
        "unlabeled": [],
        "not_training_eligible": [],
    }
    export_images: list[tuple[str, DatasetItemModel, dict]] = []
    for item, consensus in rows:
        group_id = (
            item.specimen_id
            or str((item.item_metadata or {}).get("perceptual_duplicate_of") or item.id)
        )
        entry = {
            "image_id": item.id,
            "group_id": group_id,
            "specimen_id": item.specimen_id,
            "source_sha256": item.sha256,
            "top_label": consensus.final_label,
            "meteorite_subclass": consensus.meteorite_subclass,
            "terrestrial_family": consensus.terrestrial_family,
            "human_confidence": consensus.final_confidence,
            "quality_report": item.quality_report or {},
            "consensus_status": consensus.status,
            "training_eligible": consensus.training_eligible,
            "model_version": item.model_version,
            "vision_trio": item.raw_prediction or {},
            "taxonomy_version": batch.taxonomy_version,
            "annotation_policy_version": batch.annotation_policy_version,
        }
        if consensus.final_label in {"uncertain", "unusable"}:
            entry["split"] = "unlabeled"
            excluded["unlabeled"].append(entry)
            continue
        if not (item.quality_report or {}).get("passed", True):
            entry["split"] = "hard_cases"
            excluded["hard_cases"].append(entry)
            continue
        if not consensus.training_eligible:
            entry["split"] = "not_training_eligible"
            excluded["not_training_eligible"].append(entry)
            continue
        split = deterministic_group_split(group_id)
        entry["split"] = split
        entry["image_path"] = f"images/{split}/{item.id}.jpg"
        manifests[split].append(entry)
        export_images.append((split, item, entry))

    supervised_manifest = [entry for split in ("train", "validation", "test") for entry in manifests[split]]
    if not supervised_manifest:
        raise AppProductionException("CONFLICT", "Aucune image validée à haute confiance à exporter.", 409)
    manifest = [*supervised_manifest, *excluded["hard_cases"], *excluded["unlabeled"], *excluded["not_training_eligible"]]
    statistics = {
        "total": len(manifest),
        "supervised_total": len(supervised_manifest),
        **{split: len(entries) for split, entries in manifests.items()},
        **{split: len(entries) for split, entries in excluded.items()},
        "split_policy": "grouped-deterministic-70-15-15",
    }
    export_id = str(uuid.uuid4())
    base_key = f"datasets/{payload.dataset_id}/exports/{version}"
    manifest_content = ("\n".join(json.dumps(entry, ensure_ascii=False) for entry in manifest) + "\n").encode("utf-8")
    await storage_provider.save_object(manifest_content, f"{base_key}/manifest.jsonl", "application/jsonl")
    for split, entries in manifests.items():
        await storage_provider.save_object(
            ("\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries) + "\n").encode("utf-8"),
            f"{base_key}/{split}.jsonl",
            "application/jsonl",
        )
    for split, entries in excluded.items():
        await storage_provider.save_object(
            ("\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries) + "\n").encode("utf-8"),
            f"{base_key}/{split}.jsonl",
            "application/jsonl",
        )

    archive_buffer = io.BytesIO()
    checksums: list[str] = []
    with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for _split, item, entry in export_images:
            image_bytes = await storage_provider.get_object(item.normalized_object_key or item.original_object_key)
            archive.writestr(entry["image_path"], image_bytes)
            entry["image_sha256"] = hashlib.sha256(image_bytes).hexdigest()
            checksums.append(f"{entry['image_sha256']}  {entry['image_path']}")
        archive.writestr("manifest.jsonl", "\n".join(json.dumps(entry, ensure_ascii=False) for entry in manifest) + "\n")
        for split, entries in manifests.items():
            archive.writestr(f"{split}.jsonl", "\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries) + "\n")
        for split, entries in excluded.items():
            archive.writestr(f"{split}.jsonl", "\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries) + "\n")
        csv_buffer = io.StringIO()
        fieldnames = [
            "image_id", "group_id", "specimen_id", "image_path", "image_sha256", "source_sha256",
            "split", "top_label", "meteorite_subclass", "terrestrial_family", "human_confidence",
            "consensus_status", "training_eligible", "model_version", "taxonomy_version", "annotation_policy_version",
        ]
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(manifest)
        archive.writestr("annotations.csv", csv_buffer.getvalue())
        archive.writestr("taxonomy.json", json.dumps({
            "version": batch.taxonomy_version,
            "top_labels": sorted({"meteorite", "terrestrial_rock", "uncertain", "unusable", "non_rock"}),
            "meteorite_subclasses": sorted(METEORITE_SUBCLASSES),
            "terrestrial_families": sorted(TERRESTRIAL_FAMILIES),
        }, ensure_ascii=False, indent=2))
        archive.writestr("checksums.sha256", "\n".join(checksums) + "\n")
        archive.writestr("dataset-card.md", "\n".join([
            f"# {batch.name}",
            "",
            f"Version: {version}",
            f"Model source: {MODEL_VERSION}",
            f"Taxonomy: {batch.taxonomy_version}",
            f"Annotation policy: {batch.annotation_policy_version}",
            "Split: deterministic 70/15/15 by specimen or perceptual-duplicate group.",
            "Only high-confidence, human-validated labels are included in train, validation and test.",
            json.dumps(statistics, ensure_ascii=False, indent=2),
        ]))
    await storage_provider.save_object(
        ("\n".join(json.dumps(entry, ensure_ascii=False) for entry in manifest) + "\n").encode("utf-8"),
        f"{base_key}/manifest.jsonl",
        "application/jsonl",
    )
    for split, entries in manifests.items():
        await storage_provider.save_object(
            ("\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries) + "\n").encode("utf-8"),
            f"{base_key}/{split}.jsonl",
            "application/jsonl",
        )
    archive_key = f"{base_key}/dataset-{version}.zip"
    await storage_provider.save_object(archive_buffer.getvalue(), archive_key, "application/zip")
    export = DatasetExportModel(
        id=export_id,
        batch_id=payload.dataset_id,
        version=version,
        status="completed",
        created_by=user.id,
        manifest_object_key=f"{base_key}/manifest.jsonl",
        archive_object_key=archive_key,
        statistics=statistics,
        created_at=_dataset_now(),
    )
    db.add(export)
    await _write_audit_log(
        db,
        actor_user_id=user.id,
        action="expert_dataset_exported",
        entity_type="dataset_export",
        entity_id=export.id,
        metadata={"dataset_id": payload.dataset_id, "version": version, "statistics": statistics},
    )
    await db.commit()
    return ExpertExportResponse(
        id=export.id,
        dataset_id=export.batch_id,
        version=export.version,
        status=export.status,
        statistics=statistics,
        manifest_url=await _dataset_object_url(export.manifest_object_key),
        download_url=await _dataset_object_url(export.archive_object_key),
        created_at=export.created_at.isoformat(),
    )


@app.get(
    "/api/v1/expert/exports",
    response_model=List[ExpertExportResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_list_exports(
    dataset_id: Optional[str] = Query(None),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    query = select(DatasetExportModel).order_by(DatasetExportModel.created_at.desc())
    if dataset_id:
        query = query.where(DatasetExportModel.batch_id == dataset_id)
    result = await db.execute(query)
    return [
        ExpertExportResponse(
            id=export.id,
            dataset_id=export.batch_id,
            version=export.version,
            status=export.status,
            statistics=export.statistics or {},
            manifest_url=await _dataset_object_url(export.manifest_object_key),
            download_url=await _dataset_object_url(export.archive_object_key),
            created_at=export.created_at.isoformat(),
        )
        for export in result.scalars().all()
    ]


@app.get(
    "/api/v1/expert/exports/{export_id}",
    response_model=ExpertExportResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_get_export(
    export_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    result = await db.execute(select(DatasetExportModel).where(DatasetExportModel.id == export_id))
    export = result.scalar_one_or_none()
    if not export:
        raise AppProductionException("NOT_FOUND", "Export introuvable.", 404)
    return ExpertExportResponse(
        id=export.id,
        dataset_id=export.batch_id,
        version=export.version,
        status=export.status,
        statistics=export.statistics or {},
        manifest_url=await _dataset_object_url(export.manifest_object_key),
        download_url=await _dataset_object_url(export.archive_object_key),
        created_at=export.created_at.isoformat(),
    )


@app.get(
    "/api/v1/expert/exports/{export_id}/download",
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def expert_download_export(
    export_id: str,
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    await _require_expert_context(authorization, db)
    result = await db.execute(select(DatasetExportModel).where(DatasetExportModel.id == export_id))
    export = result.scalar_one_or_none()
    if not export:
        raise AppProductionException("NOT_FOUND", "Export introuvable.", 404)
    return {
        "export_id": export.id,
        "version": export.version,
        "manifest_url": await _dataset_object_url(export.manifest_object_key),
        "download_url": await _dataset_object_url(export.archive_object_key),
        "statistics": export.statistics or {},
        "taxonomy_version": TAXONOMY_VERSION,
        "dataset_version": export.batch_id,
    }
