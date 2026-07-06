from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, status, BackgroundTasks, Body, Header, Query
from pydantic import BaseModel
from typing import List, Optional
import uuid
import anyio
import os
import re
import hashlib
import json
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import or_, text
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.staticfiles import StaticFiles

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
    normalize_billing_provider,
    quota_limit_for_tier,
    refresh_subscription_state,
    subscription_is_active,
    subscription_payload,
)

# Import of our processing modules
SKIP_MODEL_LOAD = os.getenv("TINSSIT_SKIP_MODEL_LOAD") == "1"
if SKIP_MODEL_LOAD:
    VisionPipeline = None
else:
    from pipeline_vision import VisionPipeline
from fusion_engine import MeteoriteFusionEngine
from business_logic import BusinessOrchestrator

# Import database and storage components
from database import (
    engine,
    Base,
    get_db,
    UserModel,
    AuthSessionModel,
    ScanModel,
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
)
from storage import storage_provider, UPLOAD_DIR

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
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS status VARCHAR DEFAULT 'none'"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS provider VARCHAR"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS plan VARCHAR"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS cancel_at_period_end BOOLEAN DEFAULT FALSE"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS subscription_started_at TIMESTAMP"))
        await conn.execute(text("ALTER TABLE user_subscriptions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP"))
        await conn.execute(text("UPDATE user_subscriptions SET status = CASE WHEN tier IN ('premium', 'admin') THEN 'active' ELSE 'none' END WHERE status IS NULL"))
        await conn.execute(text("UPDATE user_subscriptions SET cancel_at_period_end = FALSE WHERE cancel_at_period_end IS NULL"))
        await conn.execute(text("UPDATE user_subscriptions SET updated_at = NOW() WHERE updated_at IS NULL"))
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

def _quota_response(subscription) -> QuotaResponse:
    subscription_state = subscription_payload(subscription)
    role = subscription_state["role"]
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
    return AuthUserResponse(
        id=user.id,
        first_name=user.first_name,
        last_name=user.last_name,
        phone=user.phone,
        email=user.email,
        role=subscription_state["role"],
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
        quota=_quota_response(subscription),
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
    if subscription.tier != "admin":
        raise AppProductionException("FORBIDDEN", "Acces admin requis.", 403)
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

    if subscription.tier in {"premium", "admin"} and subscription_is_active(subscription):
        return user_id, subscription

    if subscription.remaining_tokens <= 0:
        raise AppProductionException(
            error_code="QUOTA_EXCEEDED",
            message="Quota de scans epuise. Passez a la version Premium !",
            status_code=402,
        )

    return user_id, subscription

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
    user.role = subscription.tier
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
    return _quota_response(subscription)


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
    return AdminRadarListingResponse(
        listing_id=listing.id,
        scan_id=scan.id,
        status=_effective_listing_status(listing),
        dominant_class=scan.dominant_class,
        confidence=scan.class_confidence,
        class_confidence=scan.class_confidence,
        meteorite_probability=scan.meteorite_probability,
        fusion_score=scan.meteorite_probability,
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
    return PublicListingItem(
        listing_id=listing.id,
        scan_id=scan.id,
        price=listing.price,
        status=status_value,
        dominant_class=scan.dominant_class,
        confidence=scan.class_confidence,
        class_confidence=scan.class_confidence,
        meteorite_probability=scan.meteorite_probability,
        fusion_score=scan.meteorite_probability,
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

    return CollectionItemResponse(
        id=collection.id,
        scan_id=scan.id,
        class_name=scan.dominant_class,
        fusion_score=scan.meteorite_probability,
        status=status_value,
        status_code=scan.status_code,
        is_meteorite=scan.is_meteorite,
        class_confidence=scan.class_confidence,
        created_at=collection.created_at.isoformat() if collection.created_at else "",
        weight_g=scan.weight,
        magnetic=scan.magnetic,
        latitude=scan.latitude,
        longitude=scan.longitude,
        region=listing.region if listing else None,
        notes=listing.description if listing else None,
        meteorite_probability=scan.meteorite_probability,
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
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
    accept_language: Optional[str] = Header(None, alias="Accept-Language"),
):
    actor_user_id, subscription = await _check_scan_quota_for_request(
        db,
        authorization=authorization,
        x_user_id=x_user_id,
        form_user_id=user_id,
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
        actions = business_orchestrator.build_scan_actions(
            existing_scan.status_code,
            has_interior_cut=has_interior_cut,
        )
        return {
            "status_code": existing_scan.status_code,
            "is_meteorite": existing_scan.is_meteorite,
            "meteorite_probability": existing_scan.meteorite_probability,
            "dominant_class": existing_scan.dominant_class,
            "class_confidence": existing_scan.class_confidence,
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
                meteorite_probability=existing_scan.meteorite_probability,
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
    for f in files_exterior:
        await validate_upload_file(f)
        data = await f.read()
        list_exterior_bytes.append(data)
        path = await storage_provider.save_image(data, category="exterior")
        exterior_paths.append(path)
        
    # Extraction et Sauvegarde image intérieure si présente
    interior_bytes = None
    interior_path = None
    if file_interior:
        print("💎 [Anticipation] Une photo de coupe interne a été fournie dès le départ !")
        await validate_upload_file(file_interior)
        interior_bytes = await file_interior.read()
        interior_path = await storage_provider.save_image(interior_bytes, category="interior")
    else:
        print("🔍 Analyse basée uniquement sur les caractéristiques extérieures.")

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
        interior_image_path=interior_path
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
    if refreshed_subscription.tier == "free" and refreshed_subscription.remaining_tokens <= 1:
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

    return MarketplaceListingResponse(
        status=listing.status,
        message="Requête de mise en vente traitée. Données géospatiales anonymisées.",
        listing_id=listing.id,
        scan_id=scan_id,
        is_rare_candidate=is_rare,
        dominant_class=dominant_class,
        confidence=confidence,
        class_confidence=confidence,
        meteorite_probability=scan.meteorite_probability,
        fusion_score=scan.meteorite_probability,
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
        .order_by(ListingModel.created_at.desc())
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
        .order_by(ListingModel.created_at.desc())
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
        .order_by(ListingModel.created_at.desc())
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

    result = await db.execute(query.order_by(ListingModel.created_at.desc()))
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
