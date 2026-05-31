import uuid
from datetime import datetime, timedelta, timezone

from fastapi import Depends, Form
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from database import BillingCheckoutSessionModel, InvoiceModel, UserModel, UserSubscription, get_db
from exceptions import AppProductionException

FREE_DAILY_SCAN_LIMIT = 5
PREMIUM_DAILY_SCAN_LIMIT = 10
PLAN_PRICES_DH = {
    "monthly": 100.0,
    "yearly": 960.0,
}
PLAN_DURATIONS = {
    "monthly": timedelta(days=30),
    "yearly": timedelta(days=365),
}


def quota_limit_for_tier(tier: str) -> int:
    return PREMIUM_DAILY_SCAN_LIMIT if tier in {"premium", "admin"} else FREE_DAILY_SCAN_LIMIT

def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def normalize_billing_provider(provider: str) -> str:
    value = provider.strip().lower()
    if value == "dev":
        return "mock"
    if value not in {"mock", "cmi", "stripe", "wallet", "paypal"}:
        raise AppProductionException("VALIDATION_ERROR", "Provider de paiement invalide.", 400)
    return value

def amount_for_plan(plan: str) -> float:
    if plan not in PLAN_PRICES_DH:
        raise AppProductionException("VALIDATION_ERROR", "Plan d'abonnement invalide.", 400)
    return PLAN_PRICES_DH[plan]

def subscription_is_active(subscription: UserSubscription) -> bool:
    if subscription.tier == "admin":
        return True
    if subscription.tier != "premium":
        return False
    if subscription.subscription_expires_at is None:
        return subscription.status in {"active", "cancelled"}
    return subscription.subscription_expires_at > utc_now()

def refresh_subscription_state(subscription: UserSubscription, user: UserModel | None = None) -> UserSubscription:
    if subscription.tier == "admin":
        subscription.status = "active"
        subscription.updated_at = utc_now()
        if user:
            user.role = "admin"
        return subscription

    if subscription.tier == "premium" and not subscription_is_active(subscription):
        subscription.tier = "free"
        subscription.status = "expired"
        subscription.cancel_at_period_end = False
        subscription.remaining_tokens = max(subscription.remaining_tokens, 0)
        subscription.updated_at = utc_now()
        if user:
            user.role = "free"

    if subscription.tier == "premium" and user:
        user.role = "premium"
    elif subscription.tier == "free" and user:
        user.role = "free"
    return subscription

def subscription_status(subscription: UserSubscription) -> str:
    if subscription.tier == "admin":
        return "active"
    if subscription.tier == "premium" and subscription.cancel_at_period_end:
        return "cancelled"
    if subscription.tier == "premium" and subscription_is_active(subscription):
        return "active"
    if subscription.status in {"past_due", "cancelled", "expired"}:
        return subscription.status
    return "none"

async def get_or_create_subscription(user_id: str, db: AsyncSession) -> UserSubscription:
    result = await db.execute(select(UserSubscription).where(UserSubscription.user_id == user_id))
    subscription = result.scalar_one_or_none()

    if subscription:
        refresh_subscription_state(subscription)
        return subscription

    subscription = UserSubscription(
        user_id=user_id,
        tier="free",
        remaining_tokens=FREE_DAILY_SCAN_LIMIT,
        status="none",
    )
    db.add(subscription)
    await db.commit()
    await db.refresh(subscription)
    return subscription

def subscription_payload(subscription: UserSubscription) -> dict:
    status = subscription_status(subscription)
    role = subscription.tier if subscription.tier in {"premium", "admin"} and status in {"active", "cancelled"} else "free"
    return {
        "status": status,
        "role": role,
        "provider": subscription.provider,
        "plan": subscription.plan,
        "renews_at": subscription.subscription_expires_at.isoformat() if subscription.subscription_expires_at else None,
        "cancels_at": (
            subscription.subscription_expires_at.isoformat()
            if subscription.cancel_at_period_end and subscription.subscription_expires_at
            else None
        ),
    }

def invoice_payload(invoice: InvoiceModel) -> dict:
    return {
        "id": invoice.id,
        "number": invoice.number,
        "amount_dh": invoice.amount_dh,
        "vat_dh": invoice.vat_dh,
        "total_dh": invoice.total_dh,
        "status": invoice.status,
        "created_at": invoice.created_at.isoformat() if invoice.created_at else "",
        "download_url": invoice.download_url,
    }

def checkout_payload(session: BillingCheckoutSessionModel) -> dict:
    return {
        "id": session.id,
        "provider": session.provider,
        "checkout_url": session.checkout_url,
        "amount_dh": session.amount_dh,
        "currency": session.currency,
        "expires_at": session.expires_at.isoformat() if session.expires_at else None,
        "status": session.status,
    }

def invoice_number() -> str:
    return f"INV-{utc_now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

def create_invoice(
    user_id: str,
    checkout_session: BillingCheckoutSessionModel,
    db: AsyncSession,
    status: str = "paid",
) -> InvoiceModel:
    total = checkout_session.amount_dh
    amount = round(total / 1.2, 2)
    vat = round(total - amount, 2)
    invoice = InvoiceModel(
        id=str(uuid.uuid4()),
        user_id=user_id,
        checkout_session_id=checkout_session.id,
        provider=checkout_session.provider,
        provider_invoice_id=f"{checkout_session.provider}_{checkout_session.id}",
        number=invoice_number(),
        amount_dh=amount,
        vat_dh=vat,
        total_dh=total,
        currency=checkout_session.currency,
        status=status,
        download_url=None,
    )
    db.add(invoice)
    return invoice

async def activate_subscription(
    user: UserModel,
    subscription: UserSubscription,
    provider: str,
    plan: str,
    db: AsyncSession,
) -> UserSubscription:
    amount_for_plan(plan)
    now = utc_now()
    current_expiry = subscription.subscription_expires_at
    base_date = current_expiry if current_expiry and current_expiry > now else now
    subscription.tier = "premium" if subscription.tier != "admin" else "admin"
    subscription.status = "active"
    subscription.provider = provider
    subscription.plan = plan
    subscription.cancel_at_period_end = False
    subscription.subscription_started_at = subscription.subscription_started_at or now
    subscription.subscription_expires_at = base_date + PLAN_DURATIONS[plan]
    subscription.remaining_tokens = quota_limit_for_tier(subscription.tier)
    subscription.updated_at = now
    if user.role != "admin":
        user.role = "premium"
    db.add(subscription)
    db.add(user)
    return subscription

async def create_checkout_session(
    user: UserModel,
    subscription: UserSubscription,
    provider: str,
    plan: str,
    return_url: str | None,
    db: AsyncSession,
) -> BillingCheckoutSessionModel:
    provider = normalize_billing_provider(provider)
    amount = amount_for_plan(plan)
    now = utc_now()
    session = BillingCheckoutSessionModel(
        id=str(uuid.uuid4()),
        user_id=user.id,
        provider=provider,
        plan=plan,
        status="paid" if provider == "mock" else "pending",
        amount_dh=amount,
        currency="MAD",
        checkout_url=return_url if provider == "mock" else f"https://pay.tissint.local/{provider}/{uuid.uuid4().hex}",
        expires_at=now + timedelta(minutes=30),
        completed_at=now if provider == "mock" else None,
    )
    db.add(session)

    if provider == "mock":
        await activate_subscription(user, subscription, provider, plan, db)
        create_invoice(user.id, session, db)

    return session

async def cancel_subscription(
    user: UserModel,
    subscription: UserSubscription,
    db: AsyncSession,
) -> UserSubscription:
    refresh_subscription_state(subscription, user)
    if subscription.tier == "admin":
        raise AppProductionException("CONFLICT", "Un compte admin ne peut pas etre annule via billing.", 409)
    if subscription.tier != "premium" or not subscription_is_active(subscription):
        subscription.status = "none" if subscription.status not in {"past_due", "expired"} else subscription.status
        subscription.updated_at = utc_now()
        db.add(subscription)
        return subscription

    subscription.status = "cancelled"
    subscription.cancel_at_period_end = True
    subscription.updated_at = utc_now()
    user.role = "premium"
    db.add(subscription)
    db.add(user)
    return subscription


async def check_scan_quota(user_id: str = Form(...), db: AsyncSession = Depends(get_db)):
    """
    Verifie le quota de scans pour un utilisateur.
    Si le quota est epuise et qu'il n'est pas premium, leve une exception 402.
    """
    subscription = await get_or_create_subscription(user_id, db)
    refresh_subscription_state(subscription)

    if subscription.tier in {"premium", "admin"} and subscription_is_active(subscription):
        return subscription

    if subscription.remaining_tokens <= 0:
        raise AppProductionException(
            error_code="QUOTA_EXCEEDED",
            message="Quota de scans epuise. Passez a la version Premium !",
            status_code=402,
        )

    return subscription


async def decrement_quota(user_id: str, db: AsyncSession):
    """
    Deduit un jeton du quota pour les utilisateurs free.
    """
    result = await db.execute(select(UserSubscription).where(UserSubscription.user_id == user_id))
    subscription = result.scalar_one_or_none()

    if subscription and subscription.tier == "free" and subscription.remaining_tokens > 0:
        subscription.remaining_tokens -= 1
        await db.commit()
