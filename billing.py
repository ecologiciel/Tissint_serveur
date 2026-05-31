from datetime import datetime, timezone

from fastapi import Depends, Form
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from database import UserSubscription, get_db
from exceptions import AppProductionException

FREE_DAILY_SCAN_LIMIT = 5
PREMIUM_DAILY_SCAN_LIMIT = 10


def quota_limit_for_tier(tier: str) -> int:
    return PREMIUM_DAILY_SCAN_LIMIT if tier in {"premium", "admin"} else FREE_DAILY_SCAN_LIMIT


async def get_or_create_subscription(user_id: str, db: AsyncSession) -> UserSubscription:
    result = await db.execute(select(UserSubscription).where(UserSubscription.user_id == user_id))
    subscription = result.scalar_one_or_none()

    if subscription:
        return subscription

    subscription = UserSubscription(
        user_id=user_id,
        tier="free",
        remaining_tokens=FREE_DAILY_SCAN_LIMIT,
    )
    db.add(subscription)
    await db.commit()
    await db.refresh(subscription)
    return subscription


async def check_scan_quota(user_id: str = Form(...), db: AsyncSession = Depends(get_db)):
    """
    Verifie le quota de scans pour un utilisateur.
    Si le quota est epuise et qu'il n'est pas premium, leve une exception 402.
    """
    subscription = await get_or_create_subscription(user_id, db)

    if subscription.tier == "premium":
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if subscription.subscription_expires_at is None or subscription.subscription_expires_at > now:
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
