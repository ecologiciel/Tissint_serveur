from datetime import datetime, timezone
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Form, Depends
from exceptions import AppProductionException
from database import UserSubscription, get_db

async def check_scan_quota(user_id: str = Form(...), db: AsyncSession = Depends(get_db)):
    """
    Vérifie le quota de scans pour un utilisateur.
    Si le quota est épuisé et qu'il n'est pas premium, lève une exception 402.
    """
    result = await db.execute(select(UserSubscription).where(UserSubscription.user_id == user_id))
    subscription = result.scalar_one_or_none()

    if not subscription:
        # Création d'un profil par défaut (free, 3 jetons) si non existant
        subscription = UserSubscription(user_id=user_id, tier="free", remaining_tokens=3)
        db.add(subscription)
        await db.commit()
        await db.refresh(subscription)

    if subscription.tier == "premium":
        if subscription.subscription_expires_at is None or subscription.subscription_expires_at > datetime.now(timezone.utc).replace(tzinfo=None):
            return subscription # Premium valide

    # Si c'est un profil 'free' ou premium expiré
    if subscription.remaining_tokens <= 0:
        raise AppProductionException(
            error_code="QUOTA_EXCEEDED",
            message="Quota de scans épuisé. Passez à la version Premium !",
            status_code=402
        )
    
    return subscription

async def decrement_quota(user_id: str, db: AsyncSession):
    """
    Déduit un jeton du quota pour les utilisateurs 'free'.
    """
    result = await db.execute(select(UserSubscription).where(UserSubscription.user_id == user_id))
    subscription = result.scalar_one_or_none()

    if subscription and subscription.tier == "free":
        if subscription.remaining_tokens > 0:
            subscription.remaining_tokens -= 1
            await db.commit()

