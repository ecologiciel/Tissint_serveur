import asyncio
import os
import sys
import uuid
from pathlib import Path

from sqlalchemy import or_, select

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres@127.0.0.1:55432/meteorite_db",
)
os.environ.setdefault("TINSSIT_SKIP_MODEL_LOAD", "1")

from database import AsyncSessionLocal, Base, UserModel, UserSubscription, engine
from security import hash_password


TEST_EMAIL = "user@tissint.ma"
TEST_PHONE = "+212600000001"
TEST_PASSWORD = "demo1234"


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(UserModel).where(
                or_(UserModel.email == TEST_EMAIL, UserModel.phone == TEST_PHONE)
            )
        )
        user = result.scalar_one_or_none()

        if user is None:
            user = UserModel(
                id=str(uuid.uuid4()),
                first_name="Tissint",
                last_name="Test",
                phone=TEST_PHONE,
                email=TEST_EMAIL,
                password_hash=hash_password(TEST_PASSWORD),
                role="free",
            )
            db.add(user)
            await db.flush()
        else:
            user.email = TEST_EMAIL
            user.phone = TEST_PHONE
            user.first_name = user.first_name or "Tissint"
            user.last_name = user.last_name or "Test"
            user.password_hash = hash_password(TEST_PASSWORD)
            user.role = "free"

        subscription = await db.get(UserSubscription, user.id)
        if subscription is None:
            subscription = UserSubscription(
                user_id=user.id,
                tier="free",
                remaining_tokens=5,
                status="none",
            )
            db.add(subscription)
        else:
            subscription.tier = "free"
            subscription.remaining_tokens = max(subscription.remaining_tokens or 0, 5)
            subscription.status = subscription.status or "none"

        await db.commit()

    print(f"Seeded local test user: {TEST_EMAIL}")


if __name__ == "__main__":
    asyncio.run(main())
