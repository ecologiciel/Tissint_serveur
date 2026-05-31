import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Float, Boolean, Integer, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime, timezone

# Configuration URL Production (avec fallback asyncpg)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/meteorite_db")

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

class UserSubscription(Base):
    __tablename__ = "user_subscriptions"
    
    user_id = Column(String, primary_key=True, index=True)
    tier = Column(String, nullable=False, default="free")
    remaining_tokens = Column(Integer, nullable=False, default=5)
    subscription_expires_at = Column(DateTime, nullable=True)

class UserModel(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, index=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    phone = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=True)
    password_hash = Column(String, nullable=False)
    role = Column(String, nullable=False, default="free")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class AuthSessionModel(Base):
    __tablename__ = "auth_sessions"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    device_id = Column(String, nullable=True)
    access_token_hash = Column(String, unique=True, index=True, nullable=False)
    refresh_token_hash = Column(String, unique=True, index=True, nullable=False)
    access_expires_at = Column(DateTime, nullable=False)
    refresh_expires_at = Column(DateTime, nullable=False)
    revoked_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class ScanModel(Base):
    __tablename__ = "scans"

    id = Column(String, primary_key=True, index=True)
    client_uuid = Column(String, unique=True, index=True, nullable=False)
    user_id = Column(String, index=True, nullable=False)
    status_code = Column(String, nullable=False)
    is_meteorite = Column(Boolean, nullable=False)
    meteorite_probability = Column(Float, nullable=False)
    dominant_class = Column(String, nullable=False)
    class_confidence = Column(Float, nullable=False)
    weight = Column(Float, nullable=True)
    magnetic = Column(Boolean, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    
    # Utilisation du type JSONB de PostgreSQL pour des requêtes ultra-rapides sur les tenseurs/probabilités
    raw_vision_outputs = Column(JSONB, nullable=False)
    
    # Chemins de stockage des fichiers pour découplage
    exterior_images_paths = Column(JSONB, nullable=False, default=list)
    interior_image_path = Column(String, nullable=True)

class ListingModel(Base):
    __tablename__ = "listings"
    
    id = Column(String, primary_key=True, index=True)
    scan_id = Column(String, ForeignKey("scans.id"), index=True, nullable=False)
    price = Column(Float, nullable=False)
    status = Column(String, nullable=False, default="draft")
    title = Column(String, nullable=True)
    description = Column(String, nullable=True)
    price_mode = Column(String, nullable=False, default="fixed_total")
    region = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class CollectionItemModel(Base):
    __tablename__ = "collection_items"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, index=True, nullable=False)
    scan_id = Column(String, ForeignKey("scans.id"), index=True, nullable=False)
    status = Column(String, nullable=False, default="eligible")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class MessageModel(Base):
    __tablename__ = "messages"
    
    id = Column(String, primary_key=True, index=True)
    conversation_id = Column(String, index=True, nullable=False)
    sender_id = Column(String, index=True, nullable=False)
    receiver_id = Column(String, index=True, nullable=False)
    text_content = Column(String, nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class AuditLogModel(Base):
    __tablename__ = "audit_logs"

    id = Column(String, primary_key=True, index=True)
    actor_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    action = Column(String, index=True, nullable=False)
    entity_type = Column(String, index=True, nullable=False)
    entity_id = Column(String, index=True, nullable=False)
    event_metadata = Column("metadata", JSONB, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
