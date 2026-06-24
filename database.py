import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Float, Boolean, Integer, DateTime, ForeignKey, UniqueConstraint
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
    status = Column(String, nullable=False, default="none")
    provider = Column(String, nullable=True)
    plan = Column(String, nullable=True)
    cancel_at_period_end = Column(Boolean, nullable=False, default=False)
    subscription_started_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

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

class MessageThreadModel(Base):
    __tablename__ = "message_threads"
    __table_args__ = (UniqueConstraint("listing_id", "buyer_id", "seller_id", name="uq_message_thread_participants"),)

    id = Column(String, primary_key=True, index=True)
    listing_id = Column(String, ForeignKey("listings.id"), index=True, nullable=False)
    buyer_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    seller_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    unread_for_buyer = Column(Integer, nullable=False, default=0)
    unread_for_seller = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class FavoriteModel(Base):
    __tablename__ = "favorites"
    __table_args__ = (UniqueConstraint("user_id", "listing_id", name="uq_favorite_user_listing"),)

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    listing_id = Column(String, ForeignKey("listings.id"), index=True, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class NotificationModel(Base):
    __tablename__ = "notifications"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    type = Column(String, index=True, nullable=False)
    title = Column(String, nullable=False)
    body = Column(String, nullable=False)
    read = Column(Boolean, nullable=False, default=False)
    action = Column(String, nullable=True)
    event_metadata = Column("metadata", JSONB, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class PushSubscriptionModel(Base):
    __tablename__ = "push_subscriptions"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    endpoint = Column(String, unique=True, index=True, nullable=False)
    keys = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class SellerRatingModel(Base):
    __tablename__ = "seller_ratings"
    __table_args__ = (UniqueConstraint("listing_id", "buyer_id", name="uq_seller_rating_listing_buyer"),)

    id = Column(String, primary_key=True, index=True)
    listing_id = Column(String, ForeignKey("listings.id"), index=True, nullable=False)
    seller_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    buyer_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    stars = Column(Integer, nullable=False)
    comment = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class WalletAccountModel(Base):
    __tablename__ = "wallet_accounts"

    user_id = Column(String, ForeignKey("users.id"), primary_key=True, index=True)
    balance = Column(Float, nullable=False, default=0.0)
    currency = Column(String, nullable=False, default="MAD")
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class WalletTransactionModel(Base):
    __tablename__ = "wallet_transactions"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    type = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    fee = Column(Float, nullable=False, default=0.0)
    net = Column(Float, nullable=False)
    desc = Column(String, nullable=True)
    status = Column(String, nullable=False, default="pending")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class WithdrawalRequestModel(Base):
    __tablename__ = "withdrawal_requests"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    amount = Column(Float, nullable=False)
    iban = Column(String, nullable=False)
    status = Column(String, nullable=False, default="processing")
    estimated_days = Column(Integer, nullable=False, default=2)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class AuditLogModel(Base):
    __tablename__ = "audit_logs"

    id = Column(String, primary_key=True, index=True)
    actor_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    action = Column(String, index=True, nullable=False)
    entity_type = Column(String, index=True, nullable=False)
    entity_id = Column(String, index=True, nullable=False)
    event_metadata = Column("metadata", JSONB, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class BillingCheckoutSessionModel(Base):
    __tablename__ = "billing_checkout_sessions"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    provider = Column(String, nullable=False)
    plan = Column(String, nullable=False)
    status = Column(String, nullable=False, default="pending")
    amount_dh = Column(Float, nullable=False)
    currency = Column(String, nullable=False, default="MAD")
    checkout_url = Column(String, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class InvoiceModel(Base):
    __tablename__ = "invoices"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    checkout_session_id = Column(String, ForeignKey("billing_checkout_sessions.id"), index=True, nullable=True)
    provider = Column(String, nullable=False)
    provider_invoice_id = Column(String, nullable=True)
    number = Column(String, unique=True, index=True, nullable=False)
    amount_dh = Column(Float, nullable=False)
    vat_dh = Column(Float, nullable=False, default=0.0)
    total_dh = Column(Float, nullable=False)
    currency = Column(String, nullable=False, default="MAD")
    status = Column(String, nullable=False, default="paid")
    download_url = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

class BillingEventModel(Base):
    __tablename__ = "billing_events"
    __table_args__ = (UniqueConstraint("provider", "event_id", name="uq_billing_event_provider_event"),)

    id = Column(String, primary_key=True, index=True)
    provider = Column(String, nullable=False)
    event_id = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=True)
    checkout_session_id = Column(String, ForeignKey("billing_checkout_sessions.id"), index=True, nullable=True)
    payload = Column(JSONB, nullable=False, default=dict)
    processed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
