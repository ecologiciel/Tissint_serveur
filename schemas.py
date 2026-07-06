from pydantic import BaseModel, Field, field_validator
from typing import Any, Literal, Optional, List

class ApiError(BaseModel):
    code: str
    message: str
    details: Optional[Any] = None

class ApiErrorResponse(BaseModel):
    status_code: str = "DIAGNOSTIC_FAILED"
    error: ApiError

class HealthResponse(BaseModel):
    status: str
    service: str
    database: str

class QuotaResponse(BaseModel):
    role: str
    daily_limit: int
    remaining_today: int
    resets_at: Optional[str] = None

class LoginInput(BaseModel):
    phone_or_email: str = Field(..., min_length=3, max_length=120)
    password: str = Field(..., min_length=6, max_length=200)
    device_id: Optional[str] = Field(None, max_length=150)

class RegisterInput(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=80)
    last_name: str = Field(..., min_length=1, max_length=80)
    phone: str = Field(..., min_length=5, max_length=40)
    email: Optional[str] = Field(None, max_length=120)
    password: str = Field(..., min_length=6, max_length=200)
    desired_role: str = "free"
    device_id: Optional[str] = Field(None, max_length=150)

    @field_validator("desired_role")
    @classmethod
    def validate_desired_role(cls, value: str) -> str:
        if value != "free":
            raise ValueError("Le role demande doit etre free.")
        return value

class RefreshTokenInput(BaseModel):
    refresh_token: str = Field(..., min_length=20)

class LogoutInput(BaseModel):
    refresh_token: Optional[str] = None

class AuthUserResponse(BaseModel):
    id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    role: str
    premium_expires_at: Optional[str] = None

class AuthResponse(BaseModel):
    access_token: str
    refresh_token: str = ""
    expires_at: str
    user: AuthUserResponse
    quota: QuotaResponse

class ScanActions(BaseModel):
    add_to_collection: bool
    enable_marketplace_button: bool
    invite_interior_cut: bool

class ScanMetadataApplied(BaseModel):
    weight_provided: bool
    magnetic_status: Optional[bool] = None
    has_coordinates: bool

class ScanDiagnosticMessage(BaseModel):
    language: Literal["ar", "fr"]
    tone: Literal["success", "warning", "neutral"]
    title: str
    body: str

class ScanDecisionResponse(BaseModel):
    status_code: str
    is_meteorite: bool
    meteorite_probability: float
    dominant_class: str
    class_confidence: float
    actions: ScanActions
    trigger_radar_admin: bool
    metadata_applied: ScanMetadataApplied
    message: ScanDiagnosticMessage
    scan_id: str
    is_sync_retry: bool = False

class PublishListingInput(BaseModel):
    price: Optional[float] = Field(None, ge=0.0)
    title: Optional[str] = Field(None, min_length=1, max_length=120)
    description: Optional[str] = Field(None, max_length=1000)
    price_mode: str = "fixed_total"
    region: Optional[str] = Field(None, min_length=1, max_length=120)
    weight_g: Optional[float] = Field(None, gt=0.0, le=100000.0)

    @field_validator("price_mode")
    @classmethod
    def validate_price_mode(cls, value: str) -> str:
        allowed = {"fixed_total", "price_per_gram", "negotiable", "on_request"}
        if value not in allowed:
            raise ValueError("Mode de prix invalide.")
        return value

class UpdateListingInput(BaseModel):
    price: Optional[float] = Field(None, ge=0.0)
    title: Optional[str] = Field(None, min_length=1, max_length=120)
    description: Optional[str] = Field(None, max_length=1000)
    price_mode: Optional[str] = None
    region: Optional[str] = Field(None, min_length=1, max_length=120)
    weight_g: Optional[float] = Field(None, gt=0.0, le=100000.0)

    @field_validator("price_mode")
    @classmethod
    def validate_price_mode(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        allowed = {"fixed_total", "price_per_gram", "negotiable", "on_request"}
        if value not in allowed:
            raise ValueError("Mode de prix invalide.")
        return value

class MarketplaceListingResponse(BaseModel):
    ok: bool = True
    status: str
    message: str
    listing_id: str
    scan_id: str
    is_rare_candidate: bool
    dominant_class: str
    confidence: float
    class_confidence: float
    meteorite_probability: float
    fusion_score: float
    price: float
    price_mode: str = "fixed_total"
    title: Optional[str] = None
    description: Optional[str] = None
    region: Optional[str] = None
    weight: Optional[float] = None
    weight_g: Optional[float] = None
    magnetic: Optional[bool] = None
    blurred_latitude: Optional[float] = Field(None, description="Latitude floutée pour anonymisation (précision ~11km)")
    blurred_longitude: Optional[float] = Field(None, description="Longitude floutée pour anonymisation (précision ~11km)")
    contact_locked_until: Optional[str] = None
    main_image_uri: Optional[str] = None
    image_url: Optional[str] = None
    thumbnail_uri: Optional[str] = None
    interior_image_uri: Optional[str] = None
    gallery_images: List[str] = Field(default_factory=list)

class PublicListingItem(BaseModel):
    listing_id: str
    scan_id: str
    price: float
    status: str
    dominant_class: str
    confidence: float
    class_confidence: float
    meteorite_probability: float
    fusion_score: float
    weight: Optional[float]
    weight_g: Optional[float] = None
    blurred_latitude: Optional[float]
    blurred_longitude: Optional[float]
    is_rare: bool = False
    price_mode: str = "on_request"
    created_at: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    region: Optional[str] = None
    seller_masked_name: Optional[str] = None
    seller_name: Optional[str] = None
    seller_phone: Optional[str] = None
    seller_whatsapp: Optional[str] = None
    seller_verified: bool = False
    can_contact: bool = False
    contact_lock_reason: Optional[str] = "premium_required"
    contact_locked_until: Optional[str] = None
    main_image_uri: Optional[str] = None
    image_url: Optional[str] = None
    thumbnail_uri: Optional[str] = None
    interior_image_uri: Optional[str] = None
    gallery_images: List[str] = Field(default_factory=list)

class AdminRadarListingResponse(BaseModel):
    listing_id: str
    scan_id: str
    status: str
    dominant_class: str
    confidence: float
    class_confidence: float
    meteorite_probability: float
    fusion_score: float
    price: float
    price_mode: str = "fixed_total"
    title: Optional[str] = None
    description: Optional[str] = None
    region: Optional[str] = None
    weight: Optional[float] = None
    weight_g: Optional[float] = None
    magnetic: Optional[bool] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    is_rare: bool = True
    hold_until: Optional[str] = None
    created_at: Optional[str] = None
    seller_user_id: Optional[str] = None
    seller_name: Optional[str] = None
    seller_phone: Optional[str] = None
    seller_email: Optional[str] = None
    seller_verified: bool = False
    main_image_uri: Optional[str] = None
    image_url: Optional[str] = None
    thumbnail_uri: Optional[str] = None
    interior_image_uri: Optional[str] = None
    gallery_images: List[str] = Field(default_factory=list)

class AdminListingActionInput(BaseModel):
    reason: Optional[str] = Field(None, max_length=500)

class AdminActionResponse(BaseModel):
    ok: bool = True
    status: str
    message: str
    listing: AdminRadarListingResponse

class AuditLogResponse(BaseModel):
    id: str
    actor_user_id: str
    action: str
    entity_type: str
    entity_id: str
    metadata: Optional[Any] = None
    created_at: str

class BillingCheckoutInput(BaseModel):
    plan: str = "monthly"
    provider: str = "mock"
    return_url: Optional[str] = Field(None, max_length=500)

    @field_validator("plan")
    @classmethod
    def validate_plan(cls, value: str) -> str:
        if value not in {"monthly", "yearly"}:
            raise ValueError("Plan d'abonnement invalide.")
        return value

    @field_validator("provider")
    @classmethod
    def validate_provider(cls, value: str) -> str:
        provider = value.strip().lower()
        if provider == "dev":
            return "mock"
        if provider not in {"mock", "cmi", "stripe", "wallet", "paypal"}:
            raise ValueError("Provider de paiement invalide.")
        return provider

class CheckoutSessionResponse(BaseModel):
    id: str
    provider: str
    checkout_url: Optional[str] = None
    amount_dh: float
    currency: str = "MAD"
    expires_at: Optional[str] = None
    status: str = "pending"

class SubscriptionResponse(BaseModel):
    status: str
    role: str
    provider: Optional[str] = None
    plan: Optional[str] = None
    renews_at: Optional[str] = None
    cancels_at: Optional[str] = None

class InvoiceResponse(BaseModel):
    id: str
    number: str
    amount_dh: float
    vat_dh: Optional[float] = None
    total_dh: float
    status: str
    created_at: str
    download_url: Optional[str] = None

class BillingWebhookResponse(BaseModel):
    status: str
    processed: bool
    event_id: str
    subscription: Optional[SubscriptionResponse] = None

class CollectionItemResponse(BaseModel):
    id: str
    scan_id: str
    class_name: str
    fusion_score: float
    status: str
    status_code: Optional[str] = None
    is_meteorite: Optional[bool] = None
    class_confidence: Optional[float] = None
    created_at: str
    main_image_uri: Optional[str] = None
    image_url: Optional[str] = None
    thumbnail_uri: Optional[str] = None
    interior_image_uri: Optional[str] = None
    gallery_images: List[str] = Field(default_factory=list)
    weight_g: Optional[float] = None
    magnetic: Optional[bool] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    region: Optional[str] = None
    notes: Optional[str] = None
    meteorite_probability: Optional[float] = None

class SendMessageInput(BaseModel):
    listing_id: Optional[str] = Field(None, max_length=120)
    thread_id: Optional[str] = Field(None, max_length=120)
    text: str = Field(..., min_length=1, max_length=2000)

class UiMessageResponse(BaseModel):
    id: str
    thread_id: str
    from_me: bool
    text: str
    created_at: str

class MessageThreadResponse(BaseModel):
    id: str
    listing_id: str
    listing_title: Optional[str] = None
    listing_image_uri: Optional[str] = None
    peer_name: Optional[str] = None
    peer_verified: bool = False
    last_message: Optional[str] = None
    last_at: Optional[str] = None
    unread: int = 0

class OkResponse(BaseModel):
    ok: bool = True

class NotificationResponse(BaseModel):
    id: str
    type: str
    title: str
    body: str
    read: bool
    created_at: str
    action: Optional[str] = None

class PushSubscriptionKeys(BaseModel):
    p256dh: str
    auth: str

class PushSubscribeInput(BaseModel):
    endpoint: str = Field(..., min_length=10, max_length=2000)
    keys: PushSubscriptionKeys

class PushSubscribeResponse(BaseModel):
    subscribed: bool

class RatingInput(BaseModel):
    listing_id: str
    seller_id: str
    stars: int = Field(..., ge=1, le=5)
    comment: Optional[str] = Field(None, max_length=1000)

class RatingResponse(BaseModel):
    id: str
    ok: bool = True

class SellerProfileResponse(BaseModel):
    id: str
    name: Optional[str] = None
    average_rating: float = 0.0
    total_ratings: int = 0
    listings: List[PublicListingItem] = Field(default_factory=list)

class WalletTransactionResponse(BaseModel):
    id: str
    type: str
    amount: float
    fee: float
    net: float
    desc: Optional[str] = None
    created_at: str
    status: str

class WalletResponse(BaseModel):
    balance: float
    currency: str = "MAD"
    transactions: List[WalletTransactionResponse] = Field(default_factory=list)

class WithdrawInput(BaseModel):
    amount: float = Field(..., gt=0)
    iban: str = Field(..., min_length=8, max_length=80)

class WithdrawResponse(BaseModel):
    request_id: str
    status: str
    estimated_days: int = 2

class MarketplaceSearchInput(BaseModel):
    query: Optional[str] = Field(None, max_length=120)
    region: Optional[str] = Field(None, max_length=120)
    price_min: Optional[float] = Field(None, ge=0)
    price_max: Optional[float] = Field(None, ge=0)
    classification: Optional[str] = Field(None, max_length=120)

class MarketplaceTrendingItem(BaseModel):
    classification: str
    change_percent: float = 0.0
    avg_price: float = 0.0

class MarketplaceRegionVolume(BaseModel):
    region: str
    count: int
    pct: float

class MarketplaceStatsResponse(BaseModel):
    total_listings: int
    total_sales: int
    avg_price_dh: float
    trending: List[MarketplaceTrendingItem] = Field(default_factory=list)
    volume_by_region: List[MarketplaceRegionVolume] = Field(default_factory=list)
    price_history: dict[str, List[float]] = Field(default_factory=dict)
    months: List[str] = Field(default_factory=list)

class CreateMessageInput(BaseModel):
    conversation_id: str = Field(..., description="ID unique de la conversation")
    sender_id: str = Field(..., description="ID de l'utilisateur expéditeur")
    receiver_id: str = Field(..., description="ID de l'utilisateur destinataire")
    text_content: str = Field(..., min_length=1)

class MessageResponse(BaseModel):
    id: str
    conversation_id: str
    sender_id: str
    receiver_id: str
    text_content: str
    timestamp: str

class ScanMetadataInput(BaseModel):
    client_uuid: str = Field(..., min_length=5, max_length=100, description="UUID généré par le client pour assurer l'idempotence")
    user_id: str = Field(..., min_length=3, max_length=100)
    weight: Optional[float] = Field(None, ge=0.0, le=100000.0) # Poids en grammes rationnel
    magnetic: Optional[bool] = None
    latitude: Optional[float] = Field(None, ge=-90.0, le=90.0)
    longitude: Optional[float] = Field(None, ge=-180.0, le=180.0)

    @field_validator('user_id')
    @classmethod
    def sanitize_user_id(cls, v: str) -> str:
        if not v.isalnum() and "_" not in v and "-" not in v:
            raise ValueError("L'ID utilisateur contient des caractères non autorisés.")
        return v
