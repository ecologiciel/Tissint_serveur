from pydantic import BaseModel, Field, field_validator
from typing import Any, Optional

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

class ScanDecisionResponse(BaseModel):
    status_code: str
    is_meteorite: bool
    meteorite_probability: float
    dominant_class: str
    class_confidence: float
    actions: ScanActions
    trigger_radar_admin: bool
    metadata_applied: ScanMetadataApplied
    scan_id: str
    is_sync_retry: bool = False

class PublishListingInput(BaseModel):
    price: Optional[float] = Field(None, ge=0.0)
    title: Optional[str] = Field(None, min_length=1, max_length=120)
    description: Optional[str] = Field(None, max_length=1000)
    price_mode: str = "fixed_total"
    region: Optional[str] = Field(None, min_length=1, max_length=120)

    @field_validator("price_mode")
    @classmethod
    def validate_price_mode(cls, value: str) -> str:
        allowed = {"fixed_total", "price_per_gram", "negotiable", "on_request"}
        if value not in allowed:
            raise ValueError("Mode de prix invalide.")
        return value

class MarketplaceListingResponse(BaseModel):
    status: str
    message: str
    listing_id: str
    scan_id: str
    is_rare_candidate: bool
    dominant_class: str
    confidence: float
    price: float
    price_mode: str = "fixed_total"
    title: Optional[str] = None
    description: Optional[str] = None
    region: Optional[str] = None
    weight: Optional[float] = None
    magnetic: Optional[bool] = None
    blurred_latitude: Optional[float] = Field(None, description="Latitude floutée pour anonymisation (précision ~11km)")
    blurred_longitude: Optional[float] = Field(None, description="Longitude floutée pour anonymisation (précision ~11km)")
    contact_locked_until: Optional[str] = None

class PublicListingItem(BaseModel):
    listing_id: str
    scan_id: str
    price: float
    status: str
    dominant_class: str
    confidence: float
    weight: Optional[float]
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

class AdminRadarListingResponse(BaseModel):
    listing_id: str
    scan_id: str
    status: str
    dominant_class: str
    confidence: float
    meteorite_probability: float
    price: float
    price_mode: str = "fixed_total"
    title: Optional[str] = None
    description: Optional[str] = None
    region: Optional[str] = None
    weight: Optional[float] = None
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

class AdminListingActionInput(BaseModel):
    reason: Optional[str] = Field(None, max_length=500)

class AdminActionResponse(BaseModel):
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
    created_at: str
    main_image_uri: Optional[str] = None
    meteorite_probability: Optional[float] = None

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
