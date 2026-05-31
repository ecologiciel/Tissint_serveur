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

class MarketplaceListingResponse(BaseModel):
    status: str
    message: str
    scan_id: str
    is_rare_candidate: bool
    dominant_class: str
    confidence: float
    weight: Optional[float] = None
    magnetic: Optional[bool] = None
    blurred_latitude: Optional[float] = Field(None, description="Latitude floutée pour anonymisation (précision ~11km)")
    blurred_longitude: Optional[float] = Field(None, description="Longitude floutée pour anonymisation (précision ~11km)")

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
