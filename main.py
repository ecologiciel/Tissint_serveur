from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, status, BackgroundTasks, Body, Header
from pydantic import BaseModel
from typing import List, Optional
import uuid
import anyio
import os
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from exceptions import (
    AppProductionException,
    app_exception_handler,
    http_exception_handler,
    request_validation_exception_handler,
)
from schemas import (
    ApiErrorResponse,
    CollectionItemResponse,
    HealthResponse,
    QuotaResponse,
    ScanDecisionResponse,
    ScanMetadataInput,
    PublishListingInput,
    MarketplaceListingResponse,
    PublicListingItem,
    CreateMessageInput,
    MessageResponse,
)
from security import verify_api_key, validate_upload_file
from app.services.notifier import send_telegram_radar_alert
from billing import check_scan_quota, decrement_quota, get_or_create_subscription, quota_limit_for_tier

# Import of our processing modules
from pipeline_vision import VisionPipeline
from fusion_engine import MeteoriteFusionEngine
from business_logic import BusinessOrchestrator

# Import database and storage components
from database import engine, Base, get_db, ScanModel, ListingModel, CollectionItemModel, MessageModel
from storage import storage_provider

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Lifecycle: Initialize database schema at startup if needed
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

app = FastAPI(
    title="App_meteorite Core Server", 
    description="Back-end expert d'identification avec gestion flexible des flux multimédias",
    lifespan=lifespan
)
cors_origins = [
    origin.strip()
    for origin in os.getenv("CORS_ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_exception_handler(AppProductionException, app_exception_handler)
app.add_exception_handler(RequestValidationError, request_validation_exception_handler)
app.add_exception_handler(StarletteHTTPException, http_exception_handler)

ERROR_RESPONSES = {
    400: {"model": ApiErrorResponse, "description": "Requete invalide"},
    401: {"model": ApiErrorResponse, "description": "Cle API invalide ou manquante"},
    402: {"model": ApiErrorResponse, "description": "Quota epuise"},
    404: {"model": ApiErrorResponse, "description": "Ressource introuvable"},
    409: {"model": ApiErrorResponse, "description": "Conflit metier"},
    413: {"model": ApiErrorResponse, "description": "Fichier trop volumineux"},
    415: {"model": ApiErrorResponse, "description": "Format de fichier non supporte"},
    422: {"model": ApiErrorResponse, "description": "Erreur de validation"},
    503: {"model": ApiErrorResponse, "description": "Service indisponible"},
    500: {"model": ApiErrorResponse, "description": "Erreur interne"},
}

# Global initialization of our orchestration blocks
SKIP_MODEL_LOAD = os.getenv("TINSSIT_SKIP_MODEL_LOAD") == "1"
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

def resolve_user_id(x_user_id: Optional[str] = Header(None)) -> str:
    user_id = (x_user_id or "anonymous").strip()
    if len(user_id) < 3 or len(user_id) > 100:
        raise AppProductionException("VALIDATION_ERROR", "Identifiant utilisateur invalide.", 400)
    if not all(char.isalnum() or char in {"_", "-"} for char in user_id):
        raise AppProductionException("VALIDATION_ERROR", "Identifiant utilisateur invalide.", 400)
    return user_id

@app.get(
    "/api/v1/quota/me",
    response_model=QuotaResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_quota_me(
    user_id: str = Depends(resolve_user_id),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    subscription = await get_or_create_subscription(user_id, db)
    daily_limit = quota_limit_for_tier(subscription.tier)
    remaining_today = daily_limit if subscription.tier in {"premium", "admin"} else max(subscription.remaining_tokens, 0)

    return QuotaResponse(
        role=subscription.tier,
        daily_limit=daily_limit,
        remaining_today=remaining_today,
        resets_at=None,
    )

def _blur_coordinates(scan: ScanModel):
    safe_lat = round(scan.latitude, 1) if scan.latitude is not None else None
    safe_lon = round(scan.longitude, 1) if scan.longitude is not None else None
    return safe_lat, safe_lon

def _is_rare_candidate(dominant_class: str, confidence: float) -> bool:
    rare_classes = ["Achondrite", "Carbonee", "Martian", "Lunar", "Pallasite", "Iron", "Metallique"]
    return dominant_class in rare_classes and confidence >= 0.85

def _public_listing_item(listing: ListingModel, scan: ScanModel) -> PublicListingItem:
    safe_lat, safe_lon = _blur_coordinates(scan)
    return PublicListingItem(
        listing_id=listing.id,
        scan_id=scan.id,
        price=listing.price,
        status=listing.status,
        dominant_class=scan.dominant_class,
        confidence=scan.class_confidence,
        weight=scan.weight,
        blurred_latitude=safe_lat,
        blurred_longitude=safe_lon,
        is_rare=_is_rare_candidate(scan.dominant_class, scan.class_confidence),
        price_mode="on_request" if listing.price <= 0 else "fixed_total",
        created_at=listing.created_at.isoformat() if listing.created_at else None,
        can_contact=False,
        contact_lock_reason="premium_required",
    )

def _collection_status_for_scan(scan: ScanModel, listing: Optional[ListingModel] = None) -> str:
    if listing:
        if listing.status == "sold":
            return "sold"
        if listing.status in {"available", "reserved"}:
            return "listed"
    if scan.status_code == "DIAGNOSTIC_SUCCESS_HIGH":
        return "eligible"
    if scan.status_code == "DIAGNOSTIC_HESITANT":
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
    main_image_uri = None
    if scan.exterior_images_paths:
        main_image_uri = scan.exterior_images_paths[0]

    return CollectionItemResponse(
        id=collection.id,
        scan_id=scan.id,
        class_name=scan.dominant_class,
        fusion_score=scan.meteorite_probability,
        status=status_value,
        created_at=collection.created_at.isoformat() if collection.created_at else "",
        main_image_uri=main_image_uri,
        meteorite_probability=scan.meteorite_probability,
    )

@app.post(
    "/api/v1/scan/exterior",
    response_model=ScanDecisionResponse,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def scan_exterior(
    client_uuid: str = Form(...),
    files_exterior: List[UploadFile] = File(...),
    file_interior: Optional[UploadFile] = File(None),
    weight: Optional[float] = Form(None),
    magnetic: Optional[bool] = Form(None),
    latitude: Optional[float] = Form(None),
    longitude: Optional[float] = Form(None),
    subscription = Depends(check_scan_quota),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    try:
        metadata = ScanMetadataInput(
            client_uuid=client_uuid,
            user_id=subscription.user_id,
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
        print(f"🔄 [Idempotence] Scan existant récupéré pour client_uuid: {metadata.client_uuid}")
        return {
            "status_code": existing_scan.status_code,
            "is_meteorite": existing_scan.is_meteorite,
            "meteorite_probability": existing_scan.meteorite_probability,
            "dominant_class": existing_scan.dominant_class,
            "class_confidence": existing_scan.class_confidence,
            "actions": {
                "add_to_collection": existing_scan.status_code in ["DIAGNOSTIC_SUCCESS_HIGH", "DIAGNOSTIC_HESITANT"],
                "enable_marketplace_button": existing_scan.status_code == "DIAGNOSTIC_SUCCESS_HIGH",
                "invite_interior_cut": existing_scan.status_code != "DIAGNOSTIC_REJECTED"
            },
            "trigger_radar_admin": False,
            "metadata_applied": {
                "weight_provided": existing_scan.weight is not None,
                "magnetic_status": existing_scan.magnetic,
                "has_coordinates": existing_scan.latitude is not None and existing_scan.longitude is not None
            },
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
        final_decision = business_orchestrator.evaluate_decision(fusion_results)

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
    await db.commit()

    # Déduction du quota pour le scan d'IA (uniquement flux nominal, pas en cas d'idempotence)
    await decrement_quota(subscription.user_id, db)

    # Ajout du scan_id et des infos à la réponse
    final_decision["scan_id"] = scan_id

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
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    # 1. Récupération asynchrone du scan de la BDD
    result = await db.execute(select(ScanModel).where(ScanModel.id == scan_id))
    scan = result.scalar_one_or_none()
    
    if not scan:
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
        vision_results = scan.raw_vision_outputs
        
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

    final_decision = business_orchestrator.evaluate_decision(fusion_results)

    # 5. Mise à jour du document en BDD
    # On force manuellement le json pour la mise a jour
    scan.raw_vision_outputs = vision_results
    scan.interior_image_path = interior_path
    scan.status_code = final_decision["status_code"]
    scan.is_meteorite = final_decision["is_meteorite"]
    scan.meteorite_probability = final_decision["meteorite_probability"]
    scan.dominant_class = final_decision["dominant_class"]
    scan.class_confidence = final_decision["class_confidence"]

    await db.commit()
    
    final_decision["scan_id"] = scan_id
    
    return final_decision

@app.get(
    "/api/v1/collection",
    response_model=List[CollectionItemResponse],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def list_collection(
    user_id: str = Depends(resolve_user_id),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
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
    user_id: str = Depends(resolve_user_id),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    result = await db.execute(select(ScanModel).where(ScanModel.id == scan_id))
    scan = result.scalar_one_or_none()

    if not scan:
        raise AppProductionException("NOT_FOUND", "Scan introuvable.", 404)

    owner_id = scan.user_id if user_id == "anonymous" else user_id
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
    user_id: str = Depends(resolve_user_id),
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
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
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Route de mise en vente sur le Marketplace ou validation finale.
    Sécurise la vie privée via floutage des coordonnées.
    """
    result = await db.execute(select(ScanModel).where(ScanModel.id == scan_id))
    scan = result.scalar_one_or_none()

    if not scan:
        raise AppProductionException("NOT_FOUND", "Scan introuvable.", 404)

    # Extraction des valeurs de notre BDD
    dominant_class = scan.dominant_class
    confidence = scan.class_confidence
    user_id = scan.user_id

    # Le Déclencheur Strict pour le bot Telegram
    is_rare = _is_rare_candidate(dominant_class, confidence)
    if is_rare:
        background_tasks.add_task(
            send_telegram_radar_alert,
            scan_id=scan_id,
            stone_class=dominant_class,
            confidence=confidence,
            user_id=user_id
        )

    listing_price = payload.price if payload and payload.price is not None else 0.0
    listing_result = await db.execute(select(ListingModel).where(ListingModel.scan_id == scan_id))
    listing = listing_result.scalar_one_or_none()
    if listing:
        listing.status = "available"
        if payload and payload.price is not None:
            listing.price = listing_price
    else:
        listing = ListingModel(
            id=str(uuid.uuid4()),
            scan_id=scan_id,
            price=listing_price,
            status="available"
        )
        db.add(listing)

    await db.commit()
    await db.refresh(listing)

    # Security: Anonymisation & Floutage (Arrondi d'une décimale pour une précision régionale protectrice d'environ ~11km)
    safe_lat, safe_lon = _blur_coordinates(scan)

    return MarketplaceListingResponse(
        status=listing.status,
        message="Requête de mise en vente traitée. Données géospatiales anonymisées.",
        listing_id=listing.id,
        scan_id=scan_id,
        is_rare_candidate=is_rare,
        dominant_class=dominant_class,
        confidence=confidence,
        price=listing.price,
        weight=scan.weight,
        magnetic=scan.magnetic,
        blurred_latitude=safe_lat,
        blurred_longitude=safe_lon
    )


@app.get(
    "/api/v1/marketplace/listings",
    response_model=List[PublicListingItem],
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_marketplace_listings(
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Récupère toutes les annonces disponibles sur le Marketplace.
    Les coordonnées géospatiales sont anonymisées à 1 décimale (~ 11km).
    """
    # Requires an inner join with the scans table to retrieve dominant classes, weight, lat/long etc
    query = select(ListingModel, ScanModel).join(ScanModel, ListingModel.scan_id == ScanModel.id).where(ListingModel.status == "available")
    result = await db.execute(query)
    rows = result.all()

    listings = [_public_listing_item(listing, scan) for listing, scan in rows]
        
    return listings


@app.get(
    "/api/v1/marketplace/listings/{listing_id}",
    response_model=PublicListingItem,
    status_code=status.HTTP_200_OK,
    responses=ERROR_RESPONSES,
)
async def get_marketplace_listing_detail(
    listing_id: str,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    query = select(ListingModel, ScanModel).join(ScanModel, ListingModel.scan_id == ScanModel.id).where(ListingModel.id == listing_id)
    result = await db.execute(query)
    row = result.first()

    if not row:
        raise AppProductionException("NOT_FOUND", "Annonce introuvable.", 404)

    listing, scan = row
    return _public_listing_item(listing, scan)


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
