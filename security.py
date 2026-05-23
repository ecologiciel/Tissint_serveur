import os
from fastapi import Header, UploadFile
from typing import List
from exceptions import AppProductionException

# Clé API de production (idéalement définie dans l'environnement)
EXPECTED_API_KEY = os.getenv("API_KEY", "meteorite_secret_prod_key_2026")
MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 Mo
ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png"}

async def verify_api_key(x_api_key: str = Header(...)) -> str:
    """
    Valide la clé API reçue dans les headers HTTP (X-API-Key).
    """
    if x_api_key != EXPECTED_API_KEY:
        raise AppProductionException(
            error_code="UNAUTHORIZED",
            message="Clé API invalide ou manquante.",
            status_code=401
        )
    return x_api_key

async def validate_upload_file(file: UploadFile) -> UploadFile:
    """
    Valide qu'un fichier UploadFile est bien un format accepté (JPEG/PNG)
    et ne dépasse pas la taille maximale de 5 Mo.
    """
    # 1. Validation du type MIME
    if file.content_type not in ALLOWED_CONTENT_TYPES:
        raise AppProductionException(
            error_code="INVALID_FILE_FORMAT",
            message=f"Le format du fichier {file.filename} n'est pas supporté. Utilisez JPEG ou PNG.",
            status_code=415
        )

    # 2. Validation de la taille (Lecture asynchrone sécurisée)
    file_bytes = await file.read()
    if len(file_bytes) > MAX_FILE_SIZE_BYTES:
        raise AppProductionException(
            error_code="FILE_TOO_LARGE",
            message=f"Le fichier {file.filename} dépasse la taille limite de 5 Mo.",
            status_code=413
        )
    
    # 3. Réinitialisation du curseur de lecture pour le pipeline suivant
    await file.seek(0)
    
    return file
