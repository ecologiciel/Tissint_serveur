import os
import hashlib
import hmac
import secrets
from fastapi import Header, UploadFile
from exceptions import AppProductionException

# Clé API de production (idéalement définie dans l'environnement)
EXPECTED_API_KEY = os.getenv("API_KEY", "meteorite_secret_prod_key_2026")
MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 Mo
ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png"}
PASSWORD_HASH_ITERATIONS = 260_000

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PASSWORD_HASH_ITERATIONS,
    ).hex()
    return f"pbkdf2_sha256${PASSWORD_HASH_ITERATIONS}${salt}${digest}"

def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, iterations, salt, expected_digest = stored_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            int(iterations),
        ).hex()
        return hmac.compare_digest(digest, expected_digest)
    except Exception:
        return False

def create_token() -> str:
    return secrets.token_urlsafe(48)

def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()

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
