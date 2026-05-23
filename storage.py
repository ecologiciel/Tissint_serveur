import os
import aiofiles
from uuid import uuid4
from pathlib import Path

UPLOAD_DIR = Path(os.getenv("STORAGE_DIR", "./storage_vessel"))

class ProductionStorageProvider:
    def __init__(self):
        # Création sécurisée des répertoires de stockage
        os.makedirs(UPLOAD_DIR / "exterior", exist_ok=True)
        os.makedirs(UPLOAD_DIR / "interior", exist_ok=True)

    async def save_image(self, file_bytes: bytes, category: str = "exterior") -> str:
        """Sauvegarde un fichier binaire de manière asynchrone et renvoie son chemin unique."""
        file_id = f"{uuid4()}.jpg"
        target_path = UPLOAD_DIR / category / file_id
        
        async with aiofiles.open(target_path, "wb") as f:
            await f.write(file_bytes)
            
        return str(target_path)

storage_provider = ProductionStorageProvider()
