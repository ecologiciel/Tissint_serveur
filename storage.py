import os
from pathlib import Path
from uuid import uuid4

import aiofiles


UPLOAD_DIR = Path(os.getenv("STORAGE_DIR", "./storage_vessel"))
PUBLIC_STORAGE_BASE_URL = os.getenv("PUBLIC_STORAGE_BASE_URL", "").rstrip("/")


class ProductionStorageProvider:
    def __init__(self):
        os.makedirs(UPLOAD_DIR / "exterior", exist_ok=True)
        os.makedirs(UPLOAD_DIR / "interior", exist_ok=True)

    async def save_image(self, file_bytes: bytes, category: str = "exterior") -> str:
        file_id = f"{uuid4()}.jpg"
        target_path = UPLOAD_DIR / category / file_id

        async with aiofiles.open(target_path, "wb") as f:
            await f.write(file_bytes)

        return str(target_path)

    def public_url(self, stored_path: str | None) -> str | None:
        if not stored_path:
            return None

        path = Path(stored_path)
        try:
            relative_path = path.resolve().relative_to(UPLOAD_DIR.resolve())
        except ValueError:
            relative_path = Path(path.name)

        url_path = "/".join(relative_path.parts)
        if PUBLIC_STORAGE_BASE_URL:
            return f"{PUBLIC_STORAGE_BASE_URL}/{url_path}"
        return f"/storage/{url_path}"


storage_provider = ProductionStorageProvider()
