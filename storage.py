import asyncio
import os
from pathlib import Path
from uuid import uuid4

import aiofiles


UPLOAD_DIR = Path(os.getenv("STORAGE_DIR", "./storage_vessel"))
PUBLIC_STORAGE_BASE_URL = os.getenv("PUBLIC_STORAGE_BASE_URL", "").rstrip("/")
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local").strip().lower()
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
S3_PREFIX = os.getenv("S3_PREFIX", "tissint").strip("/")
S3_REGION = os.getenv("S3_REGION", "").strip() or None
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "").strip() or None
S3_PRESIGNED_TTL = int(os.getenv("S3_PRESIGNED_TTL_SECONDS", "900"))


class LocalStorageProvider:
    def __init__(self):
        os.makedirs(UPLOAD_DIR / "exterior", exist_ok=True)
        os.makedirs(UPLOAD_DIR / "interior", exist_ok=True)
        os.makedirs(UPLOAD_DIR / "datasets", exist_ok=True)

    async def save_object(self, file_bytes: bytes, object_key: str, content_type: str | None = None) -> str:
        relative_key = Path(object_key)
        target_path = UPLOAD_DIR / relative_key
        target_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(target_path, "wb") as file_handle:
            await file_handle.write(file_bytes)
        return object_key.strip("/")

    async def get_object(self, stored_path: str) -> bytes:
        target_path = Path(stored_path)
        if not target_path.is_absolute():
            target_path = UPLOAD_DIR / target_path
        async with aiofiles.open(target_path, "rb") as file_handle:
            return await file_handle.read()

    async def save_image(self, file_bytes: bytes, category: str = "exterior") -> str:
        file_id = f"{uuid4()}.jpg"
        return await self.save_object(file_bytes, f"{category}/{file_id}", "image/jpeg")

    async def create_presigned_get(self, object_key: str, expires_in: int | None = None) -> str:
        return self.public_url(object_key)

    async def create_presigned_put(self, object_key: str, content_type: str = "application/octet-stream") -> str:
        return self.public_url(object_key)

    def public_url(self, stored_path: str | None) -> str | None:
        if not stored_path:
            return None

        path = Path(stored_path)
        try:
            relative_path = path.resolve().relative_to(UPLOAD_DIR.resolve())
        except ValueError:
            relative_path = path

        url_path = "/".join(relative_path.parts)
        if PUBLIC_STORAGE_BASE_URL:
            return f"{PUBLIC_STORAGE_BASE_URL}/{url_path}"
        return f"/storage/{url_path}"


class S3StorageProvider:
    def __init__(self):
        if not S3_BUCKET:
            raise RuntimeError("S3_BUCKET est obligatoire avec STORAGE_BACKEND=s3")
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("boto3 est obligatoire avec STORAGE_BACKEND=s3") from exc

        self.bucket = S3_BUCKET
        self.prefix = S3_PREFIX
        self.client = boto3.client(
            "s3",
            region_name=S3_REGION,
            endpoint_url=S3_ENDPOINT_URL,
        )

    def _key(self, object_key: str) -> str:
        normalized = object_key.strip("/")
        if self.prefix and (normalized == self.prefix or normalized.startswith(f"{self.prefix}/")):
            return normalized
        return f"{self.prefix}/{normalized}" if self.prefix else normalized

    async def save_object(self, file_bytes: bytes, object_key: str, content_type: str | None = None) -> str:
        key = self._key(object_key)
        extra_args = {"ContentType": content_type} if content_type else {}
        await asyncio.to_thread(
            self.client.put_object,
            Bucket=self.bucket,
            Key=key,
            Body=file_bytes,
            **extra_args,
        )
        return object_key.strip("/")

    async def get_object(self, stored_path: str) -> bytes:
        response = await asyncio.to_thread(
            self.client.get_object,
            Bucket=self.bucket,
            Key=self._key(stored_path),
        )
        return await asyncio.to_thread(response["Body"].read)

    async def save_image(self, file_bytes: bytes, category: str = "exterior") -> str:
        file_id = f"{uuid4()}.jpg"
        return await self.save_object(file_bytes, f"{category}/{file_id}", "image/jpeg")

    async def create_presigned_get(self, object_key: str, expires_in: int | None = None) -> str:
        return await asyncio.to_thread(
            self.client.generate_presigned_url,
            "get_object",
            Params={"Bucket": self.bucket, "Key": self._key(object_key)},
            ExpiresIn=expires_in or S3_PRESIGNED_TTL,
        )

    async def create_presigned_put(self, object_key: str, content_type: str = "application/octet-stream") -> str:
        params = {"Bucket": self.bucket, "Key": self._key(object_key), "ContentType": content_type}
        return await asyncio.to_thread(
            self.client.generate_presigned_url,
            "put_object",
            Params=params,
            ExpiresIn=S3_PRESIGNED_TTL,
        )

    def public_url(self, stored_path: str | None) -> str | None:
        if not stored_path:
            return None
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": self._key(stored_path)},
            ExpiresIn=S3_PRESIGNED_TTL,
        )


if STORAGE_BACKEND == "s3":
    storage_provider = S3StorageProvider()
else:
    storage_provider = LocalStorageProvider()
