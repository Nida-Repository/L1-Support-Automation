"""MinIO Object Storage Service for Email Attachments.

Handles secure, resilient attachment uploads with unique object key generation,
automatic bucket validation, connection retry policy, and metadata extraction.
"""
from __future__ import annotations

import io
import logging
import os
import re
import time
from typing import Any, Optional
import uuid

from minio import Minio
from minio.error import S3Error
import urllib3

from config.settings import settings

logger = logging.getLogger(__name__)


class MinioServiceError(Exception):
    """Base exception for MinIO object storage operations."""
    pass


class MinioService:
    """Thread-safe MinIO object storage service with exponential backoff retries."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket_name: Optional[str] = None,
        secure: Optional[bool] = None,
    ) -> None:
        self.endpoint = endpoint or settings.minio_endpoint
        self.access_key = access_key or settings.minio_root_user
        self.secret_key = secret_key or settings.minio_root_password
        self.bucket_name = bucket_name or settings.automation_bucket
        self.secure = secure if secure is not None else settings.minio_secure

        logger.info(
            "Initializing MinIO client [Endpoint: %s | Secure: %s | Bucket: %s]",
            self.endpoint,
            self.secure,
            self.bucket_name,
        )

        self._client: Optional[Minio] = None
        self._bucket_checked = False

    def _get_client(self) -> Minio:
        """Lazily initialize and return the MinIO client."""
        if self._client is None:
            try:
                # Configure custom HTTP connection pooling
                http_client = urllib3.PoolManager(
                    timeout=urllib3.Timeout(connect=5.0, read=30.0),
                    maxsize=10,
                    retries=urllib3.Retry(
                        total=3,
                        backoff_factor=0.5,
                        status_forcelist=[500, 502, 503, 504],
                    ),
                )
                self._client = Minio(
                    self.endpoint,
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                    secure=self.secure,
                    http_client=http_client,
                )
                logger.debug("MinIO client instance created successfully.")
            except Exception as exc:
                logger.error("Failed to initialize MinIO client: %s", exc)
                raise MinioServiceError(f"MinIO client initialization failed: {exc}") from exc
        return self._client

    def ensure_bucket_exists(self) -> None:
        """Verify that the target bucket exists, or create it if absent."""
        if self._bucket_checked:
            return

        client = self._get_client()
        try:
            if not client.bucket_exists(self.bucket_name):
                logger.warning("Bucket '%s' does not exist. Attempting creation...", self.bucket_name)
                client.make_bucket(self.bucket_name)
                logger.info("Successfully created MinIO bucket '%s'", self.bucket_name)
            else:
                logger.debug("Verified MinIO bucket '%s' exists.", self.bucket_name)
            self._bucket_checked = True
        except (S3Error, urllib3.exceptions.HTTPError, Exception) as exc:
            logger.error("Error verifying or creating bucket '%s': %s", self.bucket_name, exc)
            raise MinioServiceError(f"Bucket check/creation failed for '{self.bucket_name}': {exc}") from exc

    @staticmethod
    def sanitize_filename(file_name: Optional[str]) -> str:
        """Sanitize attachment filename to prevent directory traversal and invalid chars."""
        if not file_name:
            return "unnamed_attachment.bin"
        # Strip path traversal elements
        base = os.path.basename(file_name.strip())
        # Replace non-alphanumeric/safe characters with underscores
        cleaned = re.sub(r"[^a-zA-Z0-9._-]", "_", base)
        return cleaned or "attachment.bin"

    def upload_attachment(
        self,
        *,
        alert_id: int,
        thread_id: int,
        file_name: str,
        file_data: bytes,
        content_type: Optional[str] = None,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """Upload attachment bytes to MinIO with retry policy and structured object key.

        Object Key Pattern:
            {alert_id}/{thread_id}/{unique_prefix}_{sanitized_file_name}

        Returns:
            dict containing object_key, bucket_name, file_size, file_type, file_name, and etag.
        """
        self.ensure_bucket_exists()

        sanitized_name = self.sanitize_filename(file_name)
        unique_prefix = uuid.uuid4().hex[:10]
        object_key = f"{alert_id}/{thread_id}/{unique_prefix}_{sanitized_name}"
        file_size = len(file_data)
        mime_type = content_type or "application/octet-stream"

        logger.info(
            "Uploading attachment to MinIO [Bucket: %s | Key: %s | Size: %d bytes | Type: %s]",
            self.bucket_name,
            object_key,
            file_size,
            mime_type,
        )

        client = self._get_client()
        last_exception: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            try:
                data_stream = io.BytesIO(file_data)
                result = client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=object_key,
                    data=data_stream,
                    length=file_size,
                    content_type=mime_type,
                )
                etag = getattr(result, "etag", None)
                if etag:
                    etag = etag.strip('"')

                logger.info(
                    "Successfully uploaded object to MinIO [Key: %s | ETag: %s | Attempt %d/%d]",
                    object_key,
                    etag,
                    attempt,
                    max_retries,
                )
                return {
                    "object_key": object_key,
                    "bucket_name": self.bucket_name,
                    "file_size": file_size,
                    "file_type": mime_type,
                    "file_name": sanitized_name,
                    "etag": etag,
                }
            except (S3Error, urllib3.exceptions.HTTPError, OSError, Exception) as exc:
                last_exception = exc
                logger.warning(
                    "MinIO upload failed for '%s' (Attempt %d/%d): %s",
                    object_key,
                    attempt,
                    max_retries,
                    exc,
                )
                if attempt < max_retries:
                    backoff = 0.5 * (2 ** (attempt - 1))
                    time.sleep(backoff)

        logger.critical("All %d MinIO upload attempts failed for '%s'", max_retries, object_key)
        raise MinioServiceError(
            f"Failed to upload attachment '{object_key}' to MinIO after {max_retries} attempts: {last_exception}"
        ) from last_exception

    def get_object_stream(
        self,
        *,
        object_key: str,
        bucket_name: Optional[str] = None,
    ) -> tuple[Any, str, int]:
        """Stream an object from MinIO by its object key.

        Returns:
            Tuple of (response_stream, content_type, content_length).
            The caller is responsible for closing the stream after use.

        Raises:
            MinioServiceError: If the object does not exist or cannot be fetched.
        """
        target_bucket = bucket_name or self.bucket_name
        client = self._get_client()
        try:
            response = client.get_object(
                bucket_name=target_bucket,
                object_name=object_key,
            )
            content_type = response.headers.get("Content-Type", "application/octet-stream")
            content_length = int(response.headers.get("Content-Length", 0))
            logger.info(
                "Opened MinIO stream [Bucket: %s | Key: %s | Size: %d | Type: %s]",
                target_bucket,
                object_key,
                content_length,
                content_type,
            )
            return response, content_type, content_length
        except S3Error as exc:
            logger.error(
                "MinIO S3Error fetching object '%s' from bucket '%s': %s",
                object_key, target_bucket, exc,
            )
            raise MinioServiceError(
                f"Object '{object_key}' not found or inaccessible in bucket '{target_bucket}': {exc}"
            ) from exc
        except Exception as exc:
            logger.error("Unexpected error fetching MinIO object '%s': %s", object_key, exc)
            raise MinioServiceError(f"Failed to stream object '{object_key}': {exc}") from exc


# Default singleton instance
minio_service = MinioService()