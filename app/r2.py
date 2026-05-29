"""
app/r2.py — Cloudflare R2 audio storage.

Upload audio after scraping; fetch on demand for serving.
Credentials come from environment variables so the same code
works locally and on Railway.
"""

import asyncio
import logging
import os
from typing import Optional

import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

R2_ENDPOINT = os.getenv("R2_ENDPOINT", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.getenv("R2_BUCKET", "lsa-audio")


def _enabled() -> bool:
    return bool(R2_ENDPOINT and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY)


def _client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


async def upload_audio(local_path: str, key: str) -> bool:
    """Upload a local audio file to R2. Returns True on success."""
    if not _enabled():
        logger.warning("R2 not configured — skipping upload")
        return False
    try:
        c = _client()
        await asyncio.to_thread(c.upload_file, local_path, R2_BUCKET, key)
        logger.info(f"R2 upload OK: {key}")
        return True
    except Exception as e:
        logger.error(f"R2 upload failed for {key}: {e}")
        return False


async def get_audio_bytes(key: str) -> Optional[bytes]:
    """Fetch audio bytes from R2. Returns None on failure."""
    if not _enabled():
        return None
    try:
        c = _client()

        def _fetch():
            resp = c.get_object(Bucket=R2_BUCKET, Key=key)
            return resp["Body"].read()

        return await asyncio.to_thread(_fetch)
    except Exception as e:
        logger.error(f"R2 fetch failed for {key}: {e}")
        return None
