import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Whisper supports up to 25 MB per file
MAX_FILE_SIZE_MB = 25


DEFAULT_BUSINESS_TYPE = "local service business"


async def _label_speakers(transcript: str, business_type: str | None = None) -> str:
    """Use GPT-4o-mini to reformat a raw transcript with RECEPTIONIST/CALLER labels."""
    biz = (business_type or DEFAULT_BUSINESS_TYPE).strip() or DEFAULT_BUSINESS_TYPE
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        f"You are formatting a phone call transcript for a {biz}. "
                        "The call is between someone answering for the business (the receptionist) "
                        "and a potential customer (the caller). "
                        "Reformat the transcript so each speaker change starts on a new line, "
                        "prefixed with either 'RECEPTIONIST:' or 'CALLER:' based on context. "
                        "The receptionist typically answers the phone, gathers information, and explains next steps. "
                        "The caller typically describes the service or problem they need help with. "
                        "Return only the formatted transcript — no explanations, no extra text."
                    ),
                },
                {"role": "user", "content": transcript},
            ],
            temperature=0,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.warning(f"Speaker labeling failed, using raw transcript: {e}")
        return transcript


async def transcribe_audio(audio_path: str, business_type: str | None = None) -> dict:
    """
    Transcribe an audio file using OpenAI Whisper.
    Returns dict with transcript, transcription_status, and timestamps.
    """
    path = Path(audio_path)
    if not path.exists():
        return {
            "transcription_status": "failed",
            "error_message": f"Audio file not found: {audio_path}",
        }

    file_size_mb = path.stat().st_size / (1024 * 1024)
    if file_size_mb > MAX_FILE_SIZE_MB:
        return {
            "transcription_status": "failed",
            "error_message": f"Audio file too large: {file_size_mb:.1f} MB (max {MAX_FILE_SIZE_MB} MB)",
        }

    biz = (business_type or DEFAULT_BUSINESS_TYPE).strip() or DEFAULT_BUSINESS_TYPE
    logger.info(f"Transcribing {path.name} ({file_size_mb:.2f} MB) [{biz}]...")

    try:
        with open(path, "rb") as f:
            response = await client.audio.transcriptions.create(
                model="whisper-1",
                file=f,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
                prompt=(
                    f"This is a phone call to a {biz}. "
                    "The caller is a potential customer describing the service they need, "
                    "and may mention their name, address, phone number, and scheduling."
                ),
            )

        raw_transcript = response.text.strip()
        duration = getattr(response, "duration", None)

        transcript = await _label_speakers(raw_transcript, business_type)
        logger.info(f"Transcription complete: {len(transcript)} chars")
        return {
            "transcript": transcript,
            "transcription_status": "completed",
            "transcribed_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            **({"call_duration_seconds": int(duration)} if duration else {}),
        }

    except Exception as e:
        logger.exception(f"Transcription failed for {audio_path}: {e}")
        return {
            "transcription_status": "failed",
            "error_message": f"Transcription error: {str(e)}",
        }
