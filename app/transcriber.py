import logging
import os
from datetime import datetime
from pathlib import Path

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Whisper supports up to 25 MB per file
MAX_FILE_SIZE_MB = 25


async def _label_speakers(transcript: str) -> str:
    """Use GPT-4o-mini to reformat a raw transcript with RECEPTIONIST/CALLER labels."""
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are formatting a phone call transcript for a personal injury law firm. "
                        "The call is between a law firm receptionist and a potential client (the caller). "
                        "Reformat the transcript so each speaker change starts on a new line, "
                        "prefixed with either 'RECEPTIONIST:' or 'CALLER:' based on context. "
                        "The receptionist typically answers the phone, gathers information, and explains next steps. "
                        "The caller typically describes their legal situation, injury, or accident. "
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


async def transcribe_audio(audio_path: str) -> dict:
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

    logger.info(f"Transcribing {path.name} ({file_size_mb:.2f} MB)...")

    try:
        with open(path, "rb") as f:
            response = await client.audio.transcriptions.create(
                model="whisper-1",
                file=f,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
                prompt=(
                    "This is a call to a personal injury law firm. "
                    "The caller may mention car accidents, workers' comp, settlements, "
                    "medical bills, attorneys, or insurance claims."
                ),
            )

        raw_transcript = response.text.strip()
        duration = getattr(response, "duration", None)

        transcript = await _label_speakers(raw_transcript)
        logger.info(f"Transcription complete: {len(transcript)} chars")
        return {
            "transcript": transcript,
            "transcription_status": "completed",
            "transcribed_at": datetime.utcnow().isoformat(),
            **({"call_duration_seconds": int(duration)} if duration else {}),
        }

    except Exception as e:
        logger.exception(f"Transcription failed for {audio_path}: {e}")
        return {
            "transcription_status": "failed",
            "error_message": f"Transcription error: {str(e)}",
        }
