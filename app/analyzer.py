import json
import logging
import os
from datetime import datetime

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

_SYSTEM_PROMPT_CALL = """You are an expert call analyst for a local service business.
You analyze call transcripts from Google Local Services Ads leads.
Return ONLY a valid JSON object — no explanation, no markdown, just raw JSON."""

_SYSTEM_PROMPT_MESSAGE = """You are an expert lead analyst for a local service business.
You analyze message conversations from Google Local Services Ads leads.
Return ONLY a valid JSON object — no explanation, no markdown, just raw JSON."""


async def analyze_transcript(transcript: str, lead_metadata: dict = None) -> dict:
    if not transcript or not transcript.strip():
        return {
            "analysis_status": "failed",
            "error_message": "Empty transcript — nothing to analyze.",
        }

    is_message = (lead_metadata or {}).get("lead_type") == "message"

    context_parts = []
    if lead_metadata:
        if lead_metadata.get("caller_name"):
            context_parts.append(f"Caller name: {lead_metadata['caller_name']}")
        if lead_metadata.get("call_date"):
            context_parts.append(f"Date: {lead_metadata['call_date']}")
        if not is_message and lead_metadata.get("call_duration_seconds"):
            secs = lead_metadata["call_duration_seconds"]
            context_parts.append(f"Call duration: {secs // 60}m {secs % 60}s")

    context = "\n".join(context_parts)

    if is_message:
        content_label = "Message conversation"
        instructions = """Analyze this message lead and return a JSON object with these fields:
- was_answered (true/false): did the business reply to the customer's message?
- contact_name: the customer's first name if mentioned, otherwise null
- qualification_score (1-5): 5=ready to book, 1=spam/wrong number
- qualification_reason: 1-2 sentences explaining the score
- call_summary: 2-4 sentences summarizing the message exchange and any next steps
- service_requested: what service/job they were asking about (if mentioned)
- follow_up_required (true/false): does the business still need to respond or take action?
- follow_up_notes: what follow-up is needed (if any)"""
        system_prompt = _SYSTEM_PROMPT_MESSAGE
    else:
        content_label = "Call transcript"
        instructions = """Analyze this call and return a JSON object with these fields:
- was_answered (true/false): did a human actually speak, or was it voicemail/missed?
- contact_name: the customer's first name if mentioned in the call, otherwise null
- qualification_score (1-5): 5=ready to book, 1=spam/wrong number
- qualification_reason: 1-2 sentences explaining the score
- call_summary: 2-4 sentences of what was discussed and any next steps
- service_requested: what service/job they were asking about (if mentioned)
- follow_up_required (true/false): is there a clear action item needed?
- follow_up_notes: what follow-up is needed (if any)"""
        system_prompt = _SYSTEM_PROMPT_CALL

    user_message = f"""{"Metadata:\n" + context + "\n\n" if context else ""}{content_label}:
---
{transcript}
---

{instructions}"""

    logger.info(f"Sending {'message' if is_message else 'call'} to GPT-4o-mini for analysis...")

    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            max_tokens=1024,
            temperature=0.2,
        )

        raw = response.choices[0].message.content
        result = json.loads(raw)

        logger.info(f"Analysis complete. Score: {result.get('qualification_score')}/5")

        return {
            "is_answered": 1 if result.get("was_answered") else 0,
            "contact_name": result.get("contact_name") or None,
            "qualification_score": result.get("qualification_score"),
            "qualification_reason": result.get("qualification_reason"),
            "call_summary": result.get("call_summary"),
            "analysis_json": json.dumps(result),
            "analysis_status": "completed",
            "analyzed_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.exception(f"Analysis failed: {e}")
        return {
            "analysis_status": "failed",
            "error_message": f"Analysis error: {str(e)}",
        }
