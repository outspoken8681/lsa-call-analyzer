"""
app/webhook.py — Outbound webhook delivery for CRM integrations.

Each lead that completes analysis is pushed to the client's configured
webhook URL.  Deliveries are signed with HMAC-SHA256 so the receiver can
verify authenticity.
"""

import hashlib
import hmac
import json
import logging
from datetime import datetime as _dt, timezone

import httpx

logger = logging.getLogger(__name__)


# ── Signing ───────────────────────────────────────────────────────────────────

def sign_body(body: bytes, secret: str) -> str:
    """Return the HMAC-SHA256 hex digest of *body* using *secret*."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


# ── Payload ───────────────────────────────────────────────────────────────────

def build_payload(lead: dict, base_url: str) -> dict:
    """Assemble the JSON payload that is POST-ed to the CRM webhook."""
    ad = lead.get("analysis_data") or {}

    # Prefer cloud (R2) URL; fall back to a server-hosted URL when base_url known
    audio_url: str | None = lead.get("audio_url") or None
    if not audio_url and lead.get("audio_path") and base_url:
        audio_url = f"{base_url.rstrip('/')}/audio/{lead['id']}"

    return {
        "event":     "lead.analyzed",
        "timestamp": _dt.now(timezone.utc).isoformat(),
        "source":    "Google Local Services Ads",
        "lead": {
            "id":                   str(lead["id"]),
            "date":                 lead.get("call_date"),
            "type":                 lead.get("lead_type") or "phone",
            "caller_id":            lead.get("caller_phone"),
            "contact_name":         lead.get("contact_name") or lead.get("caller_name"),
            "location":             lead.get("location"),
            "job_type":             lead.get("job_type"),
            "answered":             (bool(lead["is_answered"])
                                     if lead.get("is_answered") is not None else None),
            "charge_status":        lead.get("charge_status"),
            "duration_seconds":     lead.get("call_duration_seconds"),
            "audio_url":            audio_url,
            "transcript":           lead.get("transcript"),
            "summary":              lead.get("call_summary"),
            "qualification_score":  lead.get("qualification_score"),
            "qualification_reason": lead.get("qualification_reason"),
            "service_requested":    ad.get("service_requested"),
            "follow_up_required":   bool(ad.get("follow_up_required", False)),
            "follow_up_notes":      ad.get("follow_up_notes"),
        },
    }


# ── Delivery ──────────────────────────────────────────────────────────────────

async def deliver(
    delivery_id: int,
    lead: dict,
    client: dict,
    base_url: str,
) -> tuple[bool, int | None, str]:
    """
    Attempt one HTTPS POST to the client's webhook URL.

    Returns
    -------
    (success, http_status_code, message)
      success     – True if the server responded 2xx.
      status_code – The HTTP status code, or None if the request never landed.
      message     – Response body excerpt (non-2xx) or exception string.
    """
    webhook_url    = (client.get("webhook_url") or "").strip()
    webhook_secret = (client.get("webhook_secret") or "").strip()

    if not webhook_url:
        return False, None, "No webhook URL configured for this client."

    payload = build_payload(lead, base_url)
    body    = json.dumps(payload, default=str).encode("utf-8")

    signature = f"sha256={sign_body(body, webhook_secret)}" if webhook_secret else ""
    ts        = str(int(_dt.now(timezone.utc).timestamp()))

    headers = {
        "Content-Type":        "application/json",
        "X-Webhook-Id":        str(delivery_id),
        "X-Webhook-Signature": signature,
        "X-Webhook-Timestamp": ts,
        "User-Agent":          "TripleTake-LSA-Analyzer/1.0",
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            response = await http.post(webhook_url, content=body, headers=headers)
        ok  = 200 <= response.status_code < 300
        msg = "" if ok else response.text[:500]
        return ok, response.status_code, msg
    except Exception as exc:
        return False, None, str(exc)[:500]
