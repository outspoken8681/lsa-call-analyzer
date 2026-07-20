"""
app/phone_lookup.py — caller phone reputation via IPQualityScore.

One HTTPS lookup per new lead (numbers are also cached on the lead row, so a
re-process never re-spends quota). Provider-specific bits are contained here so
IPQS can be swapped (e.g. for Twilio Lookup) without touching the pipeline.

Configured by the IPQS_API_KEY env var; when unset, lookups are silently
skipped and the rest of the spam scoring still works.
"""

import json
import logging
import os
import re
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# httpx logs full request URLs at INFO — ours embed the API key in the path,
# which must never land in server logs. Quiet it down.
logging.getLogger("httpx").setLevel(logging.WARNING)

IPQS_API_KEY = os.getenv("IPQS_API_KEY", "").strip()
_IPQS_URL = "https://www.ipqualityscore.com/api/json/phone/{key}/{number}"

_EXT_RE = re.compile(r'\bext\.?\s*\d+', re.I)

# When IPQS reports quota exhaustion, stop trying until this timestamp —
# every further request would just burn time and clutter logs.
_quota_blocked_until: float = 0.0


def enabled() -> bool:
    return bool(IPQS_API_KEY)


def normalize_phone(raw: str | None) -> tuple[Optional[str], bool]:
    """
    Return (digits_with_country_code or None, had_extension).

    Numbers carrying an "ext. NNNNN" suffix are Google's call-tracking numbers,
    NOT the caller — had_extension lets callers skip reputation lookups for
    those (they'd rate Google's VoIP infrastructure, not the customer).
    """
    if not raw:
        return None, False
    had_ext = bool(_EXT_RE.search(raw))
    digits = re.sub(r'\D', '', _EXT_RE.sub('', raw))
    if len(digits) == 10:
        digits = "1" + digits
    if len(digits) != 11 or not digits.startswith("1"):
        return None, had_ext
    return digits, had_ext


async def lookup_phone_reputation(raw_phone: str | None) -> Optional[dict]:
    """
    Rate a caller's number. Returns a compact dict of reputation fields, or
    None when disabled, unratable (no/invalid number), or when the number is a
    Google tracking number (extension present).
    """
    global _quota_blocked_until
    if not enabled():
        return None
    if time.time() < _quota_blocked_until:
        return None
    digits, had_ext = normalize_phone(raw_phone)
    if not digits:
        return None
    if had_ext:
        logger.info(f"[phone-lookup] {raw_phone!r} is a tracking number (ext) — skipping.")
        return None
    try:
        async with httpx.AsyncClient(timeout=20.0) as http:
            r = await http.get(
                _IPQS_URL.format(key=IPQS_API_KEY, number=digits),
                params={"country[]": "US"},
            )
        data = r.json()
    except Exception as e:
        logger.warning(f"[phone-lookup] IPQS request failed: {e}")
        return None
    if not data.get("success"):
        msg = data.get("message") or ""
        if "quota" in msg.lower():
            _quota_blocked_until = time.time() + 3600  # back off an hour
            logger.warning("[phone-lookup] IPQS daily quota exhausted — pausing lookups for 1h.")
        else:
            logger.warning(f"[phone-lookup] IPQS error: {msg}")
        return None
    return {
        "provider":     "ipqs",
        "number":       digits,
        "valid":        data.get("valid"),
        "active":       data.get("active"),
        "fraud_score":  data.get("fraud_score"),
        "recent_abuse": data.get("recent_abuse"),
        "spammer":      data.get("spammer"),
        "risky":        data.get("risky"),
        "voip":         data.get("VOIP"),
        "line_type":    data.get("line_type"),
        "carrier":      data.get("carrier"),
    }


def to_json(lookup: Optional[dict]) -> Optional[str]:
    return json.dumps(lookup) if lookup else None
