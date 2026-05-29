"""
app/tokens.py — signed, time-limited access tokens.

Currently used to grant a CRM webhook consumer read-only access to a single
lead's audio recording via /audio/{lead_id}?token=... without an admin session.
The token is an itsdangerous-signed blob carrying the client_id + lead_id, so it
cannot be forged or pointed at a different lead.
"""

import os

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

_SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
_audio_signer = URLSafeTimedSerializer(_SECRET_KEY, salt="audio-access")

# How long a webhook-delivered audio link stays valid (seconds). 90 days gives
# a CRM ample time to fetch/cache the recording while still bounding exposure.
AUDIO_TOKEN_MAX_AGE = 86400 * 90


def make_audio_token(client_id: int, lead_id: str) -> str:
    return _audio_signer.dumps({"c": client_id, "l": str(lead_id)})


def verify_audio_token(token: str | None, lead_id: str,
                       max_age: int = AUDIO_TOKEN_MAX_AGE) -> int | None:
    """Return the client_id the token authorises for *lead_id*, or None if invalid."""
    if not token:
        return None
    try:
        data = _audio_signer.loads(token, max_age=max_age)
    except (BadSignature, SignatureExpired):
        return None
    if not isinstance(data, dict) or str(data.get("l")) != str(lead_id):
        return None
    return data.get("c")
