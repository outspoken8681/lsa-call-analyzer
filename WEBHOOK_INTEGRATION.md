# CRM Webhook Integration Guide

**Provided by:** Triple Take Marketing — Google LSA Analyzer  
**Version:** 1.1

---

## Overview

The LSA Analyzer automatically pushes fully analysed lead records to your CRM as soon as analysis completes.  Each delivery is a single HTTPS `POST` to a webhook URL you configure.  Deliveries are signed so you can verify they genuinely came from us.

> **Integration model — push, not pull.** This is a *push* integration: **you** expose an HTTPS endpoint, and **we** `POST` each new lead to it. There is no polling API to query on your side — you receive data as it is produced. The only request you make back to us is an optional `GET` to download a call recording (see `audio_url` below), and that link is pre-authorised by a token embedded in the payload.

---

## Quick-Start Checklist

1. Create a public HTTPS endpoint in your CRM that accepts `POST` requests.
2. Share the URL with Triple Take Marketing; they configure it per client account.
3. Triple Take generates a **shared secret** and shares it with you securely.
4. Verify the `X-Webhook-Signature` header on every incoming request (see below).
5. Respond `2xx` within 30 seconds to acknowledge receipt.

---

## Authentication — HMAC-SHA256 Signature

Every request includes three security headers:

| Header | Example | Purpose |
|---|---|---|
| `X-Webhook-Signature` | `sha256=a3f8c2...` | HMAC-SHA256 of the raw request body |
| `X-Webhook-Timestamp` | `1717000000` | Unix epoch (UTC) when the request was sent |
| `X-Webhook-Id` | `42` | Unique delivery ID (integer) |

### How to verify (pseudocode)

```
expected = "sha256=" + HMAC_SHA256(shared_secret, raw_request_body)
if constant_time_compare(request.header("X-Webhook-Signature"), expected):
    # authentic — process the payload
else:
    # reject — return 401
```

### Code examples

**Python**
```python
import hashlib, hmac

def verify(body: bytes, secret: str, signature_header: str) -> bool:
    expected = "sha256=" + hmac.new(
        secret.encode(), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)
```

**Node.js**
```javascript
const crypto = require('crypto');

function verify(bodyBuffer, secret, signatureHeader) {
  const expected = 'sha256=' + crypto
    .createHmac('sha256', secret)
    .update(bodyBuffer)
    .digest('hex');
  return crypto.timingSafeEqual(
    Buffer.from(expected),
    Buffer.from(signatureHeader)
  );
}
```

**PHP**
```php
function verify(string $body, string $secret, string $signatureHeader): bool {
    $expected = 'sha256=' . hash_hmac('sha256', $body, $secret);
    return hash_equals($expected, $signatureHeader);
}
```

> **Important:** Always use a constant-time comparison function (`hmac.compare_digest`, `crypto.timingSafeEqual`, `hash_equals`) to prevent timing attacks.

> **Note:** The signature is computed only when a shared secret has been configured for your account (it always is for production integrations). If `X-Webhook-Signature` is ever empty or absent, treat the request as **unverified and reject it**. Confirm with Triple Take that your secret is set before going live.

---

## Request Format

```
POST <your-webhook-url>
Content-Type: application/json
X-Webhook-Signature: sha256=<hex>
X-Webhook-Timestamp: <unix-epoch>
X-Webhook-Id: <integer>
User-Agent: TripleTake-LSA-Analyzer/1.0
```

---

## Payload Schema

```json
{
  "event": "lead.analyzed",
  "timestamp": "2026-05-28T21:00:00.000000+00:00",
  "source": "Google Local Services Ads",
  "lead": {
    "id": "314161249",
    "date": "2026-05-20T11:17:00",
    "type": "phone",
    "caller_id": "(407) 970-1161",
    "contact_name": "John Smith",
    "location": "Marietta",
    "job_type": "Roofing",
    "answered": false,
    "charge_status": "Not charged",
    "duration_seconds": null,
    "audio_url": "https://lsa.tripletakemarketing.com/audio/314161249?token=eyJjIjoxLCJsIjoiMzE0MTYxMjQ5In0.aBcD...",
    "transcript": "...",
    "summary": "Missed call — no recording available.",
    "qualification_score": null,
    "qualification_reason": null,
    "service_requested": "Roof inspection",
    "follow_up_required": false,
    "follow_up_notes": null
  }
}
```

### Field Reference

| Field | Type | Description |
|---|---|---|
| `event` | string | Always `"lead.analyzed"` |
| `timestamp` | ISO 8601 string | UTC time the push was made |
| `source` | string | Always `"Google Local Services Ads"` |
| **Lead fields** | | |
| `lead.id` | string | Google LSA lead ID |
| `lead.date` | string \| null | Call/message date (ISO 8601) |
| `lead.type` | string | `"phone"` or `"message"` |
| `lead.caller_id` | string \| null | Caller's phone number |
| `lead.contact_name` | string \| null | Name extracted from transcript or provided by Google |
| `lead.location` | string \| null | City/area from Google |
| `lead.job_type` | string \| null | Job category from Google |
| `lead.answered` | boolean \| null | Whether the call was answered (`null` for messages) |
| `lead.charge_status` | string \| null | Google charge status (e.g. `"Charged"`, `"Not charged"`) |
| `lead.duration_seconds` | integer \| null | Call duration in seconds |
| `lead.audio_url` | string \| null | Pre-authorised HTTPS URL to the call recording. A plain `GET` returns the MP3 (`Content-Type: audio/mpeg`) — no auth headers needed; the embedded `token` grants read access to this one lead's audio and is valid for **90 days**. `null` when there is no recording (e.g. a missed call or a message lead). Append `&download=true` to receive it as a file attachment. |
| `lead.transcript` | string \| null | Full call transcript or message thread |
| `lead.summary` | string \| null | One-paragraph AI summary of the call |
| `lead.qualification_score` | integer \| null | AI score 1–5 (5 = highest quality lead) |
| `lead.qualification_reason` | string \| null | AI explanation of the score |
| `lead.service_requested` | string \| null | AI-extracted service type (e.g. `"Roof replacement"`) |
| `lead.follow_up_required` | boolean | Whether AI flagged this lead for follow-up |
| `lead.follow_up_notes` | string \| null | AI notes on recommended follow-up action |

---

## Events

Currently only one event type is delivered:

| Event | Trigger |
|---|---|
| `lead.analyzed` | Fired once, immediately after AI analysis of a lead completes |

---

## Responding to Deliveries

- Return any **2xx status code** (200, 201, 204, etc.) to acknowledge receipt.
- The response body is ignored.
- You must respond within **30 seconds**; the request will time out otherwise.
- Do **not** return a 2xx if you are unable to process the record — return a 4xx or 5xx so the delivery is retried.

---

## Retry Policy

If your endpoint returns a non-2xx response or the request times out, the delivery is automatically retried:

| Attempt | Timing |
|---|---|
| 1 | Immediately after analysis completes |
| 2 | 10 minutes after attempt 1 |
| 3 | 10 minutes after attempt 2 |
| 4 | 10 minutes after attempt 3 |
| 5 | 10 minutes after attempt 4 |

After 5 failed attempts the delivery is marked **permanently failed** and an alert is shown in the admin dashboard.  A Triple Take administrator must resolve the issue and re-trigger delivery manually if needed.

**Idempotency:** The `X-Webhook-Id` header identifies the delivery record; it stays the **same** across automatic retries of the same lead (it does not change per attempt).  We never re-deliver a lead once a prior attempt has succeeded, so use **`lead.id`** as your idempotency key.

---

## Duplicate Prevention

The LSA Analyzer tracks every delivery.  Once a lead is successfully delivered it will never be pushed again, even if the lead is re-analysed.

---

## Security Best Practices

1. **Always verify the signature** before processing any payload.
2. **Compare timestamps** — reject requests where `X-Webhook-Timestamp` is more than 5 minutes old to prevent replay attacks.
3. **Use HTTPS only** — the sender will refuse to deliver to plain HTTP URLs.
4. **Rotate your secret** periodically. Contact Triple Take to update the configured secret.
5. **Allowlist our IP** if your firewall requires it — contact Triple Take for current egress IPs.

---

## Timestamp Replay-Attack Check (recommended)

```python
import time

def is_fresh(timestamp_header: str, tolerance_seconds: int = 300) -> bool:
    try:
        ts = int(timestamp_header)
        return abs(time.time() - ts) <= tolerance_seconds
    except (ValueError, TypeError):
        return False
```

---

## Testing Your Endpoint

You can use [webhook.site](https://webhook.site) or [ngrok](https://ngrok.com) to inspect incoming payloads during development.

**Sample curl to simulate a delivery:**

```bash
SECRET="your-shared-secret"
BODY='{"event":"lead.analyzed","timestamp":"2026-01-01T00:00:00+00:00","source":"Google Local Services Ads","lead":{"id":"test-001","date":null,"type":"phone","caller_id":"(555) 000-0001","contact_name":"Test User","location":"Atlanta","job_type":"Roofing","answered":true,"charge_status":"Charged","duration_seconds":120,"audio_url":null,"transcript":"Test transcript.","summary":"Test call.","qualification_score":4,"qualification_reason":"Qualified lead.","service_requested":"Roof repair","follow_up_required":false,"follow_up_notes":null}}'

SIG="sha256=$(echo -n "$BODY" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $2}')"
TS=$(date +%s)

curl -X POST https://your-crm.example.com/webhooks/leads \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Signature: $SIG" \
  -H "X-Webhook-Timestamp: $TS" \
  -H "X-Webhook-Id: 0" \
  -H "User-Agent: TripleTake-LSA-Analyzer/1.0" \
  -d "$BODY"
```

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Signature mismatch | Using the parsed/decoded body instead of the raw request bytes |
| All deliveries time out | Webhook URL is not publicly reachable or firewall is blocking |
| 401 / 403 from your CRM | CRM requires additional authentication headers — contact Triple Take |
| Duplicate records | Not deduplicating on `lead.id` in your CRM |
| `audio_url` is `null` | Lead was a missed call/voicemail with no recording, or a message lead (text), which has no audio |
| `GET` on `audio_url` returns `403` | The `token` query param was dropped or altered — fetch the full URL exactly as delivered, unmodified |
| `GET` on `audio_url` returns `404` | The recording is no longer on the server (rare) — contact Triple Take to re-source it |

---

## Contact

For webhook configuration changes, secret rotation, or support, contact **Triple Take Marketing**.
