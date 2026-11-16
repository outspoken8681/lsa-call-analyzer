"""
app/ads_api.py — Local Services leads read from the Google Ads API.

Why this exists: Google is moving Local Services Ads into the regular Google Ads
interface as a pay-per-lead Performance Max campaign. Once an account has moved,
its new leads stop appearing in the old Local Services inbox the scraper reads
(ads.google.com/localservices/inbox), so the scraper silently finds nothing.
The same leads — same lead IDs — are still served by the Google Ads API:

  local_services_lead               type, charged or not, caller name/phone, service
  local_services_lead_conversation  each call (duration + recording URL) and
                                    each message / SMS / email in the thread

The recording URL downloads as MP3 with the API's OAuth bearer token, so no
browser session is needed for these accounts.

The API serves these resources for accounts Google has NOT moved yet too, so
every client syncs from here once its `ads_customer_id` (the 10-digit Google Ads
customer id its leads live in) is known. That id is found automatically by
looking the client's own lead ids up across the manager's accounts
(find_customer_for_leads) — lead ids are identical in the inbox and the API —
or can be set by hand on the Clients page.

Credentials (env):
  GOOGLE_ADS_CLIENT_ID / GOOGLE_ADS_CLIENT_SECRET   OAuth client (AdForge's)
  GOOGLE_ADS_REFRESH_TOKEN                          a Google login with access
                                                    to the accounts (adwords scope)
  GOOGLE_ADS_LOGIN_CUSTOMER_ID                      the manager (MCC) the accounts
                                                    are reached through
No developer token: Google retired them in Sept 2026; access is granted to the
Cloud project that owns the OAuth client.
"""

import asyncio
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)

API_VERSION = os.getenv("GOOGLE_ADS_API_VERSION", "v26")
_TOKEN_URL = "https://oauth2.googleapis.com/token"

_token: dict = {"value": None, "expires": 0.0}
_accounts: dict = {"ids": None, "at": 0.0}
_tz_cache: dict[str, str] = {}


class AdsApiError(RuntimeError):
    pass


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def enabled() -> bool:
    return bool(_env("GOOGLE_ADS_CLIENT_ID") and _env("GOOGLE_ADS_CLIENT_SECRET")
                and _env("GOOGLE_ADS_REFRESH_TOKEN"))


def normalize_customer_id(cid: str | None) -> str | None:
    digits = re.sub(r"\D", "", cid or "")
    return digits if len(digits) == 10 else None


async def _access_token() -> str:
    if _token["value"] and time.time() < _token["expires"] - 120:
        return _token["value"]
    if not enabled():
        raise AdsApiError("Google Ads API credentials are not set (GOOGLE_ADS_CLIENT_ID / "
                          "GOOGLE_ADS_CLIENT_SECRET / GOOGLE_ADS_REFRESH_TOKEN).")
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(_TOKEN_URL, data={
            "client_id": _env("GOOGLE_ADS_CLIENT_ID"),
            "client_secret": _env("GOOGLE_ADS_CLIENT_SECRET"),
            "refresh_token": _env("GOOGLE_ADS_REFRESH_TOKEN"),
            "grant_type": "refresh_token",
        })
    if r.status_code != 200:
        raise AdsApiError(f"Google OAuth token refresh failed (HTTP {r.status_code}): {r.text[:200]}")
    j = r.json()
    _token["value"] = j["access_token"]
    _token["expires"] = time.time() + int(j.get("expires_in", 3600))
    return _token["value"]


def _api_error_message(r: httpx.Response) -> str:
    try:
        j = r.json()
        errs = (j.get("error", {}).get("details") or [{}])[0].get("errors") or []
        if errs:
            return "; ".join(e.get("message", "") for e in errs)
        return j.get("error", {}).get("message") or r.text[:300]
    except Exception:
        return r.text[:300]


async def search(customer_id: str, query: str) -> list[dict]:
    """Run a GAQL query and return every result row (follows pagination)."""
    token = await _access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    login = normalize_customer_id(_env("GOOGLE_ADS_LOGIN_CUSTOMER_ID"))
    if login and login != customer_id:
        headers["login-customer-id"] = login
    url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{customer_id}/googleAds:search"
    rows: list[dict] = []
    body: dict = {"query": query}
    async with httpx.AsyncClient(timeout=60) as c:
        while True:
            r = await c.post(url, headers=headers, json=body)
            if r.status_code != 200:
                raise AdsApiError(f"Google Ads API {r.status_code} for {customer_id}: {_api_error_message(r)}")
            j = r.json()
            rows.extend(j.get("results", []))
            nxt = j.get("nextPageToken")
            if not nxt:
                return rows
            body = {"query": query, "pageToken": nxt}


async def account_time_zone(customer_id: str) -> str:
    if customer_id not in _tz_cache:
        rows = await search(customer_id, "SELECT customer.time_zone FROM customer")
        _tz_cache[customer_id] = (rows[0].get("customer", {}).get("timeZone") if rows else None) or "America/New_York"
    return _tz_cache[customer_id]


# ── Normalization (pure) ──────────────────────────────────────────────────────

def parse_api_datetime(s: str | None, tz_name: str) -> Optional[datetime]:
    """'2026-10-30 11:11:36.213223' in the account's time zone → aware datetime."""
    if not s:
        return None
    s = s.strip()[:19]
    try:
        naive = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return naive.replace(tzinfo=ZoneInfo(tz_name))


def to_call_date(dt: Optional[datetime]) -> Optional[str]:
    """
    Store call_date the way the scraper always has: naive ISO in UTC
    (the scraper read the inbox in a UTC browser), e.g. '2026-10-30T15:11:00'.
    Seconds are dropped to match the inbox's minute precision.
    """
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(tzinfo=None, second=0, microsecond=0).isoformat()


def format_phone(raw: str | None) -> Optional[str]:
    """'+17706349254' → '(770) 634-9254' (the inbox's format); other shapes pass through."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return raw.strip() or None


def charge_status(lead: dict) -> str:
    """Map the API's charged / credit state onto the inbox's wording."""
    credit = ((lead.get("creditDetails") or {}).get("creditState") or "").upper()
    if credit == "CREDITED":
        return "Credited"
    if credit == "PENDING":
        return "In review"
    return "Charged" if lead.get("leadCharged") else "Not charged"


def service_label(service_id: str | None) -> Optional[str]:
    """'installation_fencing_pro' → 'Installation fencing pro'."""
    if not service_id:
        return None
    s = service_id.replace("_", " ").strip()
    return s[:1].upper() + s[1:] if s else None


def message_transcript(convs: list[dict], tz_name: str, consumer_name: str | None) -> Optional[str]:
    """
    Build the 'Sender (time): message' transcript the scraper produced from the
    inbox, from a lead's message/SMS/email conversation rows (oldest first).
    """
    lines = []
    for c in sorted(convs, key=lambda c: c.get("eventDateTime") or ""):
        text = ((c.get("messageDetails") or {}).get("text") or "").strip()
        if not text:
            continue
        who = (consumer_name or "Customer") if c.get("participantType") == "CONSUMER" else "Business"
        dt = parse_api_datetime(c.get("eventDateTime"), tz_name)
        when = dt.strftime("%b %d, %Y %I:%M %p").replace(" 0", " ") if dt else ""
        text = re.sub(r"\s+", " ", text)
        lines.append(f"{who} ({when}): {text}" if when else f"{who}: {text}")
    return "\n".join(lines) or None


def normalize_lead(lead: dict, convs: list[dict], tz_name: str) -> dict:
    """
    One API lead + its conversation rows → the scraper's lead-record shape, plus
    '_recording_url' (phone leads with a recording) for the caller to download.
    """
    contact = lead.get("contactDetails") or {}
    name = (contact.get("consumerName") or "").strip() or None
    is_message = (lead.get("leadType") or "").upper() in ("MESSAGE", "BOOKING")
    rec: dict = {
        "id": str(lead.get("id")),
        "lead_type": "message" if is_message else "phone",
        "caller_name": name,
        "caller_phone": format_phone(contact.get("phoneNumber")),
        "job_type": service_label(lead.get("serviceId")),
        "call_date": to_call_date(parse_api_datetime(lead.get("creationDateTime"), tz_name)),
        "charge_status": charge_status(lead),
    }
    if is_message:
        rec["transcript"] = message_transcript(
            [c for c in convs if c.get("conversationChannel") != "PHONE_CALL"], tz_name, name)
        return rec
    calls = sorted((c for c in convs if c.get("conversationChannel") == "PHONE_CALL"),
                   key=lambda c: c.get("eventDateTime") or "")
    with_rec = [c for c in calls if (c.get("phoneCallDetails") or {}).get("callRecordingUrl")]
    call = with_rec[0] if with_rec else (calls[0] if calls else None)
    if call:
        millis = (call.get("phoneCallDetails") or {}).get("callDurationMillis")
        if millis is not None:
            rec["call_duration_seconds"] = round(int(millis) / 1000)
        rec["_recording_url"] = (call.get("phoneCallDetails") or {}).get("callRecordingUrl")
    return rec


# ── Fetchers ──────────────────────────────────────────────────────────────────

async def fetch_leads(customer_id: str, days: int = 30, lead_id: str | None = None) -> list[dict]:
    """Normalized leads for the last `days` days (or one lead by id), newest first."""
    tz_name = await account_time_zone(customer_id)
    if lead_id:
        where = f"local_services_lead.id = {int(lead_id)}"
    else:
        since = (datetime.now(ZoneInfo(tz_name)) - timedelta(days=days)).strftime("%Y-%m-%d")
        where = f"local_services_lead.creation_date_time >= '{since}'"
    leads = await search(customer_id, f"""
        SELECT local_services_lead.id, local_services_lead.lead_type,
               local_services_lead.creation_date_time, local_services_lead.lead_charged,
               local_services_lead.lead_status, local_services_lead.service_id,
               local_services_lead.contact_details,
               local_services_lead.credit_details.credit_state
        FROM local_services_lead WHERE {where}
        ORDER BY local_services_lead.creation_date_time DESC""")
    if not leads:
        return []
    ids = ", ".join(f"'customers/{customer_id}/localServicesLeads/{r['localServicesLead']['id']}'" for r in leads)
    convs = await search(customer_id, f"""
        SELECT local_services_lead_conversation.lead,
               local_services_lead_conversation.conversation_channel,
               local_services_lead_conversation.participant_type,
               local_services_lead_conversation.event_date_time,
               local_services_lead_conversation.phone_call_details.call_duration_millis,
               local_services_lead_conversation.phone_call_details.call_recording_url,
               local_services_lead_conversation.message_details.text
        FROM local_services_lead_conversation
        WHERE local_services_lead_conversation.lead IN ({ids})""")
    by_lead: dict[str, list[dict]] = {}
    for r in convs:
        c = r.get("localServicesLeadConversation", {})
        by_lead.setdefault((c.get("lead") or "").rsplit("/", 1)[-1], []).append(c)
    return [normalize_lead(r["localServicesLead"], by_lead.get(str(r["localServicesLead"]["id"]), []), tz_name)
            for r in leads]


async def download_recording(url: str, attempts: int = 3) -> bytes:
    """MP3 bytes of a call recording. Google's attachment server returns an
    occasional HTTP 500 that succeeds on retry, so try a few times."""
    token = await _access_token()
    status = ""
    async with httpx.AsyncClient(timeout=60, follow_redirects=False) as c:
        for i in range(attempts):
            r = await c.get(url, headers={"Authorization": f"Bearer {token}"})
            ctype = r.headers.get("content-type", "")
            if r.status_code == 200 and "audio" in ctype:
                return r.content
            status = f"HTTP {r.status_code} ({ctype or 'no content-type'})"
            if r.status_code < 500:
                break
            await asyncio.sleep(2 * (i + 1))
    raise AdsApiError(f"Recording download failed: {status}")


async def manager_accounts() -> list[str]:
    """Enabled, non-manager accounts under GOOGLE_ADS_LOGIN_CUSTOMER_ID (cached 6 h)."""
    login = normalize_customer_id(_env("GOOGLE_ADS_LOGIN_CUSTOMER_ID"))
    if not login:
        return []
    if _accounts["ids"] is None or time.time() - _accounts["at"] > 6 * 3600:
        rows = await search(login, """
            SELECT customer_client.id FROM customer_client
            WHERE customer_client.manager = false AND customer_client.status = 'ENABLED'""")
        _accounts["ids"] = [str(r["customerClient"]["id"]) for r in rows]
        _accounts["at"] = time.time()
    return _accounts["ids"]


async def find_customer_for_leads(lead_ids: list[str]) -> Optional[str]:
    """
    The Google Ads account whose Local Services leads include these lead ids
    (a client's most recent ones), or None. When several accounts hold some of
    them, the one holding the most wins.
    """
    ids = [str(int(i)) for i in lead_ids if str(i).isdigit()]
    if not ids:
        return None
    best, best_n = None, 0
    for cid in await manager_accounts():
        try:
            rows = await search(cid, "SELECT local_services_lead.id FROM local_services_lead "
                                     f"WHERE local_services_lead.id IN ({', '.join(ids)})")
        except AdsApiError as e:
            logger.debug(f"Lead lookup skipped {cid}: {e}")
            continue
        if len(rows) > best_n:
            best, best_n = cid, len(rows)
            if best_n == len(ids):
                break
    return best


def _is_lsa_campaign(campaign: dict) -> bool:
    kind = campaign.get("advertisingChannelType")
    if kind == "LOCAL_SERVICES":
        return True
    return kind == "PERFORMANCE_MAX" and bool((campaign.get("pmaxCampaignSettings") or {}).get("localServicesEnabled"))


async def fetch_metrics(customer_id: str, start: date, end: date) -> dict:
    """
    Local Services impressions per day (account time zone) and total cost for
    start..end, summed across the old LocalServicesCampaign and the pay-per-lead
    Performance Max campaign that replaced it.
    """
    rows = await search(customer_id, f"""
        SELECT campaign.id, campaign.advertising_channel_type,
               campaign.pmax_campaign_settings.local_services_enabled,
               segments.date, metrics.impressions, metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
          AND campaign.advertising_channel_type IN ('LOCAL_SERVICES', 'PERFORMANCE_MAX')""")
    daily: dict[str, int] = {}
    cost_micros = 0
    for r in rows:
        if not _is_lsa_campaign(r.get("campaign", {})):
            continue
        d = r.get("segments", {}).get("date")
        m = r.get("metrics", {})
        daily[d] = daily.get(d, 0) + int(m.get("impressions") or 0)
        cost_micros += int(m.get("costMicros") or 0)
    return {"daily_impressions": daily, "cost": round(cost_micros / 1_000_000, 2)}
