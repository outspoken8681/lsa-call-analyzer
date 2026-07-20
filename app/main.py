import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime as _datetime, timedelta, timezone as _timezone
from zoneinfo import ZoneInfo

_EASTERN = ZoneInfo("America/New_York")
from pathlib import Path
from urllib.parse import quote

import bcrypt
from dotenv import load_dotenv

load_dotenv()

from fastapi import BackgroundTasks, Depends, FastAPI, Form, HTTPException, Request, Response
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from app.analyzer import analyze_transcript
from app.database import (
    close_db,
    create_client,
    create_webhook_delivery,
    delete_client,
    delete_lead,
    get_all_clients,
    get_all_leads,
    get_client,
    get_client_by_slug,
    get_failed_webhook_count,
    get_lead,
    get_leads_count,
    get_pending_webhook_retries,
    get_webhook_deliveries_for_lead,
    get_webhook_delivery,
    get_setting_updated_at,
    get_daily_metrics,
    get_all_caller_phones,
    AUTH_STATE_KEY,
    init_db,
    load_auth_state,
    save_auth_state,
    update_client,
    update_lead,
    update_webhook_delivery,
    upsert_daily_metric,
    upsert_lead,
)
from app.webhook import deliver as webhook_deliver
from app.scraper import ensure_auth, open_login_browser, confirm_login, scrape_lead_audio, scrape_all_leads, run_diagnostics, get_lead_list, scrape_impressions_for_date, scrape_account_summary, AUTH_STATE_PATH
from app.phone_lookup import lookup_phone_reputation, normalize_phone, to_json as phone_lookup_to_json
from app.r2 import get_audio_bytes as r2_get_audio
from app.tokens import verify_audio_token
from app.transcriber import transcribe_audio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"

SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
SYNC_ENABLED = os.getenv("SYNC_ENABLED", "false").lower() == "true"
# Bearer token the local push_google_auth script uses to upload a fresh Google
# session. Defaults to ADMIN_PASSWORD so there's nothing extra to configure.
AUTH_UPLOAD_TOKEN = os.getenv("AUTH_UPLOAD_TOKEN", "") or ADMIN_PASSWORD
_signer = URLSafeTimedSerializer(SECRET_KEY)

# Pre-hash admin password at startup
_admin_hash = bcrypt.hashpw(ADMIN_PASSWORD.encode(), bcrypt.gensalt()) if ADMIN_PASSWORD else b""

# ── Scan-all state (in-memory, single-process) ────────────────────────────────
_scan_state: dict = {"running": False, "current": "", "done": [], "total": 0}

# ── Webhook constants ─────────────────────────────────────────────────────────
MAX_WEBHOOK_ATTEMPTS  = 5
WEBHOOK_RETRY_MINUTES = 10


# ── Login rate limiting (in-memory, single-process) ───────────────────────────
import time as _time

_LOGIN_MAX_ATTEMPTS = 5          # failures allowed within the window
_LOGIN_WINDOW_SEC   = 300        # rolling window for counting failures
_LOGIN_LOCKOUT_SEC  = 900        # lockout duration once tripped
_login_failures: dict[str, list[float]] = {}
_login_locked:   dict[str, float]       = {}


def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _login_lockout_remaining(key: str) -> int:
    """Return seconds remaining on an active lockout for *key*, else 0."""
    until = _login_locked.get(key)
    if until and until > _time.time():
        return int(until - _time.time())
    if until:
        _login_locked.pop(key, None)  # expired
    return 0


def _record_login_failure(key: str) -> None:
    now = _time.time()
    fails = [t for t in _login_failures.get(key, []) if now - t < _LOGIN_WINDOW_SEC]
    fails.append(now)
    _login_failures[key] = fails
    if len(fails) >= _LOGIN_MAX_ATTEMPTS:
        _login_locked[key] = now + _LOGIN_LOCKOUT_SEC
        _login_failures.pop(key, None)
        logger.warning(f"[login] {key} locked out for {_LOGIN_LOCKOUT_SEC}s after {len(fails)} failures.")


def _clear_login_failures(key: str) -> None:
    _login_failures.pop(key, None)
    _login_locked.pop(key, None)


# ── CSRF protection (double-submit cookie) ────────────────────────────────────
import hmac as _hmac
import secrets as _secrets

CSRF_COOKIE = "csrf_token"


def _csrf_valid(request: Request, submitted: str | None) -> bool:
    cookie = request.cookies.get(CSRF_COOKIE)
    return bool(cookie and submitted and _hmac.compare_digest(cookie, submitted))


async def _csrf_header(request: Request) -> None:
    """CSRF guard for fetch()/XHR calls — token travels in the X-CSRF-Token header."""
    if not _csrf_valid(request, request.headers.get("x-csrf-token")):
        raise HTTPException(status_code=403, detail="CSRF validation failed")


async def _csrf_form(request: Request, csrf_token: str = Form("")) -> None:
    """CSRF guard for HTML <form> posts — token travels in a hidden csrf_token field."""
    if not _csrf_valid(request, csrf_token):
        raise HTTPException(status_code=403, detail="CSRF validation failed")


def _sign(value: str) -> str:
    return _signer.dumps(value)


def _unsign(token: str | None, max_age: int = 86400 * 30) -> str | None:
    if not token:
        return None
    try:
        return _signer.loads(token, max_age=max_age)
    except (BadSignature, SignatureExpired):
        return None


def _get_admin_client_id(request: Request) -> int | None:
    token = request.cookies.get("admin_client_id")
    if not token:
        return None
    val = _unsign(token)
    try:
        return int(val) if val else None
    except (TypeError, ValueError):
        return None


def _is_admin(request: Request) -> bool:
    token = request.cookies.get("admin_session")
    return bool(_unsign(token))


def _portal_slug(request: Request) -> str | None:
    token = request.cookies.get("portal_session")
    return _unsign(token)


_scheduler = AsyncIOScheduler()


async def _scheduled_sync():
    """Triggered automatically 3× per day — syncs all clients that have a lead list URL."""
    if _scan_state["running"]:
        logger.info("[scheduler] Auto-sync skipped — scan already in progress.")
        return
    if not SYNC_ENABLED:
        return
    if not await ensure_auth():
        logger.info("[scheduler] Auto-sync skipped — not authenticated with Google.")
        return
    clients = await get_all_clients()
    eligible = [c for c in clients if c.get("lead_list_url")]
    if not eligible:
        logger.info("[scheduler] Auto-sync skipped — no clients configured.")
        return
    logger.info(f"[scheduler] Auto-sync starting for {len(eligible)} client(s)...")
    await _scan_all_clients_task(eligible)
    # Spend any leftover daily IPQS quota rating historical caller numbers.
    await _drip_phone_lookups()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()

    # Restore Google auth state. Railway's disk is ephemeral, so the durable copy
    # lives in the DB (uploaded via the local push_google_auth script). Prefer that;
    # fall back to the PLAYWRIGHT_AUTH_B64 env var only for first-time bootstrap.
    _auth_path = Path(os.getenv("AUTH_STATE_PATH", "auth.json"))
    _auth_path.parent.mkdir(parents=True, exist_ok=True)
    _db_auth = await load_auth_state()
    if _db_auth:
        _auth_path.write_text(_db_auth)
        logger.info("[auth] Google auth state restored from database.")
    else:
        _auth_b64 = os.getenv("PLAYWRIGHT_AUTH_B64", "").strip()
        if _auth_b64:
            import base64 as _b64
            _auth_path.write_bytes(_b64.b64decode(_auth_b64))
            logger.info("[auth] Google auth state restored from PLAYWRIGHT_AUTH_B64 (env bootstrap).")

    # Webhook retry checker — always active (works on Railway and locally)
    _scheduler.add_job(
        _process_webhook_retries, "interval", minutes=5,
        id="webhook_retries", misfire_grace_time=60,
    )
    if SYNC_ENABLED:
        # Every 2 hours, 8am–8pm Eastern (8,10,12,14,16,18,20 = 7 runs/day).
        # Pinned to Eastern because the server (Railway) runs in UTC.
        _scheduler.add_job(
            _scheduled_sync,
            CronTrigger(hour="8-20/2", minute=0, timezone=_EASTERN),
            id="auto_sync", misfire_grace_time=300,
        )
        logger.info("[scheduler] Auto-sync scheduled every 2 hours, 8 am–8 pm Eastern.")
    _scheduler.start()
    logger.info("[scheduler] Webhook retry checker active (every 5 min).")
    yield
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
    await close_db()


app = FastAPI(title="Triple Take", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@app.middleware("http")
async def _ensure_csrf_cookie(request: Request, call_next):
    """Issue a CSRF token cookie (readable by JS) whenever one isn't present."""
    response = await call_next(request)
    if not request.cookies.get(CSRF_COOKIE):
        response.set_cookie(
            CSRF_COOKIE, _secrets.token_urlsafe(32),
            samesite="lax", max_age=86400 * 30, path="/",
        )
    return response


def _fmt_call_date(date_str: str | None) -> str:
    """Format a call_date string (ISO or legacy) as 'Thu, May 22 at 3:14 PM'."""
    if not date_str:
        return "—"
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%m/%d/%y %I:%M %p",
        "%B %d, %Y %I:%M %p",  # after stripping " at "
        "%b %d, %Y %I:%M %p",  # abbreviated month (detail-page fallback)
    ]
    dt = None
    for fmt in formats:
        s = date_str.replace(" at ", " ")
        try:
            dt = _datetime.strptime(s, fmt)
            break
        except ValueError:
            continue
    if dt is None:
        return date_str  # fallback: show raw string
    day = dt.strftime("%a")
    month_day = dt.strftime("%b ") + str(dt.day)
    time = dt.strftime("%I:%M %p").lstrip("0")
    return f"{day}, {month_day} at {time}"


templates.env.filters["fmt_call_date"] = _fmt_call_date


def _google_account_id(lead_list_url: str | None) -> str | None:
    """Extract the Google customer ID (cid) from a client's lead list URL."""
    if not lead_list_url:
        return None
    from urllib.parse import parse_qs, urlparse
    cid = (parse_qs(urlparse(lead_list_url).query).get("cid") or [None])[0]
    return cid


templates.env.filters["google_account_id"] = _google_account_id


# ── Shared template context helpers ──────────────────────────────────────────

async def _admin_context(request: Request) -> dict:
    """Base context injected into every admin page."""
    clients = await get_all_clients()
    client_id = _get_admin_client_id(request)
    if not client_id and clients:
        client_id = clients[0]["id"]
    current_client = next((c for c in clients if c["id"] == client_id), None)
    base = BASE_URL or str(request.base_url).rstrip("/")
    return {
        "all_clients": clients,
        "current_client": current_client,
        "portal_mode": False,
        "base_url": base,
        "sync_enabled": SYNC_ENABLED,
    }


# ── Pipeline helpers ──────────────────────────────────────────────────────────

_UNKNOWN_NAMES = {"unknown caller", "unknown", ""}

def _best_caller_name(lead: dict) -> str | None:
    """
    Return the best Google-provided name for a lead, or None.
    - For phone leads: uses caller_name (rare) or skips — phone number is not a name.
    - For message leads: uses caller_name (new scraper) or falls back to caller_phone
      (old scraper stored name there by mistake).
    """
    name = (lead.get("caller_name") or "").strip()
    if name and name.lower() not in _UNKNOWN_NAMES:
        return name
    # Legacy: message leads scraped before the fix had name stored in caller_phone
    if lead.get("lead_type") == "message":
        phone_field = (lead.get("caller_phone") or "").strip()
        if phone_field and any(c.isalpha() for c in phone_field):
            return phone_field
    return None


async def _score_spam(client_id: int, lead_id: str) -> None:
    """
    Best-effort caller/spam rating for an analysed lead. Never raises.

    Blends three signals into a 0-100 spam_score (+ human-readable reasons):
      1. AI transcript classification (spam_likelihood in analysis_json)
      2. IPQS caller-number reputation (skipped for Google tracking numbers)
      3. Cross-account history — the same caller hitting multiple unrelated
         client accounts is a near-certain spam signal unique to our data.
    """
    try:
        lead = await get_lead(client_id, lead_id)
        if not lead:
            return
        reasons: list[str] = []
        score = 0

        # 1) AI transcript signal (primary evidence when a transcript exists)
        try:
            ad = json.loads(lead.get("analysis_json") or "{}")
        except Exception:
            ad = {}
        ai_spam = ad.get("spam_likelihood")
        if isinstance(ai_spam, (int, float)):
            score = max(score, int(ai_spam))
            if ai_spam >= 50:
                reasons.append(f"AI: {ad.get('spam_type') or 'spam'} ({int(ai_spam)}%)")

        # 2) Caller-number reputation (cached on the lead row; one lookup ever)
        lookup = None
        updates: dict = {}
        if lead.get("phone_lookup_json"):
            try:
                lookup = json.loads(lead["phone_lookup_json"])
            except Exception:
                lookup = None
        elif lead.get("caller_phone"):
            lookup = await lookup_phone_reputation(lead["caller_phone"])
            if lookup:
                updates["phone_lookup_json"] = phone_lookup_to_json(lookup)
        if lookup:
            if lookup.get("spammer") or lookup.get("recent_abuse"):
                score = max(score, 75)
                reasons.append("number flagged for abuse/spam (IPQS)")
            elif (lookup.get("fraud_score") or 0) >= 85:
                score = max(score, 50)
                reasons.append(f"high-risk number (IPQS {lookup['fraud_score']})")
            if lookup.get("valid") is False:
                score = max(score, 60)
                reasons.append("invalid phone number")

        # 3) Cross-account repeat caller
        digits, had_ext = normalize_phone(lead.get("caller_phone"))
        if digits and not had_ext:
            others = set()
            for cid, phone in await get_all_caller_phones():
                d, _ = normalize_phone(phone)
                if d == digits and cid != client_id:
                    others.add(cid)
            if others:
                score = min(100, score + 25)
                reasons.append(f"same caller seen on {len(others)} other account(s)")

        await update_lead(client_id, lead_id, {
            **updates,
            "spam_score": score,
            "spam_reasons": "; ".join(reasons) or None,
        })
        if score >= 50:
            logger.info(f"[spam] Lead {lead_id} scored {score}: {'; '.join(reasons)}")
    except Exception as e:
        logger.warning(f"[spam] Scoring failed for lead {lead_id}: {e}")


async def _drip_phone_lookups(max_lookups: int = 25) -> None:
    """
    Spend leftover daily IPQS quota rating historical leads that never got a
    number lookup (the free tier allows ~35/day; new leads use some, this
    drip uses the rest). Newest calls first, across all clients. Stops the
    moment quota is exhausted. Self-healing: once history is covered this
    no-ops. Never raises.
    """
    from app import phone_lookup as _pl
    if not _pl.enabled():
        return
    try:
        candidates: list[tuple[str, int, str]] = []  # (call_date, client_id, lead_id)
        for c in await get_all_clients():
            for l in await get_all_leads(c["id"], limit=1000, offset=0):
                if l.get("phone_lookup_json"):
                    continue
                digits, ext = _pl.normalize_phone(l.get("caller_phone"))
                if not digits or ext:
                    continue
                candidates.append((l.get("call_date") or "", c["id"], l["id"]))
        if not candidates:
            return
        candidates.sort(reverse=True)  # newest first
        done = 0
        for _, cid, lid in candidates[:max_lookups * 2]:  # headroom for failures
            if done >= max_lookups or _pl._quota_blocked_until > 0:
                break
            await _score_spam(cid, lid)
            lead = await get_lead(cid, lid)
            if lead and lead.get("phone_lookup_json"):
                done += 1
        logger.info(f"[spam] Lookup drip: rated {done} historical number(s), "
                    f"{len(candidates) - done} still pending.")
    except Exception as e:
        logger.warning(f"[spam] Lookup drip failed: {e}")


async def _process_lead(client_id: int, lead_id: str):
    """Full pipeline: scrape → (transcribe if phone) → analyze."""
    lead = await get_lead(client_id, lead_id)
    if not lead:
        logger.error(f"Lead {lead_id} not found for client {client_id}")
        return

    client = await get_client(client_id)
    if not client:
        return

    is_message = lead.get("lead_type") == "message"

    # ── Scrape step ───────────────────────────────────────────────────────────
    # For phone leads: download audio. For message leads: extract conversation.
    audio_on_disk = lead.get("audio_path") and Path(lead["audio_path"]).exists()
    needs_scrape = lead.get("scrape_status") != "completed" or (not is_message and not audio_on_disk)
    if needs_scrape:
        await update_lead(client_id, lead_id, {"scrape_status": "in_progress"})
        scrape_result = await scrape_lead_audio(client, lead_id, lead.get("lead_url", ""))
        await update_lead(client_id, lead_id, scrape_result)
        if scrape_result.get("scrape_status") != "completed":
            return
        lead = await get_lead(client_id, lead_id)

    # ── Transcription step (phone leads only) ─────────────────────────────────
    if not is_message and (lead.get("transcription_status") != "completed" or not lead.get("transcript")):
        audio_path = lead.get("audio_path")
        if not audio_path:
            await update_lead(client_id, lead_id, {
                "transcription_status": "failed",
                "error_message": "No audio path",
            })
            return
        await update_lead(client_id, lead_id, {"transcription_status": "in_progress"})
        transcription_result = await transcribe_audio(audio_path, client.get("business_type"))
        await update_lead(client_id, lead_id, transcription_result)
        if transcription_result.get("transcription_status") != "completed":
            return
        lead = await get_lead(client_id, lead_id)

    # ── Analysis step ─────────────────────────────────────────────────────────
    if lead.get("analysis_status") != "completed":
        await update_lead(client_id, lead_id, {"analysis_status": "in_progress"})
        analysis_result = await analyze_transcript(lead.get("transcript", ""), lead)
        google_name = _best_caller_name(lead)
        if google_name:
            analysis_result["contact_name"] = google_name
        await update_lead(client_id, lead_id, analysis_result)
        if analysis_result.get("analysis_status") == "completed":
            await _score_spam(client_id, lead_id)
            await _trigger_webhook_for_lead(client_id, lead_id)


async def _scrape_and_process_all(client: dict, max_leads: int = 50):
    """Scrape all leads for a client then transcribe + analyze."""
    client_id = client["id"]
    count_before = await get_leads_count(client_id)

    # ── Step 1: Quick table read — surface all leads as Pending immediately ──
    logger.info(f"[{client['slug']}] Reading lead list for quick preview...")
    try:
        basic_leads = await get_lead_list(client)
    except Exception as e:
        logger.warning(f"[{client['slug']}] Quick lead list read failed ({e}), continuing to full scrape anyway.")
        basic_leads = []

    for lead in basic_leads[:max_leads]:
        existing = await get_lead(client_id, lead["id"])
        if not existing:
            # New lead — save immediately so it shows in the UI as Pending
            await upsert_lead(client_id, lead)
            logger.info(f"[{client['slug']}] New lead queued: {lead['id']}")

    # ── Step 2: Full scrape — visits each lead's detail page + downloads audio ──
    # Build set of message leads already fully processed so scraper can skip them
    all_existing = await get_all_leads(client_id, limit=500, offset=0)
    done_message_ids = {
        str(l["id"]) for l in all_existing
        if l.get("lead_type") == "message"
        and l.get("analysis_status") == "completed"
        and l.get("transcript")
    }
    # Phone leads already fully processed — audio is in R2, so skip re-scrape
    # (re-scraping would reset their statuses and can fail, wiping a good lead).
    done_phone_ids = {
        str(l["id"]) for l in all_existing
        if l.get("lead_type") != "message"
        and l.get("analysis_status") == "completed"
        and l.get("transcript")
    }
    logger.info(f"[{client['slug']}] Starting full scrape (max {max_leads})...")

    async def _persist_and_process(lead: dict) -> None:
        """Save + process each lead the instant it is scraped, so a mid-scrape
        crash never discards work already done."""
        existing = await get_lead(client_id, lead["id"])
        already_done = existing and existing.get("analysis_status") == "completed"
        # If Google gave us a real name and contact_name not already set, copy it over
        google_name = _best_caller_name(lead)
        if google_name and not (existing and existing.get("contact_name")):
            lead["contact_name"] = google_name
        await upsert_lead(client_id, lead)
        if already_done:
            logger.info(f"Lead {lead['id']} already analyzed, skipping")
            return
        is_message = lead.get("lead_type") == "message"
        if lead.get("scrape_status") == "completed" and (lead.get("audio_path") or is_message):
            await _transcribe_and_analyze(client_id, lead["id"])

    try:
        await scrape_all_leads(client, max_leads=max_leads,
                               skip_message_ids=done_message_ids,
                               skip_phone_ids=done_phone_ids,
                               on_lead=_persist_and_process)
    except RuntimeError as e:
        logger.error(f"Scrape failed: {e}")
        return
    except Exception as e:
        # A mid-scrape crash no longer loses work — leads were persisted as they came.
        logger.exception(f"[{client['slug']}] Scrape ended early: {e}")

    count_after = await get_leads_count(client_id)
    new_leads = max(0, count_after - count_before)
    logger.info(f"[{client['slug']}] Full scrape complete. {new_leads} new lead(s).")

    # Accumulate today's new leads across all syncs; reset if it's a new day
    today = _datetime.now(_EASTERN).strftime("%Y-%m-%d")
    fresh = await get_client(client["id"])
    last_date = (fresh.get("last_synced_at") or "")[:10]
    daily_total = ((fresh.get("last_sync_new_leads") or 0) + new_leads) if last_date == today else new_leads

    await update_client(client["id"], {
        "last_synced_at": _datetime.now(_EASTERN).strftime("%Y-%m-%dT%H:%M:%S"),
        "last_sync_new_leads": daily_total,
    })

    # Capture yesterday's ad-impressions count (a completed day) — best-effort.
    await _capture_impressions(client, _datetime.now(_EASTERN).date() - timedelta(days=1))
    # Refresh the rolling 30-day spend / charged-leads snapshot — best-effort.
    await _capture_account_summary(client)


async def _capture_impressions(client: dict, target_date) -> None:
    """Best-effort: scrape and store one day's ad impressions. Never raises."""
    if not client.get("lead_list_url"):
        return
    try:
        imp = await scrape_impressions_for_date(client, target_date)
        if imp is not None:
            await upsert_daily_metric(client["id"], target_date.isoformat(), imp)
            logger.info(f"[{client['slug']}] Stored impressions {imp} for {target_date}.")
    except Exception as e:
        logger.warning(f"[{client['slug']}] Impressions capture failed for {target_date}: {e}")


async def _capture_account_summary(client: dict) -> None:
    """Best-effort: refresh the rolling 30-day spend + charged-leads totals. Never raises."""
    if not client.get("lead_list_url"):
        return
    try:
        summary = await scrape_account_summary(client)
        if summary and (summary.get("spend") is not None or summary.get("leads") is not None):
            await update_client(client["id"], {
                "r30_spend": summary.get("spend"),
                "r30_leads": summary.get("leads"),
                "r30_updated_at": _datetime.now(_EASTERN).strftime("%Y-%m-%dT%H:%M:%S"),
            })
            logger.info(f"[{client['slug']}] Stored 30-day summary: "
                        f"spend={summary.get('spend')} leads={summary.get('leads')}.")
    except Exception as e:
        logger.warning(f"[{client['slug']}] Account-summary capture failed: {e}")


async def _transcribe_and_analyze(client_id: int, lead_id: str):
    lead = await get_lead(client_id, lead_id)
    if not lead:
        return

    is_message = lead.get("lead_type") == "message"

    # Transcription — phone leads only
    if not is_message and (lead.get("transcription_status") != "completed" or not lead.get("transcript")):
        audio_path = lead.get("audio_path")
        if not audio_path:
            return
        client = await get_client(client_id)
        await update_lead(client_id, lead_id, {"transcription_status": "in_progress"})
        result = await transcribe_audio(audio_path, (client or {}).get("business_type"))
        await update_lead(client_id, lead_id, result)
        if result.get("transcription_status") != "completed":
            return
        lead = await get_lead(client_id, lead_id)

    if lead.get("analysis_status") != "completed":
        await update_lead(client_id, lead_id, {"analysis_status": "in_progress"})
        result = await analyze_transcript(lead.get("transcript", ""), lead)
        google_name = _best_caller_name(lead)
        if google_name:
            result["contact_name"] = google_name
        await update_lead(client_id, lead_id, result)
        if result.get("analysis_status") == "completed":
            await _score_spam(client_id, lead_id)
            await _trigger_webhook_for_lead(client_id, lead_id)


def _enrich_leads(leads: list[dict]) -> list[dict]:
    for lead in leads:
        if lead.get("analysis_json"):
            try:
                lead["analysis_data"] = json.loads(lead["analysis_json"])
            except Exception:
                lead["analysis_data"] = {}
    return leads


def _parse_call_date(date_str: str | None):
    """Parse a stored call_date string into a Python date object, or None."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                "%m/%d/%y %I:%M %p", "%B %d, %Y %I:%M %p", "%b %d, %Y %I:%M %p"):
        try:
            return _datetime.strptime(date_str.replace(" at ", " "), fmt).date()
        except ValueError:
            continue
    return None


async def _get_week_chart_data(client_id: int) -> tuple[str, str, bool]:
    """Return (chart_leads_json, weeks_json, has_impressions) for the past 6 Sun–Sat weeks."""
    today_et = _datetime.now(_EASTERN).date()
    days_since_sunday = (today_et.weekday() + 1) % 7
    week_start  = today_et - timedelta(days=days_since_sunday)   # Sunday of current week
    range_start = week_start - timedelta(weeks=5)                 # 6 weeks total
    range_end   = week_start + timedelta(days=6)

    all_recent = _enrich_leads(await get_all_leads(client_id, limit=500, offset=0))
    chart_leads = []
    for lead in all_recent:
        d = _parse_call_date(lead.get("call_date"))
        if not d or not (range_start <= d <= range_end):
            continue
        ad = lead.get("analysis_data") or {}
        chart_leads.append({
            "id":           str(lead["id"]),
            "date":         d.isoformat(),
            "lead_type":    lead.get("lead_type") or "phone",
            "is_answered":  lead.get("is_answered"),
            "caller_name":  lead.get("caller_name") or "",
            "contact_name": lead.get("contact_name") or "",
            "service":      ad.get("service_requested") or "",
        })

    impressions = await get_daily_metrics(
        client_id, range_start.isoformat(), range_end.isoformat()
    )

    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    all_weeks = []
    for w in range(6):   # w=0 oldest (5 weeks ago), w=5 current week
        ws = week_start - timedelta(weeks=5 - w)
        we = ws + timedelta(days=6)
        days = []
        for i in range(7):
            d = ws + timedelta(days=i)
            days.append({
                "date":        d.isoformat(),
                "day":         day_names[i],
                "md":          f"{d.month}/{d.day}",
                "is_today":    d == today_et,
                "impressions": impressions.get(d.isoformat()),
            })
        all_weeks.append({
            "week_start": ws.isoformat(),
            "week_end":   we.isoformat(),
            "days":       days,
        })

    has_impressions = any(v is not None for v in impressions.values())
    return json.dumps(chart_leads), json.dumps(all_weeks), has_impressions


# ── Webhook helpers ───────────────────────────────────────────────────────────

async def _trigger_webhook_for_lead(client_id: int, lead_id: str) -> None:
    """Fire-and-record a webhook push for a freshly analysed lead."""
    client = await get_client(client_id)
    if not client or not (client.get("webhook_url") or "").strip():
        return  # not configured

    # No duplicates — skip if a successful or in-flight delivery already exists
    existing = await get_webhook_delivery(client_id, lead_id)
    if existing and existing["status"] in ("success", "pending", "retrying"):
        logger.info(f"[webhook] Lead {lead_id}: delivery already '{existing['status']}', skipping.")
        return

    delivery = await create_webhook_delivery(client_id, lead_id)
    lead = await get_lead(client_id, lead_id)
    if not lead:
        return
    if lead.get("analysis_json"):
        try:
            lead["analysis_data"] = json.loads(lead["analysis_json"])
        except Exception:
            lead["analysis_data"] = {}

    base = BASE_URL or ""
    success, code, msg = await webhook_deliver(delivery["id"], lead, client, base)
    now_utc = _datetime.now(_timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    if success:
        await update_webhook_delivery(delivery["id"], {
            "status": "success", "attempts": 1,
            "last_attempt_at": now_utc, "response_code": code,
        })
        logger.info(f"[webhook] Lead {lead_id} → DELIVERED (HTTP {code}).")
    else:
        next_at = (_datetime.now(_timezone.utc) + timedelta(minutes=WEBHOOK_RETRY_MINUTES)).strftime("%Y-%m-%dT%H:%M:%S")
        await update_webhook_delivery(delivery["id"], {
            "status": "retrying", "attempts": 1,
            "last_attempt_at": now_utc, "response_code": code,
            "error_message": msg, "next_attempt_at": next_at,
        })
        logger.warning(f"[webhook] Lead {lead_id} → FAILED attempt 1: {msg}. Retry at {next_at}.")


async def _process_webhook_retries() -> None:
    """APScheduler job — retry webhook deliveries that are past due."""
    pending = await get_pending_webhook_retries()
    if not pending:
        return
    logger.info(f"[webhook] Processing {len(pending)} retry delivery(ies)...")
    for delivery in pending:
        client = await get_client(delivery["client_id"])
        lead   = await get_lead(delivery["client_id"], delivery["lead_id"])
        if not lead or not client:
            await update_webhook_delivery(delivery["id"], {
                "status": "failed", "error_message": "Lead or client no longer exists.",
            })
            continue
        if lead.get("analysis_json"):
            try:
                lead["analysis_data"] = json.loads(lead["analysis_json"])
            except Exception:
                lead["analysis_data"] = {}

        base    = BASE_URL or ""
        success, code, msg = await webhook_deliver(delivery["id"], lead, client, base)
        new_attempts = delivery["attempts"] + 1
        now_utc      = _datetime.now(_timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        if success:
            await update_webhook_delivery(delivery["id"], {
                "status": "success", "attempts": new_attempts,
                "last_attempt_at": now_utc, "response_code": code, "error_message": None,
            })
            logger.info(f"[webhook] Lead {delivery['lead_id']} → DELIVERED on attempt {new_attempts}.")
        elif new_attempts >= MAX_WEBHOOK_ATTEMPTS:
            await update_webhook_delivery(delivery["id"], {
                "status": "failed", "attempts": new_attempts,
                "last_attempt_at": now_utc, "response_code": code,
                "error_message": msg, "next_attempt_at": None,
            })
            logger.error(
                f"[webhook] Lead {delivery['lead_id']} → PERMANENTLY FAILED "
                f"after {new_attempts} attempts: {msg}"
            )
        else:
            next_at = (_datetime.now(_timezone.utc) + timedelta(minutes=WEBHOOK_RETRY_MINUTES)).strftime("%Y-%m-%dT%H:%M:%S")
            await update_webhook_delivery(delivery["id"], {
                "status": "retrying", "attempts": new_attempts,
                "last_attempt_at": now_utc, "response_code": code,
                "error_message": msg, "next_attempt_at": next_at,
            })
            logger.warning(
                f"[webhook] Lead {delivery['lead_id']} → FAILED attempt {new_attempts}, "
                f"retry at {next_at}."
            )


# ── Admin auth routes ─────────────────────────────────────────────────────────

@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    if _is_admin(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(request, "admin_login.html", {})


@app.post("/admin/login")
async def admin_login(request: Request, password: str = Form(...), _csrf: None = Depends(_csrf_form)):
    key = f"admin:{_client_ip(request)}"
    locked = _login_lockout_remaining(key)
    if locked:
        return templates.TemplateResponse(
            request, "admin_login.html",
            {"error": f"Too many attempts. Try again in {locked // 60 + 1} minute(s)."},
            status_code=429,
        )
    if ADMIN_PASSWORD and bcrypt.checkpw(password.encode(), _admin_hash):
        _clear_login_failures(key)
        response = RedirectResponse("/", status_code=302)
        response.set_cookie("admin_session", _sign("1"), httponly=True, samesite="lax", max_age=86400 * 30)
        return response
    _record_login_failure(key)
    return templates.TemplateResponse(request, "admin_login.html", {"error": "Incorrect password"}, status_code=401)


@app.post("/admin/logout")
async def admin_logout(_csrf: None = Depends(_csrf_form)):
    response = RedirectResponse("/admin/login", status_code=302)
    response.delete_cookie("admin_session")
    response.delete_cookie("admin_client_id")
    return response


# ── Admin: client management ──────────────────────────────────────────────────

@app.get("/admin/clients", response_class=HTMLResponse)
async def admin_clients(request: Request):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    ctx = await _admin_context(request)
    return templates.TemplateResponse(request, "admin_clients.html", ctx)


@app.post("/admin/clients")
async def admin_create_client(
    request: Request,
    name: str = Form(...),
    slug: str = Form(...),
    lead_list_url: str = Form(""),
    portal_password: str = Form(""),
    business_type: str = Form(""),
    google_account_id: str = Form(""),
    _csrf: None = Depends(_csrf_form),
):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    pw_hash = bcrypt.hashpw(portal_password.encode(), bcrypt.gensalt()).decode() if portal_password else None
    client = await create_client(name, slug.lower().strip(), lead_list_url or None, pw_hash)
    extra: dict = {}
    if portal_password:
        extra["portal_password_plain"] = portal_password
    if business_type.strip():
        extra["business_type"] = business_type.strip()
    if google_account_id.strip():
        extra["google_account_id"] = google_account_id.strip()
    if extra:
        await update_client(client["id"], extra)
    return RedirectResponse("/admin/clients", status_code=302)


@app.post("/admin/clients/{client_id}/update")
async def admin_update_client(
    request: Request,
    client_id: int,
    name: str = Form(...),
    slug: str = Form(...),
    lead_list_url: str = Form(""),
    portal_password: str = Form(""),
    webhook_url: str = Form(""),
    webhook_secret: str = Form(""),
    business_type: str = Form(""),
    google_account_id: str = Form(""),
    _csrf: None = Depends(_csrf_form),
):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    updates: dict = {
        "name":               name,
        "slug":               slug.lower().strip(),
        "lead_list_url":      lead_list_url or None,
        "webhook_url":        webhook_url.strip() or None,
        "business_type":      business_type.strip() or None,
        "google_account_id":  google_account_id.strip() or None,
    }
    if webhook_secret.strip():
        updates["webhook_secret"] = webhook_secret.strip()
    if portal_password:
        updates["portal_password"] = bcrypt.hashpw(portal_password.encode(), bcrypt.gensalt()).decode()
        updates["portal_password_plain"] = portal_password
    await update_client(client_id, updates)
    return RedirectResponse("/admin/clients", status_code=302)


@app.post("/admin/clients/{client_id}/delete")
async def admin_delete_client(request: Request, client_id: int, _csrf: None = Depends(_csrf_form)):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    await delete_client(client_id)
    return RedirectResponse("/admin/clients", status_code=302)


@app.post("/admin/clients/{client_id}/select")
async def admin_select_client(client_id: int, _csrf: None = Depends(_csrf_form)):
    response = RedirectResponse("/leads", status_code=302)
    response.set_cookie("admin_client_id", _sign(str(client_id)), httponly=True, samesite="lax", max_age=86400 * 30)
    return response


# ── Admin: dashboard ──────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    ctx = await _admin_context(request)
    if ctx["current_client"]:
        return RedirectResponse("/leads", status_code=302)
    return RedirectResponse("/admin/clients", status_code=302)


@app.get("/admin/scan-status")
async def scan_status(request: Request):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    return _scan_state


@app.post("/admin/scan-all")
async def scan_all_clients(request: Request, background_tasks: BackgroundTasks, _csrf: None = Depends(_csrf_header)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    if not await ensure_auth():
        raise HTTPException(status_code=401, detail="Not authenticated with Google.")
    if _scan_state["running"]:
        raise HTTPException(status_code=409, detail="Scan already in progress.")
    clients = await get_all_clients()
    eligible = [c for c in clients if c.get("lead_list_url")]
    if not eligible:
        raise HTTPException(status_code=400, detail="No clients have a lead list URL configured.")
    background_tasks.add_task(_scan_all_clients_task, eligible)
    return {"message": f"Scanning {len(eligible)} client(s) in background.", "total": len(eligible)}


async def _scan_all_clients_task(clients: list[dict]):
    _scan_state["running"] = True
    _scan_state["done"] = []
    _scan_state["total"] = len(clients)
    for client in clients:
        _scan_state["current"] = client["name"]
        logger.info(f"[scan-all] Scanning {client['name']}...")
        try:
            await _scrape_and_process_all(client)
        except Exception as e:
            logger.error(f"[scan-all] Error scanning {client['name']}: {e}")
        _scan_state["done"].append(client["name"])
        logger.info(f"[scan-all] Done with {client['name']} ({len(_scan_state['done'])}/{len(clients)})")
    _scan_state["running"] = False
    _scan_state["current"] = ""
    logger.info(f"[scan-all] All {len(clients)} client(s) scanned.")


@app.get("/leads", response_class=HTMLResponse)
async def dashboard(request: Request, page: int = 1):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)

    ctx = await _admin_context(request)
    current_client = ctx["current_client"]

    if not current_client:
        return RedirectResponse("/admin/clients", status_code=302)

    client_id = current_client["id"]

    # Parse multi-value filter params from query string
    filter_answered = request.query_params.getlist("answered") or None
    filter_charged = request.query_params.getlist("charged") or None
    search = (request.query_params.get("q") or "").strip() or None

    page_size = 25
    offset = (page - 1) * page_size
    leads = _enrich_leads(await get_all_leads(
        client_id, limit=page_size, offset=offset,
        filter_answered=filter_answered, filter_charged=filter_charged, search=search,
    ))
    total = await get_leads_count(client_id,
                                  filter_answered=filter_answered,
                                  filter_charged=filter_charged, search=search)
    is_authenticated = await ensure_auth()
    chart_leads_json, chart_days_json, has_impressions = await _get_week_chart_data(client_id)
    failed_webhooks = await get_failed_webhook_count(client_id)

    return templates.TemplateResponse(request, "index.html", {
        **ctx,
        "leads": leads,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
        "is_authenticated": is_authenticated,
        "filter_answered": filter_answered or [],
        "filter_charged": filter_charged or [],
        "search_query": search or "",
        "weekly_chart_leads_json": chart_leads_json,
        "weekly_chart_weeks_json": chart_days_json,
        "chart_has_impressions": has_impressions,
        "failed_webhook_count": failed_webhooks,
    })


@app.get("/leads/{lead_id}", response_class=HTMLResponse)
async def lead_detail(request: Request, lead_id: str):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)

    ctx = await _admin_context(request)
    current_client = ctx["current_client"]
    if not current_client:
        return RedirectResponse("/admin/clients", status_code=302)

    lead = await get_lead(current_client["id"], lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    if lead.get("analysis_json"):
        try:
            lead["analysis_data"] = json.loads(lead["analysis_json"])
        except Exception:
            lead["analysis_data"] = {}

    webhook_deliveries = await get_webhook_deliveries_for_lead(current_client["id"], lead_id)
    return templates.TemplateResponse(request, "lead.html", {
        **ctx,
        "lead": lead,
        "webhook_deliveries": webhook_deliveries,
    })


# ── Admin: auth flow ──────────────────────────────────────────────────────────

def _is_headless_server() -> bool:
    """True when running where no GUI browser can open (e.g. Railway)."""
    # A real desktop has DISPLAY (Linux) or is macOS/Windows. Railway has neither.
    return os.name == "posix" and not os.environ.get("DISPLAY") and sys.platform == "linux"


@app.post("/auth/login")
async def trigger_login(request: Request, background_tasks: BackgroundTasks, _csrf: None = Depends(_csrf_header)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    # The visible-browser login only works on a machine with a display. On the
    # hosted server there is no screen, so explain the real procedure instead of
    # pretending a window opened.
    if _is_headless_server():
        return JSONResponse({
            "message": (
                "This server has no display, so a Google login window can't open here. "
                "Run the local auth tool on your computer instead:  "
                "python scripts/push_google_auth.py — it opens Chromium, you log in, "
                "and it pushes the session straight to this app."
            ),
        }, status_code=409)
    background_tasks.add_task(open_login_browser)
    return JSONResponse({"message": "Browser opening — log in to Google, navigate to the account picker, then click Confirm."})


@app.post("/auth/confirm")
async def confirm_auth(request: Request, _csrf: None = Depends(_csrf_header)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    result = await confirm_login()
    # Persist whatever was just captured so it survives redeploys.
    if result.get("success"):
        try:
            await save_auth_state(Path(AUTH_STATE_PATH).read_text())
        except Exception as e:
            logger.warning(f"[auth] Could not persist auth state to DB: {e}")
    return JSONResponse(result)


@app.post("/auth/upload")
async def upload_auth(request: Request):
    """
    Accept a Google/Playwright auth-state JSON and install it (file + DB).
    Authenticated by a bearer token, so the local push_google_auth script can
    call it directly — no browser session or CSRF needed.
    """
    token = (request.headers.get("authorization", "") or "").removeprefix("Bearer ").strip()
    if not AUTH_UPLOAD_TOKEN or not _hmac.compare_digest(token, AUTH_UPLOAD_TOKEN):
        raise HTTPException(status_code=403, detail="Invalid or missing upload token.")
    raw = await request.body()
    try:
        data = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Body must be valid auth-state JSON.")
    if not isinstance(data, dict) or "cookies" not in data:
        raise HTTPException(status_code=400, detail="JSON missing 'cookies' — not a valid auth state.")

    text = json.dumps(data)
    Path(AUTH_STATE_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(AUTH_STATE_PATH).write_text(text)
    await save_auth_state(text)
    ok = await ensure_auth()
    logger.info(f"[auth] Auth state uploaded via /auth/upload (cookies={len(data.get('cookies', []))}).")
    return {"success": ok, "cookies": len(data.get("cookies", [])), "authenticated": ok}


@app.get("/auth/status")
async def auth_status(request: Request):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    return {
        "authenticated": await ensure_auth(),
        "last_updated": await get_setting_updated_at(AUTH_STATE_KEY),
        "headless_server": _is_headless_server(),
    }


# ── Admin: scrape + pipeline ──────────────────────────────────────────────────

@app.post("/scrape")
async def trigger_scrape(request: Request, background_tasks: BackgroundTasks, max_leads: int = 50, _csrf: None = Depends(_csrf_header)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    if not await ensure_auth():
        raise HTTPException(status_code=401, detail="Not authenticated.")
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    if not client:
        raise HTTPException(status_code=400, detail="No client selected.")
    if not client.get("lead_list_url"):
        raise HTTPException(status_code=400, detail="Client has no lead list URL configured.")
    background_tasks.add_task(_scrape_and_process_all, client, max_leads)
    return {"message": f"Scraping up to {max_leads} leads for {client['name']} in background."}


@app.post("/leads/backfill-names")
async def backfill_names(request: Request, background_tasks: BackgroundTasks, _csrf: None = Depends(_csrf_header)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    if not client:
        raise HTTPException(status_code=400, detail="No client selected.")
    background_tasks.add_task(_backfill_names_task, client["id"])
    return {"message": "Name backfill started — this may take a minute."}


async def _backfill_names_task(client_id: int):
    """
    For every lead:
      1. If Google gave a real name → always use it (overrides any partial AI-extracted name).
      2. Else if contact_name already set → skip (don't overwrite manually edited names).
      3. Else if transcript exists → ask AI for the name only (cheap, max 64 tokens).
    Each lead is processed independently so one failure never stops the rest.
    """
    import asyncio
    from app.analyzer import extract_contact_name
    leads = await get_all_leads(client_id, limit=1000, offset=0)
    logger.info(f"[client {client_id}] Name backfill: processing {len(leads)} leads...")
    updated = 0
    skipped = 0
    errors = 0
    for lead in leads:
        try:
            google_name = _best_caller_name(lead)
            if google_name:
                if lead.get("contact_name") != google_name:
                    await update_lead(client_id, lead["id"], {"contact_name": google_name})
                    updated += 1
                else:
                    skipped += 1
                continue
            # No Google name — only fill in if blank (don't overwrite manual edits)
            if lead.get("contact_name"):
                skipped += 1
                continue
            if lead.get("transcript"):
                await asyncio.sleep(0.3)  # avoid OpenAI rate limit
                name = await extract_contact_name(lead["transcript"], lead)
                if name:
                    await update_lead(client_id, lead["id"], {"contact_name": name})
                    updated += 1
                else:
                    skipped += 1
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            logger.warning(f"[client {client_id}] Name backfill error on lead {lead.get('id')}: {e}")
    logger.info(f"[client {client_id}] Name backfill complete — {updated} updated, {skipped} skipped, {errors} errors.")


@app.post("/leads/{lead_id}/process")
async def process_lead(request: Request, lead_id: str, background_tasks: BackgroundTasks, _csrf: None = Depends(_csrf_header)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    if not client:
        raise HTTPException(status_code=400, detail="No client selected.")
    lead = await get_lead(client["id"], lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    background_tasks.add_task(_process_lead, client["id"], lead_id)
    return {"message": f"Processing lead {lead_id} in background."}


@app.post("/leads/{lead_id}/reanalyze")
async def reanalyze_lead(request: Request, lead_id: str, background_tasks: BackgroundTasks, _csrf: None = Depends(_csrf_header)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    if not client:
        raise HTTPException(status_code=400, detail="No client selected.")
    lead = await get_lead(client["id"], lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    if not lead.get("transcript"):
        raise HTTPException(status_code=400, detail="No transcript yet — run full process first.")
    await update_lead(client["id"], lead_id, {"analysis_status": "pending"})

    async def _reanalyze():
        result = await analyze_transcript(lead["transcript"], lead)
        google_name = _best_caller_name(lead)
        if google_name:
            result["contact_name"] = google_name
        await update_lead(client["id"], lead_id, result)
        if result.get("analysis_status") == "completed":
            await _score_spam(client["id"], lead_id)

    background_tasks.add_task(_reanalyze)
    return {"message": "Re-analyzing lead in background."}


@app.post("/leads/{lead_id}/contact-name")
async def update_contact_name(request: Request, lead_id: str, _csrf: None = Depends(_csrf_header)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    if not client:
        raise HTTPException(status_code=400, detail="No client selected.")
    body = await request.json()
    name = (body.get("contact_name") or "").strip() or None
    await update_lead(client["id"], lead_id, {"contact_name": name})
    return {"contact_name": name}


@app.delete("/leads/{lead_id}")
async def remove_lead(request: Request, lead_id: str, _csrf: None = Depends(_csrf_header)):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    if not client:
        raise HTTPException(status_code=400, detail="No client selected.")
    lead = await get_lead(client["id"], lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    if lead.get("audio_path"):
        Path(lead["audio_path"]).unlink(missing_ok=True)
    await delete_lead(client["id"], lead_id)
    return {"message": f"Lead {lead_id} deleted"}


@app.get("/audio/{lead_id}")
async def serve_audio(request: Request, lead_id: str, download: bool = False, token: str | None = None):
    # Two ways in: a signed audio token (used by CRM webhook consumers) that
    # authorises one specific lead, or a logged-in admin session.
    client_id = verify_audio_token(token, lead_id) if token else None
    if client_id is None:
        if not _is_admin(request):
            raise HTTPException(status_code=403)
        ctx = await _admin_context(request)
        client = ctx["current_client"]
        if not client:
            raise HTTPException(status_code=400, detail="No client selected.")
        client_id = client["id"]
    lead = await get_lead(client_id, lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Audio not found")
    headers = {}
    if download:
        name = (lead.get("caller_name") or lead_id).replace(" ", "_")
        headers["Content-Disposition"] = f'attachment; filename="{name}.mp3"'
    path = Path(lead["audio_path"]) if lead.get("audio_path") else None
    if path and path.exists():
        return FileResponse(path, media_type="audio/mpeg", headers=headers)
    if lead.get("audio_url"):
        data = await r2_get_audio(lead["audio_url"])
        if data:
            return Response(content=data, media_type="audio/mpeg", headers=headers)
    raise HTTPException(status_code=404, detail="Audio not found")


# ── Read-only client portal ───────────────────────────────────────────────────

def _safe_portal_next(slug: str, nxt: str | None) -> str:
    """Validate a post-login redirect target — must be a path within this portal."""
    default = f"/portal/{slug}/leads"
    if nxt and nxt.startswith(f"/portal/{slug}/") and "//" not in nxt[1:] and "\\" not in nxt:
        return nxt
    return default


@app.get("/portal/{slug}", response_class=HTMLResponse)
async def portal_login_page(request: Request, slug: str, next: str = ""):
    client = await get_client_by_slug(slug)
    if not client or not client.get("portal_password"):
        raise HTTPException(status_code=404, detail="Portal not found")
    dest = _safe_portal_next(slug, next)
    # Already logged in?
    if _portal_slug(request) == slug:
        return RedirectResponse(dest, status_code=302)
    return templates.TemplateResponse(request, "portal_login.html", {"client": client, "next": dest})


@app.post("/portal/{slug}/login")
async def portal_login(request: Request, slug: str, password: str = Form(...), next: str = Form(""), _csrf: None = Depends(_csrf_form)):
    client = await get_client_by_slug(slug)
    if not client or not client.get("portal_password"):
        raise HTTPException(status_code=404, detail="Portal not found")
    dest = _safe_portal_next(slug, next)
    key = f"portal:{slug}:{_client_ip(request)}"
    locked = _login_lockout_remaining(key)
    if locked:
        return templates.TemplateResponse(
            request, "portal_login.html",
            {"client": client, "next": dest, "error": f"Too many attempts. Try again in {locked // 60 + 1} minute(s)."},
            status_code=429,
        )
    if bcrypt.checkpw(password.encode(), client["portal_password"].encode()):
        _clear_login_failures(key)
        response = RedirectResponse(dest, status_code=302)
        response.set_cookie("portal_session", _sign(slug), httponly=True, samesite="lax", max_age=86400 * 30)
        return response
    _record_login_failure(key)
    return templates.TemplateResponse(request, "portal_login.html", {"client": client, "next": dest, "error": "Incorrect password"}, status_code=401)


@app.get("/portal/{slug}/leads", response_class=HTMLResponse)
async def portal_leads(request: Request, slug: str, page: int = 1):
    if _portal_slug(request) != slug:
        return RedirectResponse(f"/portal/{slug}", status_code=302)
    # (deep links to a specific lead carry ?next= via portal_lead_detail)
    client = await get_client_by_slug(slug)
    if not client:
        raise HTTPException(status_code=404, detail="Portal not found")

    filter_answered = request.query_params.getlist("answered") or None
    filter_charged = request.query_params.getlist("charged") or None
    search = (request.query_params.get("q") or "").strip() or None

    page_size = 25
    offset = (page - 1) * page_size
    leads = _enrich_leads(await get_all_leads(
        client["id"], limit=page_size, offset=offset,
        filter_answered=filter_answered, filter_charged=filter_charged, search=search,
    ))
    total = await get_leads_count(client["id"],
                                  filter_answered=filter_answered,
                                  filter_charged=filter_charged, search=search)

    chart_leads_json, chart_days_json, has_impressions = await _get_week_chart_data(client["id"])

    return templates.TemplateResponse(request, "index.html", {
        "leads": leads,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
        "is_authenticated": False,
        "portal_mode": True,
        "portal_slug": slug,
        "current_client": client,
        "all_clients": [],
        "filter_answered": filter_answered or [],
        "filter_charged": filter_charged or [],
        "search_query": search or "",
        "weekly_chart_leads_json": chart_leads_json,
        "weekly_chart_weeks_json": chart_days_json,
        "chart_has_impressions": has_impressions,
    })


@app.get("/portal/{slug}/leads/{lead_id}", response_class=HTMLResponse)
async def portal_lead_detail(request: Request, slug: str, lead_id: str):
    if _portal_slug(request) != slug:
        return RedirectResponse(f"/portal/{slug}?next={quote(request.url.path)}", status_code=302)
    client = await get_client_by_slug(slug)
    if not client:
        raise HTTPException(status_code=404, detail="Portal not found")

    lead = await get_lead(client["id"], lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    if lead.get("analysis_json"):
        try:
            lead["analysis_data"] = json.loads(lead["analysis_json"])
        except Exception:
            lead["analysis_data"] = {}

    return templates.TemplateResponse(request, "lead.html", {
        "lead": lead,
        "portal_mode": True,
        "portal_slug": slug,
        "current_client": client,
        "all_clients": [],
    })


@app.get("/portal/{slug}/audio/{lead_id}")
async def portal_audio(request: Request, slug: str, lead_id: str, download: bool = False):
    if _portal_slug(request) != slug:
        raise HTTPException(status_code=403, detail="Not authenticated")
    client = await get_client_by_slug(slug)
    if not client:
        raise HTTPException(status_code=404)
    lead = await get_lead(client["id"], lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Audio not found")
    headers = {}
    if download:
        name = (lead.get("caller_name") or lead_id).replace(" ", "_")
        headers["Content-Disposition"] = f'attachment; filename="{name}.mp3"'
    path = Path(lead["audio_path"]) if lead.get("audio_path") else None
    if path and path.exists():
        return FileResponse(path, media_type="audio/mpeg", headers=headers)
    if lead.get("audio_url"):
        data = await r2_get_audio(lead["audio_url"])
        if data:
            return Response(content=data, media_type="audio/mpeg", headers=headers)
    raise HTTPException(status_code=404, detail="Audio not found")


# ── Debug ─────────────────────────────────────────────────────────────────────

@app.get("/debug/page")
async def debug_page(request: Request):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    if not await ensure_auth():
        raise HTTPException(status_code=401, detail="Not authenticated.")
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    url = client["lead_list_url"] if client and client.get("lead_list_url") else "https://ads.google.com/localservices/accountpicker"
    result = await run_diagnostics(url)
    return JSONResponse(result)


@app.get("/debug/screenshot")
async def debug_screenshot(request: Request, lead_id: str = None):
    if not _is_admin(request):
        raise HTTPException(status_code=403)
    # Guard against path traversal — lead IDs are numeric strings
    if lead_id and not lead_id.isalnum():
        raise HTTPException(status_code=400, detail="Invalid lead_id")
    path = Path(f"debug_lead_{lead_id}.png") if lead_id else Path("debug_screenshot.png")
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"No screenshot at {path}")
    return FileResponse(path, media_type="image/png")


@app.get("/admin/status")
async def admin_status(request: Request):
    """Operational snapshot — confirms whether auto-sync and Google auth are live."""
    if not _is_admin(request):
        raise HTTPException(status_code=403)

    google_ok = await ensure_auth()

    # Next scheduled auto-sync run times (only present when SYNC_ENABLED)
    next_runs = []
    for job in _scheduler.get_jobs():
        if job.id.startswith("auto_sync"):
            nrt = getattr(job, "next_run_time", None)
            if nrt:
                next_runs.append(nrt.isoformat())
    next_runs.sort()

    clients = await get_all_clients()
    client_status = [
        {
            "name":            c["name"],
            "slug":            c["slug"],
            "auto_sync_eligible": bool(c.get("lead_list_url")),
            "last_synced_at":  c.get("last_synced_at"),
            "last_sync_new_leads": c.get("last_sync_new_leads"),
            "webhook_configured": bool((c.get("webhook_url") or "").strip()),
        }
        for c in clients
    ]

    return {
        "sync_enabled":          SYNC_ENABLED,
        "google_authenticated":  google_ok,
        "auto_sync_active":      SYNC_ENABLED and google_ok,
        "next_auto_sync_runs":   next_runs,
        "scan_in_progress":      _scan_state["running"],
        "scan_current":          _scan_state.get("current") or None,
        "client_count":          len(clients),
        "clients":               client_status,
        "server_time":           _datetime.now(_timezone.utc).isoformat(),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}
