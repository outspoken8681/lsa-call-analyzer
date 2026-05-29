import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime as _datetime, timedelta
from zoneinfo import ZoneInfo

_EASTERN = ZoneInfo("America/New_York")
from pathlib import Path

import bcrypt
from dotenv import load_dotenv

load_dotenv()

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Request, Response
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
    init_db,
    update_client,
    update_lead,
    update_webhook_delivery,
    upsert_lead,
)
from app.webhook import deliver as webhook_deliver
from app.scraper import ensure_auth, open_login_browser, confirm_login, scrape_lead_audio, scrape_all_leads, run_diagnostics, get_lead_list
from app.r2 import get_audio_bytes as r2_get_audio
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
_signer = URLSafeTimedSerializer(SECRET_KEY)

# Pre-hash admin password at startup
_admin_hash = bcrypt.hashpw(ADMIN_PASSWORD.encode(), bcrypt.gensalt()) if ADMIN_PASSWORD else b""

# ── Scan-all state (in-memory, single-process) ────────────────────────────────
_scan_state: dict = {"running": False, "current": "", "done": [], "total": 0}

# ── Webhook constants ─────────────────────────────────────────────────────────
MAX_WEBHOOK_ATTEMPTS  = 5
WEBHOOK_RETRY_MINUTES = 10


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    # Webhook retry checker — always active (works on Railway and locally)
    _scheduler.add_job(
        _process_webhook_retries, "interval", minutes=5,
        id="webhook_retries", misfire_grace_time=60,
    )
    if SYNC_ENABLED:
        _scheduler.add_job(_scheduled_sync, CronTrigger(hour=10, minute=0),  id="auto_sync_1", misfire_grace_time=300)
        _scheduler.add_job(_scheduled_sync, CronTrigger(hour=13, minute=0),  id="auto_sync_2", misfire_grace_time=300)
        _scheduler.add_job(_scheduled_sync, CronTrigger(hour=16, minute=30), id="auto_sync_3", misfire_grace_time=300)
        logger.info("[scheduler] Auto-sync scheduled at 10 am, 1 pm, 4:30 pm (local time).")
    _scheduler.start()
    logger.info("[scheduler] Webhook retry checker active (every 5 min).")
    yield
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
    await close_db()


app = FastAPI(title="Triple Take", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _fmt_call_date(date_str: str | None) -> str:
    """Format a call_date string (ISO or legacy) as 'Thu, May 22 at 3:14 PM'."""
    if not date_str:
        return "—"
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%m/%d/%y %I:%M %p",
        "%B %d, %Y %I:%M %p",  # after stripping " at "
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
        transcription_result = await transcribe_audio(audio_path)
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
    logger.info(f"[{client['slug']}] Starting full scrape (max {max_leads})...")
    try:
        leads = await scrape_all_leads(client, max_leads=max_leads, skip_message_ids=done_message_ids)
    except RuntimeError as e:
        logger.error(f"Scrape failed: {e}")
        return

    for lead in leads:
        existing = await get_lead(client_id, lead["id"])
        already_done = existing and existing.get("analysis_status") == "completed"
        # If Google gave us a real name and contact_name not already set, copy it over
        google_name = _best_caller_name(lead)
        if google_name and not (existing and existing.get("contact_name")):
            lead["contact_name"] = google_name
        await upsert_lead(client_id, lead)
        if already_done:
            logger.info(f"Lead {lead['id']} already analyzed, skipping")
            continue
        is_message = lead.get("lead_type") == "message"
        if lead.get("scrape_status") == "completed" and (lead.get("audio_path") or is_message):
            await _transcribe_and_analyze(client_id, lead["id"])

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
        await update_lead(client_id, lead_id, {"transcription_status": "in_progress"})
        result = await transcribe_audio(audio_path)
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
                "%m/%d/%y %I:%M %p", "%B %d, %Y %I:%M %p"):
        try:
            return _datetime.strptime(date_str.replace(" at ", " "), fmt).date()
        except ValueError:
            continue
    return None


async def _get_week_chart_data(client_id: int) -> tuple[str, str]:
    """Return (chart_leads_json, weeks_json) covering the past 6 Sun–Sat weeks (Eastern)."""
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

    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    all_weeks = []
    for w in range(6):   # w=0 oldest (5 weeks ago), w=5 current week
        ws = week_start - timedelta(weeks=5 - w)
        we = ws + timedelta(days=6)
        days = []
        for i in range(7):
            d = ws + timedelta(days=i)
            days.append({
                "date":     d.isoformat(),
                "day":      day_names[i],
                "md":       f"{d.month}/{d.day}",
                "is_today": d == today_et,
            })
        all_weeks.append({
            "week_start": ws.isoformat(),
            "week_end":   we.isoformat(),
            "days":       days,
        })

    return json.dumps(chart_leads), json.dumps(all_weeks)


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
    now_utc = _datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    if success:
        await update_webhook_delivery(delivery["id"], {
            "status": "success", "attempts": 1,
            "last_attempt_at": now_utc, "response_code": code,
        })
        logger.info(f"[webhook] Lead {lead_id} → DELIVERED (HTTP {code}).")
    else:
        next_at = (_datetime.utcnow() + timedelta(minutes=WEBHOOK_RETRY_MINUTES)).strftime("%Y-%m-%dT%H:%M:%S")
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
        now_utc      = _datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

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
            next_at = (_datetime.utcnow() + timedelta(minutes=WEBHOOK_RETRY_MINUTES)).strftime("%Y-%m-%dT%H:%M:%S")
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
async def admin_login(request: Request, password: str = Form(...)):
    if ADMIN_PASSWORD and bcrypt.checkpw(password.encode(), _admin_hash):
        response = RedirectResponse("/", status_code=302)
        response.set_cookie("admin_session", _sign("1"), httponly=True, samesite="lax", max_age=86400 * 30)
        return response
    return templates.TemplateResponse(request, "admin_login.html", {"error": "Incorrect password"}, status_code=401)


@app.post("/admin/logout")
async def admin_logout():
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
):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    pw_hash = bcrypt.hashpw(portal_password.encode(), bcrypt.gensalt()).decode() if portal_password else None
    client = await create_client(name, slug.lower().strip(), lead_list_url or None, pw_hash)
    if portal_password:
        await update_client(client["id"], {"portal_password_plain": portal_password})
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
):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    updates: dict = {
        "name":          name,
        "slug":          slug.lower().strip(),
        "lead_list_url": lead_list_url or None,
        "webhook_url":   webhook_url.strip() or None,
    }
    if webhook_secret.strip():
        updates["webhook_secret"] = webhook_secret.strip()
    if portal_password:
        updates["portal_password"] = bcrypt.hashpw(portal_password.encode(), bcrypt.gensalt()).decode()
        updates["portal_password_plain"] = portal_password
    await update_client(client_id, updates)
    return RedirectResponse("/admin/clients", status_code=302)


@app.post("/admin/clients/{client_id}/delete")
async def admin_delete_client(request: Request, client_id: int):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    await delete_client(client_id)
    return RedirectResponse("/admin/clients", status_code=302)


@app.post("/admin/clients/{client_id}/select")
async def admin_select_client(client_id: int):
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
async def scan_all_clients(request: Request, background_tasks: BackgroundTasks):
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

    page_size = 25
    offset = (page - 1) * page_size
    leads = _enrich_leads(await get_all_leads(
        client_id, limit=page_size, offset=offset,
        filter_answered=filter_answered, filter_charged=filter_charged,
    ))
    total = await get_leads_count(client_id,
                                  filter_answered=filter_answered,
                                  filter_charged=filter_charged)
    is_authenticated = await ensure_auth()
    chart_leads_json, chart_days_json = await _get_week_chart_data(client_id)
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
        "weekly_chart_leads_json": chart_leads_json,
        "weekly_chart_weeks_json": chart_days_json,
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

@app.post("/auth/login")
async def trigger_login(background_tasks: BackgroundTasks):
    background_tasks.add_task(open_login_browser)
    return JSONResponse({"message": "Browser opening — log in to Google, navigate to the account picker, then click Confirm."})


@app.post("/auth/confirm")
async def confirm_auth():
    result = await confirm_login()
    return JSONResponse(result)


@app.get("/auth/status")
async def auth_status():
    return {"authenticated": await ensure_auth()}


# ── Admin: scrape + pipeline ──────────────────────────────────────────────────

@app.post("/scrape")
async def trigger_scrape(request: Request, background_tasks: BackgroundTasks, max_leads: int = 50):
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
async def backfill_names(request: Request, background_tasks: BackgroundTasks):
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
async def process_lead(request: Request, lead_id: str, background_tasks: BackgroundTasks):
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
async def reanalyze_lead(request: Request, lead_id: str, background_tasks: BackgroundTasks):
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

    background_tasks.add_task(_reanalyze)
    return {"message": "Re-analyzing lead in background."}


@app.post("/leads/{lead_id}/contact-name")
async def update_contact_name(request: Request, lead_id: str):
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    if not client:
        raise HTTPException(status_code=400, detail="No client selected.")
    body = await request.json()
    name = (body.get("contact_name") or "").strip() or None
    await update_lead(client["id"], lead_id, {"contact_name": name})
    return {"contact_name": name}


@app.delete("/leads/{lead_id}")
async def remove_lead(request: Request, lead_id: str):
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
async def serve_audio(request: Request, lead_id: str, download: bool = False):
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    if not client:
        raise HTTPException(status_code=400, detail="No client selected.")
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


# ── Read-only client portal ───────────────────────────────────────────────────

@app.get("/portal/{slug}", response_class=HTMLResponse)
async def portal_login_page(request: Request, slug: str):
    client = await get_client_by_slug(slug)
    if not client or not client.get("portal_password"):
        raise HTTPException(status_code=404, detail="Portal not found")
    # Already logged in?
    if _portal_slug(request) == slug:
        return RedirectResponse(f"/portal/{slug}/leads", status_code=302)
    return templates.TemplateResponse(request, "portal_login.html", {"client": client})


@app.post("/portal/{slug}/login")
async def portal_login(request: Request, slug: str, password: str = Form(...)):
    client = await get_client_by_slug(slug)
    if not client or not client.get("portal_password"):
        raise HTTPException(status_code=404, detail="Portal not found")
    if bcrypt.checkpw(password.encode(), client["portal_password"].encode()):
        response = RedirectResponse(f"/portal/{slug}/leads", status_code=302)
        response.set_cookie("portal_session", _sign(slug), httponly=True, samesite="lax", max_age=86400 * 30)
        return response
    return templates.TemplateResponse(request, "portal_login.html", {"client": client, "error": "Incorrect password"}, status_code=401)


@app.get("/portal/{slug}/leads", response_class=HTMLResponse)
async def portal_leads(request: Request, slug: str, page: int = 1):
    if _portal_slug(request) != slug:
        return RedirectResponse(f"/portal/{slug}", status_code=302)
    client = await get_client_by_slug(slug)
    if not client:
        raise HTTPException(status_code=404, detail="Portal not found")

    filter_answered = request.query_params.getlist("answered") or None
    filter_charged = request.query_params.getlist("charged") or None

    page_size = 25
    offset = (page - 1) * page_size
    leads = _enrich_leads(await get_all_leads(
        client["id"], limit=page_size, offset=offset,
        filter_answered=filter_answered, filter_charged=filter_charged,
    ))
    total = await get_leads_count(client["id"],
                                  filter_answered=filter_answered,
                                  filter_charged=filter_charged)

    chart_leads_json, chart_days_json = await _get_week_chart_data(client["id"])

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
        "weekly_chart_leads_json": chart_leads_json,
        "weekly_chart_weeks_json": chart_days_json,
    })


@app.get("/portal/{slug}/leads/{lead_id}", response_class=HTMLResponse)
async def portal_lead_detail(request: Request, slug: str, lead_id: str):
    if _portal_slug(request) != slug:
        return RedirectResponse(f"/portal/{slug}", status_code=302)
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
    if not await ensure_auth():
        raise HTTPException(status_code=401, detail="Not authenticated.")
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    url = client["lead_list_url"] if client and client.get("lead_list_url") else "https://ads.google.com/localservices/accountpicker"
    result = await run_diagnostics(url)
    return JSONResponse(result)


@app.get("/debug/screenshot")
async def debug_screenshot(lead_id: str = None):
    path = Path(f"debug_lead_{lead_id}.png") if lead_id else Path("debug_screenshot.png")
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"No screenshot at {path}")
    return FileResponse(path, media_type="image/png")


@app.get("/health")
async def health():
    return {"status": "ok"}
