import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime as _datetime
from pathlib import Path

import bcrypt
from dotenv import load_dotenv

load_dotenv()

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from app.analyzer import analyze_transcript
from app.database import (
    close_db,
    create_client,
    delete_client,
    delete_lead,
    get_all_clients,
    get_all_leads,
    get_client,
    get_client_by_slug,
    get_lead,
    get_leads_count,
    init_db,
    update_client,
    update_lead,
    upsert_lead,
)
from app.scraper import ensure_auth, open_login_browser, confirm_login, scrape_lead_audio, scrape_all_leads, run_diagnostics, get_lead_list
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
_signer = URLSafeTimedSerializer(SECRET_KEY)

# Pre-hash admin password at startup
_admin_hash = bcrypt.hashpw(ADMIN_PASSWORD.encode(), bcrypt.gensalt()) if ADMIN_PASSWORD else b""


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


app = FastAPI(title="LSA Call Analyzer", lifespan=lifespan)
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
    }


# ── Pipeline helpers ──────────────────────────────────────────────────────────

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
        await update_lead(client_id, lead_id, analysis_result)


async def _scrape_and_process_all(client: dict, max_leads: int = 50):
    """Scrape all leads for a client then transcribe + analyze."""
    client_id = client["id"]

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
    logger.info(f"[{client['slug']}] Starting full scrape (max {max_leads})...")
    try:
        leads = await scrape_all_leads(client, max_leads=max_leads)
    except RuntimeError as e:
        logger.error(f"Scrape failed: {e}")
        return

    for lead in leads:
        await upsert_lead(client_id, lead)
        existing = await get_lead(client_id, lead["id"])
        if existing and existing.get("analysis_status") == "completed":
            logger.info(f"Lead {lead['id']} already analyzed, skipping")
            continue
        is_message = lead.get("lead_type") == "message"
        if lead.get("scrape_status") == "completed" and (lead.get("audio_path") or is_message):
            await _transcribe_and_analyze(client_id, lead["id"])

    logger.info(f"[{client['slug']}] Full scrape complete.")


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
        await update_lead(client_id, lead_id, result)


def _enrich_leads(leads: list[dict]) -> list[dict]:
    for lead in leads:
        if lead.get("analysis_json"):
            try:
                lead["analysis_data"] = json.loads(lead["analysis_json"])
            except Exception:
                lead["analysis_data"] = {}
    return leads


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
):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    updates: dict = {"name": name, "slug": slug.lower().strip(), "lead_list_url": lead_list_url or None}
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
    return RedirectResponse("/admin/clients", status_code=302)


@app.get("/leads", response_class=HTMLResponse)
async def dashboard(request: Request, page: int = 1):
    if not _is_admin(request):
        return RedirectResponse("/admin/login", status_code=302)

    ctx = await _admin_context(request)
    current_client = ctx["current_client"]

    if not current_client:
        return RedirectResponse("/admin/clients", status_code=302)

    client_id = current_client["id"]
    page_size = 25
    offset = (page - 1) * page_size
    leads = _enrich_leads(await get_all_leads(client_id, limit=page_size, offset=offset))
    total = await get_leads_count(client_id)
    is_authenticated = await ensure_auth()

    return templates.TemplateResponse(request, "index.html", {
        **ctx,
        "leads": leads,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
        "is_authenticated": is_authenticated,
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

    return templates.TemplateResponse(request, "lead.html", {**ctx, "lead": lead})


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
        await update_lead(client["id"], lead_id, result)

    background_tasks.add_task(_reanalyze)
    return {"message": "Re-analyzing lead in background."}


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
async def serve_audio(request: Request, lead_id: str):
    ctx = await _admin_context(request)
    client = ctx["current_client"]
    if not client:
        raise HTTPException(status_code=400, detail="No client selected.")
    lead = await get_lead(client["id"], lead_id)
    if not lead or not lead.get("audio_path"):
        raise HTTPException(status_code=404, detail="Audio not found")
    path = Path(lead["audio_path"])
    if not path.exists():
        raise HTTPException(status_code=404, detail="Audio file not found on disk")
    return FileResponse(path, media_type="audio/mpeg")


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

    page_size = 25
    offset = (page - 1) * page_size
    leads = _enrich_leads(await get_all_leads(client["id"], limit=page_size, offset=offset))
    total = await get_leads_count(client["id"])

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
async def portal_audio(request: Request, slug: str, lead_id: str):
    if _portal_slug(request) != slug:
        raise HTTPException(status_code=403, detail="Not authenticated")
    client = await get_client_by_slug(slug)
    if not client:
        raise HTTPException(status_code=404)
    lead = await get_lead(client["id"], lead_id)
    if not lead or not lead.get("audio_path"):
        raise HTTPException(status_code=404, detail="Audio not found")
    path = Path(lead["audio_path"])
    if not path.exists():
        raise HTTPException(status_code=404, detail="Audio file not found on disk")
    return FileResponse(path, media_type="audio/mpeg")


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
