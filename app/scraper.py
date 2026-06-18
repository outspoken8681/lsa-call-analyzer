"""
Google Local Services Ads scraper — multi-tenant edition.

Authentication: first run opens a visible browser so you can log in to Google.
One auth session covers all clients. Session saved to auth.json.

Audio: stored per client at audio/{client_slug}/{lead_id}.mp3
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from playwright.async_api import Page, async_playwright

from app.r2 import upload_audio as r2_upload

logger = logging.getLogger(__name__)

AUTH_STATE_PATH = os.getenv("AUTH_STATE_PATH", "auth.json")
AUDIO_DIR = Path(os.getenv("AUDIO_DIR", "audio"))
LSA_BASE_URL = "https://ads.google.com/localservices/accountpicker"

def _clean_charge_status(text: str | None) -> str | None:
    """Strip Material icon names (e.g. 'help_outline') that Google appends to status text."""
    if not text:
        return text
    return re.sub(r'\s+\w+_\w+', '', text).strip() or text


async def _safe_go_to_list(go_to_list, lead_id: str) -> None:
    """
    Return to the lead-list table between leads. Google occasionally navigates
    mid-evaluation ("Execution context was destroyed"), which previously crashed
    the entire scrape. Swallow such errors so one hiccup never aborts the run;
    the next lead's row-click will simply fail gracefully if we're off-list.
    """
    try:
        await go_to_list()
    except Exception as e:
        logger.warning(f"go_to_list failed after lead {lead_id} (continuing): {e}")


# Shared state for the login flow
_login_event: asyncio.Event | None = None
_login_page = None
_login_context = None
_login_browser = None


async def ensure_auth() -> bool:
    """Return True if auth.json exists and appears valid."""
    path = Path(AUTH_STATE_PATH)
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text())
        return "cookies" in data
    except Exception:
        return False


async def open_login_browser():
    """
    Opens a visible browser for the user to log in to Google.
    Waits until confirm_login() is called before saving the session.
    """
    global _login_event, _login_page, _login_context, _login_browser

    _login_event = asyncio.Event()

    p = await async_playwright().start()
    _login_browser = await p.chromium.launch(headless=False, slow_mo=100)
    _login_context = await _login_browser.new_context()
    _login_page = await _login_context.new_page()
    await _login_page.goto(LSA_BASE_URL)

    logger.info("Login browser open — waiting for user to confirm via dashboard.")
    await _login_event.wait()

    await _login_context.storage_state(path=AUTH_STATE_PATH)
    logger.info("Auth session saved.")

    await _login_browser.close()
    _login_browser = None
    _login_context = None
    _login_page = None
    _login_event = None


async def confirm_login() -> dict:
    """Called when the user clicks Confirm in the dashboard."""
    global _login_event
    if _login_event is None:
        return {
            "success": False,
            "message": "No login session in progress. Click Connect Google Account first.",
        }
    _login_event.set()
    await asyncio.sleep(1)
    authenticated = await ensure_auth()
    return {
        "success": authenticated,
        "message": "Google session saved! You can now add clients and sync leads." if authenticated else "Something went wrong — try again.",
    }


async def run_diagnostics(lead_list_url: str) -> dict:
    """Navigate to a lead list URL and save a screenshot."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(storage_state=AUTH_STATE_PATH)
        page = await context.new_page()
        try:
            await page.goto(lead_list_url, wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            logger.warning(f"goto timeout (continuing): {e}")
        await page.wait_for_timeout(5000)
        final_url = page.url
        title = await page.title()
        screenshot_path = "debug_screenshot.png"
        await page.screenshot(path=screenshot_path, full_page=True)
        body_text = await page.evaluate("() => document.body.innerText.slice(0, 1000)")
        await browser.close()
        return {
            "saved_url": lead_list_url,
            "final_url": final_url,
            "page_title": title,
            "screenshot_saved": screenshot_path,
            "page_text": body_text,
        }


async def _pick_account(page, lead_list_url: str) -> bool:
    """
    We're on the Google LSA account picker. Find and click the account that
    matches the bid/cid in lead_list_url, then wait for the lead list to load.
    """
    await page.screenshot(path="debug_accountpicker.png", full_page=True)
    params = parse_qs(urlparse(lead_list_url).query)
    target_bid = (params.get("bid") or [None])[0]
    target_cid = (params.get("cid") or [None])[0]
    logger.info(f"Account picker — looking for bid={target_bid} cid={target_cid}")

    # Find any link on the picker whose href contains our bid or cid
    account_href = await page.evaluate(f"""() => {{
        const links = Array.from(document.querySelectorAll('a'));
        const match = links.find(a =>
            ('{target_bid}' && a.href.includes('bid={target_bid}')) ||
            ('{target_cid}' && a.href.includes('cid={target_cid}'))
        );
        return match ? match.href : null;
    }}""")
    logger.info(f"Account picker link found: {account_href}")

    if account_href:
        try:
            await page.goto(account_href, wait_until="domcontentloaded", timeout=20000)
        except Exception:
            pass
        await page.wait_for_timeout(4000)
        table_rows = await page.evaluate(
            "() => document.querySelectorAll('table tbody tr').length"
        )
        if table_rows > 0:
            logger.info("Recovered: selected account from picker, lead list loaded.")
            return True

    logger.warning("Could not find matching account in picker.")
    return False


async def _ensure_on_lead_list(page, lead_list_url: str) -> bool:
    """
    Google sometimes redirects the lead list URL to a Business Verification page
    or the account picker (common for MCC-managed accounts). This function detects
    where we ended up and navigates to the actual lead list.

    Flow:
      • Already on lead list → done
      • On account picker → click the right account
      • On anything else (e.g. Business Verification) →
          open hamburger → click Leads → may land on inbox or account picker
    """
    # Already there?
    table_rows = await page.evaluate(
        "() => document.querySelectorAll('table tbody tr').length"
    )
    if table_rows > 0:
        return True

    # Landed directly on account picker?
    if "accountpicker" in page.url:
        logger.info("Landed on account picker directly — selecting account...")
        return await _pick_account(page, lead_list_url)

    logger.info(f"No lead table found (url={page.url[:80]}). Opening hamburger menu...")

    # Open the hamburger drawer
    opened = await page.evaluate("""() => {
        const btn = document.querySelector('[aria-label="Main menu"]');
        if (btn) { btn.click(); return true; }
        return false;
    }""")
    logger.info(f"Hamburger click result: {opened}")
    await page.wait_for_timeout(2000)

    # Use Playwright's real mouse click on the "Leads" nav item.
    # A synthetic JS click() doesn't fire all the same events as a real mouse click,
    # which causes Google's SPA router to behave differently.
    leads_clicked = False
    try:
        # Playwright text selector — finds the visible "Leads" element in the drawer
        leads_el = await page.query_selector("text='Leads'")
        if leads_el:
            await leads_el.click()
            leads_clicked = True
            logger.info("Clicked Leads via Playwright real mouse click")
    except Exception as e:
        logger.warning(f"Playwright Leads click failed: {e}")

    if leads_clicked:
        # Wait for navigation — could go to inbox (standalone) or account picker (MCC)
        try:
            await page.wait_for_url(
                lambda url: "inbox" in url or "accountpicker" in url,
                timeout=12000,
            )
        except Exception:
            pass
        await page.wait_for_timeout(3000)

        # Landed on account picker (MCC path)?
        if "accountpicker" in page.url:
            logger.info("Leads click went to account picker — selecting account...")
            return await _pick_account(page, lead_list_url)

        # Check for table (standalone account path)
        table_rows = await page.evaluate(
            "() => document.querySelectorAll('table tbody tr').length"
        )
        if table_rows > 0:
            logger.info("Recovered: hamburger → Leads → lead list.")
            return True

    await page.screenshot(path="debug_recovery_failed.png", full_page=True)
    logger.warning("Could not reach lead list. Saved debug_recovery_failed.png")
    return False


async def get_lead_list(client: dict) -> list[dict]:
    """
    Quick pass — reads the lead list table only, no detail page visits.
    Returns basic metadata for all phone leads so they can be saved to the
    DB immediately and appear in the UI while the full scrape runs.
    """
    if not await ensure_auth():
        raise RuntimeError("No auth state.")

    lead_list_url = client["lead_list_url"]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            storage_state=AUTH_STATE_PATH,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
        page = await context.new_page()
        try:
            await page.goto(lead_list_url, wait_until="domcontentloaded", timeout=20000)
        except Exception:
            pass
        await page.wait_for_timeout(4000)

        if "accounts.google.com" in page.url or "signin" in page.url.lower():
            await browser.close()
            Path(AUTH_STATE_PATH).unlink(missing_ok=True)
            raise RuntimeError("Session expired. Re-authenticate.")

        await _ensure_on_lead_list(page, lead_list_url)

        rows = await page.evaluate("""() => {
            const trs = document.querySelectorAll('table tbody tr');
            return Array.from(trs).map(tr =>
                Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim())
            );
        }""")
        await browser.close()

    leads = []
    for cells in rows:
        if not cells or len(cells) < 2:
            continue
        lead_id = cells[-1].strip()
        if not lead_id or not lead_id.isdigit():
            continue
        raw_type = (cells[4] if len(cells) > 4 else "").strip().lower()
        lead_type = "message" if raw_type == "message" else "phone"
        cell0 = (cells[0] or "").strip() if cells else ""
        # For message leads cells[0] is sometimes a name, sometimes a phone number.
        # Distinguish by presence of digits: digits → phone, letters only → name.
        if lead_type == "message" and cell0 and not any(c.isdigit() for c in cell0):
            caller_name, caller_phone = cell0, None
        else:
            caller_name, caller_phone = None, cell0 or None
        leads.append({
            "id": lead_id,
            "lead_type": lead_type,
            "caller_name": caller_name,
            "caller_phone": caller_phone,
            "job_type": cells[1] if len(cells) > 1 and cells[1] != "-" else None,
            "location": cells[3] if len(cells) > 3 and cells[3] != "-" else None,
            "call_date": _normalize_call_date(cells[6]) if len(cells) > 6 else None,
            "charge_status": _clean_charge_status(cells[5]) if len(cells) > 5 else None,
            "scrape_status": "pending",
            "transcription_status": "completed" if lead_type == "message" else "pending",
            "analysis_status": "pending",
        })

    logger.info(f"[{client['slug']}] Lead list read: {len(leads)} leads found ({sum(1 for l in leads if l['lead_type'] == 'message')} messages)")
    return leads


async def _go_to_reports(page, lead_list_url: str) -> bool:
    """
    Navigate to the LSA Reports page. The direct /reports URL redirect-loops on
    MCC-managed accounts, so we reach it the same way the UI does: load the lead
    list, then open the hamburger drawer and click 'Reports'.
    """
    try:
        await page.goto(lead_list_url, wait_until="domcontentloaded", timeout=25000)
    except Exception:
        pass
    await page.wait_for_timeout(5000)
    await _ensure_on_lead_list(page, lead_list_url)

    await page.evaluate("""() => {
        const b = document.querySelector('[aria-label="Main menu"]');
        if (b) b.click();
    }""")
    await page.wait_for_timeout(1500)
    try:
        el = await page.query_selector("text='Reports'")
        if el:
            await el.click()
    except Exception as e:
        logger.warning(f"Reports nav click failed: {e}")
        return False
    await page.wait_for_timeout(2000)
    return await _wait_for_text(page, r"ad impressions", timeout_ms=30000)


async def _wait_for_text(page, pattern: str, timeout_ms: int = 25000, poll_ms: int = 1000) -> bool:
    """
    Poll the page until its visible text matches `pattern` (regex, case-insensitive),
    up to timeout_ms. Returns True if it appeared. The LSA Reports page is a heavy
    SPA that renders its tiles a few seconds after navigation, so we wait for the
    content to actually exist instead of guessing a fixed sleep.
    """
    import re as _re
    waited = 0
    rx = _re.compile(pattern, _re.IGNORECASE)
    while waited < timeout_ms:
        try:
            txt = await page.evaluate("() => document.body.innerText")
        except Exception:
            txt = ""
        if rx.search(txt or ""):
            return True
        await page.wait_for_timeout(poll_ms)
        waited += poll_ms
    return False


def _parse_impressions(text: str) -> Optional[int]:
    """Pull an 'Ad impressions' integer out of a blob of report page text."""
    # Look for the number immediately around the 'Ad impressions' label.
    m = re.search(r'Ad impressions\s*([\d,]+)', text, re.IGNORECASE)
    if not m:
        m = re.search(r'([\d,]+)\s*Ad impressions', text, re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


async def _select_single_day_and_read(page, target_date, slug: str) -> Optional[int]:
    """
    On an already-loaded Reports page, set the date range to a single day and read
    'Ad impressions'. Reusable across days within one browser session.

    Mirrors the manual flow: open the date dropdown via its caret → Custom →
    navigate to the target month → click the day twice (sets Start=End) → Apply.
    """
    day_num = str(target_date.day)
    expected_val = target_date.strftime("%b ") + str(target_date.day) + target_date.strftime(", %Y")  # "Jun 16, 2026"
    target_header = target_date.strftime("%B %Y")

    # Open the date-range dropdown. The "Date range" input is disabled; the
    # clickable trigger is the caret element just past its right edge.
    box = await page.evaluate("""() => {
        const i = Array.from(document.querySelectorAll('input'))
            .find(x => x.getAttribute('aria-label') === 'Date range');
        if (!i) return null;
        const ir = i.getBoundingClientRect();
        const caret = Array.from(document.querySelectorAll('*')).find(e => {
            const r = e.getBoundingClientRect();
            return r.width > 0 && r.width < 48 && r.height > 0 && r.height < 48 &&
                   r.x >= ir.right - 5 && r.x < ir.right + 60 &&
                   Math.abs((r.y + r.height/2) - (ir.y + ir.height/2)) < 20;
        });
        const r = caret ? caret.getBoundingClientRect() : null;
        return {
            x: Math.round(r ? r.x + r.width/2 : ir.right + 24),
            y: Math.round((r ? r.y + r.height/2 : ir.y + ir.height/2)),
        };
    }""")
    if not box:
        logger.warning(f"[{slug}] Date-range control not found.")
        return None
    await page.mouse.click(box["x"], box["y"])
    await page.wait_for_timeout(1500)

    # Choose "Custom".
    try:
        await page.get_by_text("Custom", exact=True).first.click(timeout=5000)
    except Exception:
        await page.evaluate("""() => {
            const t = Array.from(document.querySelectorAll('li,[role=option],span,div,button'))
                .find(e => e.children.length === 0 && (e.textContent||'').trim() === 'Custom');
            if (t) t.click();
        }""")
    await page.wait_for_timeout(2000)

    # The calendar opens on the current month; navigate back to the target month.
    # Day cells carry an unambiguous data-day-of-month attribute (only real days,
    # no padding), so there is never a duplicate/adjacent-month day to confuse.
    navigated = False
    for _ in range(18):  # safety bound (covers ~17 months back)
        header = await page.evaluate(r"""() => {
            const h = Array.from(document.querySelectorAll('*')).find(e =>
                e.children.length === 0 &&
                /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$/.test((e.textContent||'').trim()));
            return h ? h.textContent.trim() : null;
        }""")
        if header == target_header:
            navigated = True
            break
        moved = await page.evaluate("""() => {
            const b = Array.from(document.querySelectorAll('button,[role=button]'))
                .find(e => (e.getAttribute('aria-label')||'').toLowerCase() === 'previous month');
            if (b) { b.click(); return true; }
            return false;
        }""")
        if not moved:
            break
        await page.wait_for_timeout(600)
    if not navigated:
        logger.warning(f"[{slug}] Could not navigate calendar to {target_header}.")
        return None

    # Click the target day TWICE (sets Start and End to the same date). Target by
    # data-day-of-month — exact, position-independent.
    day_xy = await page.evaluate("""(dayNum) => {
        const e = document.querySelector(`[role=gridcell][data-day-of-month="${dayNum}"]`);
        if (!e || e.offsetParent === null) return null;
        const r = e.getBoundingClientRect();
        return {x: Math.round(r.x + r.width/2), y: Math.round(r.y + r.height/2)};
    }""", day_num)
    if not day_xy:
        logger.warning(f"[{slug}] Calendar day {day_num} not found in {target_header}.")
        return None
    await page.mouse.click(day_xy["x"], day_xy["y"])
    await page.wait_for_timeout(300)
    await page.mouse.click(day_xy["x"], day_xy["y"])
    await page.wait_for_timeout(800)

    # Sanity-check the picker captured our single day before applying.
    se = await page.evaluate("""() => {
        const f = Array.from(document.querySelectorAll('input'))
            .filter(i => ['Start','End'].includes(i.getAttribute('aria-label')));
        return f.map(i => i.value.trim());
    }""")
    if not (se and all(v == expected_val for v in se)):
        logger.warning(f"[{slug}] Date pick mismatch: got {se}, expected {expected_val!r}.")

    # Apply.
    try:
        await page.get_by_text("APPLY", exact=True).first.click(timeout=5000)
    except Exception:
        await page.evaluate("""() => {
            const t = Array.from(document.querySelectorAll('button,span'))
                .find(e => (e.textContent||'').trim().toUpperCase() === 'APPLY');
            if (t) t.click();
        }""")

    # Tiles re-fetch after Apply — wait for them to settle.
    await _wait_for_text(page, r"ad impressions", timeout_ms=20000)
    await page.wait_for_timeout(2500)

    impressions = _parse_impressions(await page.evaluate("() => document.body.innerText"))
    if impressions is None:
        logger.warning(f"[{slug}] Could not read impressions for {target_date}.")
    else:
        logger.info(f"[{slug}] Impressions for {target_date}: {impressions}")
    return impressions


async def _open_reports_page(p, lead_list_url: str, slug: str):
    """Launch a browser and land on the Reports page. Returns (browser, page) or (browser, None)."""
    browser = await p.chromium.launch(
        headless=True, args=["--disable-blink-features=AutomationControlled"],
    )
    context = await browser.new_context(
        storage_state=AUTH_STATE_PATH,
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    )
    page = await context.new_page()
    reached = await _go_to_reports(page, lead_list_url)
    if not reached:
        logger.info(f"[{slug}] Reports load flaked, retrying once...")
        reached = await _go_to_reports(page, lead_list_url)
    if not reached:
        await page.screenshot(path=f"debug_reports_{slug}.png", full_page=True)
        logger.warning(f"[{slug}] Could not reach Reports page. Saved debug_reports_{slug}.png")
        return browser, None
    return browser, page


async def scrape_impressions_for_date(client: dict, target_date) -> Optional[int]:
    """
    Read the 'Ad impressions' count for a single day from the LSA Reports page.
    """
    if not await ensure_auth():
        raise RuntimeError("No auth state.")
    from datetime import date as _date
    if isinstance(target_date, str):
        target_date = _date.fromisoformat(target_date)

    async with async_playwright() as p:
        browser, page = await _open_reports_page(p, client["lead_list_url"], client["slug"])
        try:
            if page is None:
                return None
            return await _select_single_day_and_read(page, target_date, client["slug"])
        finally:
            await browser.close()


async def scrape_impressions_range(client: dict, dates: list) -> dict:
    """
    Read 'Ad impressions' for many days in ONE browser session (efficient backfill).
    `dates` is a list of date objects or YYYY-MM-DD strings.
    Returns {date_iso: impressions} for the days that were read successfully.
    """
    if not await ensure_auth():
        raise RuntimeError("No auth state.")
    from datetime import date as _date
    norm = [(_date.fromisoformat(d) if isinstance(d, str) else d) for d in dates]
    results: dict = {}

    async with async_playwright() as p:
        browser, page = await _open_reports_page(p, client["lead_list_url"], client["slug"])
        try:
            if page is None:
                return results
            for d in norm:
                try:
                    imp = await _select_single_day_and_read(page, d, client["slug"])
                    if imp is not None:
                        results[d.isoformat()] = imp
                except Exception as e:
                    logger.warning(f"[{client['slug']}] Impressions read failed for {d}: {e}")
            return results
        finally:
            await browser.close()


async def scrape_all_leads(client: dict, max_leads: int = 50, skip_message_ids: set = None,
                           skip_phone_ids: set = None, on_lead=None) -> list[dict]:
    """
    Scrape all phone leads for a client.
    client dict must have: slug, lead_list_url

    skip_message_ids / skip_phone_ids: lead IDs already fully processed in the DB.
    These are skipped without re-visiting Google, so a re-sync never clobbers the
    status of an already-completed lead (audio lives in R2, not on ephemeral disk).

    on_lead: optional async callback invoked with each lead dict the moment it is
    scraped. Lets the caller persist + process leads incrementally, so a crash
    partway through never discards work already done.
    """
    if not await ensure_auth():
        raise RuntimeError("No auth state. Use Connect Google Account first.")
    skip_phone_ids = skip_phone_ids or set()

    async def _emit(lead: dict) -> None:
        """Hand a freshly scraped lead to the caller; never let it crash the scrape."""
        if on_lead is None:
            return
        try:
            await on_lead(dict(lead))
        except Exception as cb_err:
            logger.exception(f"on_lead callback failed for {lead.get('id')}: {cb_err}")

    client_audio_dir = AUDIO_DIR / client["slug"]
    client_audio_dir.mkdir(parents=True, exist_ok=True)
    lead_list_url = client["lead_list_url"]
    results: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            storage_state=AUTH_STATE_PATH,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
        page = await context.new_page()

        async def go_to_list():
            # First try clicking "Leads" directly via the hamburger — this avoids
            # the redirect loop that happens when navigating to the URL directly
            # on accounts (like MCC-managed ones) that redirect to verification.
            try:
                hamburger = await page.query_selector('[aria-label="Main menu"]')
                if hamburger:
                    await hamburger.click()
                    await page.wait_for_timeout(1500)
                leads_el = await page.query_selector("text='Leads'")
                if leads_el:
                    await leads_el.click()
                    await page.wait_for_timeout(3000)
                    table_rows = await page.evaluate(
                        "() => document.querySelectorAll('table tbody tr').length"
                    )
                    if table_rows > 0:
                        return  # success — skip the full goto below
            except Exception:
                pass

            # Fallback: navigate directly (works for accounts without redirect issues)
            try:
                await page.goto(lead_list_url, wait_until="domcontentloaded", timeout=20000)
            except Exception:
                pass
            await page.wait_for_timeout(4000)
            await _ensure_on_lead_list(page, lead_list_url)

        await go_to_list()

        if "accounts.google.com" in page.url or "signin" in page.url.lower():
            await browser.close()
            Path(AUTH_STATE_PATH).unlink(missing_ok=True)
            raise RuntimeError("Session expired. Re-authenticate via Connect Google Account.")

        rows = await page.evaluate("""() => {
            const trs = document.querySelectorAll('table tbody tr');
            return Array.from(trs).map(tr =>
                Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim())
            );
        }""")

        all_leads = []
        for cells in rows:
            if not cells or len(cells) < 2:
                continue
            lead_id = cells[-1].strip()
            if not lead_id or not lead_id.isdigit():
                continue
            raw_type = (cells[4] if len(cells) > 4 else "").strip().lower()
            lead_type = "message" if raw_type == "message" else "phone"
            cell0 = (cells[0] or "").strip() if cells else ""
            # For message leads cells[0] is sometimes a name, sometimes a phone number.
            # Distinguish by presence of digits: digits → phone, letters only → name.
            if lead_type == "message" and cell0 and not any(c.isdigit() for c in cell0):
                caller_name, caller_phone = cell0, None
            else:
                caller_name, caller_phone = None, cell0 or None
            all_leads.append({
                "id": lead_id,
                "lead_type": lead_type,
                "caller_name": caller_name,
                "caller_phone": caller_phone,
                "job_type": cells[1] if len(cells) > 1 and cells[1] != "-" else None,
                "location": cells[3] if len(cells) > 3 and cells[3] != "-" else None,
                "call_date": _normalize_call_date(cells[6]) if len(cells) > 6 else None,
                "charge_status": _clean_charge_status(cells[5]) if len(cells) > 5 else None,
                "scrape_status": "pending",
                "transcription_status": "completed" if lead_type == "message" else "pending",
                "analysis_status": "pending",
            })

        msg_count = sum(1 for l in all_leads if l["lead_type"] == "message")
        logger.info(f"[{client['slug']}] Found {len(all_leads)} leads ({msg_count} messages, {len(all_leads)-msg_count} phone)")

        for lead in all_leads[:max_leads]:
            lead_id = lead["id"]
            try:
                # ── Skip message leads already fully analyzed ─────────────────
                if lead.get("lead_type") == "message" and skip_message_ids and lead_id in skip_message_ids:
                    logger.info(f"Lead {lead_id}: message already analyzed — skipping")
                    lead.pop("transcription_status", None)
                    lead.pop("analysis_status", None)
                    lead["scrape_status"] = "completed"
                    results.append(lead)
                    await _emit(lead)
                    continue

                # ── Skip phone leads already fully processed in the DB ────────
                # Audio lives in R2, not on Railway's ephemeral disk, so we must
                # NOT re-scrape these — doing so resets their statuses to pending
                # and can fail ("SHOW RECORDING not found"), wiping a good lead.
                if lead.get("lead_type") != "message" and lead_id in skip_phone_ids:
                    logger.info(f"Lead {lead_id}: phone lead already completed in DB — skipping")
                    lead.pop("transcription_status", None)
                    lead.pop("analysis_status", None)
                    lead["scrape_status"] = "completed"
                    results.append(lead)
                    await _emit(lead)
                    continue

                # ── Skip phone leads whose audio is already on disk ───────────
                if lead.get("lead_type") != "message":
                    existing_audio = client_audio_dir / f"{lead_id}.mp3"
                    if existing_audio.exists():
                        logger.info(f"Lead {lead_id}: audio already on disk — skipping download")
                        r2_key = f"{client['slug']}/{lead_id}.mp3"
                        uploaded = await r2_upload(str(existing_audio), r2_key)
                        # Drop default pending statuses so we don't overwrite completed ones in DB
                        lead.pop("transcription_status", None)
                        lead.pop("analysis_status", None)
                        lead.update({
                            "audio_path": str(existing_audio),
                            "audio_url": r2_key if uploaded else lead.get("audio_url"),
                            "scrape_status": "completed",
                        })
                        results.append(lead)
                        await _emit(lead)
                        continue

                clicked = await page.evaluate("""(leadId) => {
                    const rows = document.querySelectorAll('table tbody tr');
                    for (const row of rows) {
                        const cells = row.querySelectorAll('td');
                        const last = cells[cells.length - 1];
                        if (last && last.textContent.trim() === leadId) {
                            row.click();
                            return true;
                        }
                    }
                    return false;
                }""", lead_id)

                if not clicked:
                    logger.warning(f"Lead {lead_id}: row not found in table")
                    lead.update({"scrape_status": "failed", "error_message": "Row not found"})
                    results.append(lead)
                    await _emit(lead)
                    continue

                await page.wait_for_timeout(3000)
                lead["lead_url"] = page.url
                logger.info(f"Lead {lead_id}: on detail page {page.url[:60]}")

                # ── Message lead — extract conversation, skip audio ────────────
                if lead.get("lead_type") == "message":
                    message_text = await _extract_message_content(page)
                    metadata = await _extract_lead_detail_metadata(page)
                    if message_text:
                        lead.update({
                            **metadata,
                            "transcript": message_text,
                            "is_answered": 1,
                            "scrape_status": "completed",
                            "transcription_status": "completed",
                            "scraped_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                        })
                        logger.info(f"Lead {lead_id}: message content extracted ({len(message_text)} chars)")
                    else:
                        lead.update({"scrape_status": "failed", "error_message": "Could not extract message content"})
                        logger.warning(f"Lead {lead_id}: no message content found")
                    results.append(lead)
                    await _emit(lead)
                    await _safe_go_to_list(go_to_list, lead_id)
                    continue

                page_text = await page.evaluate("() => document.body.innerText.toUpperCase()")
                if "MISSED CALL" in page_text or "NO ANSWER" in page_text:
                    logger.info(f"Lead {lead_id}: missed call — skipping audio")
                    lead.update({
                        "is_answered": 0,
                        "scrape_status": "completed",
                        "transcription_status": "completed",
                        "analysis_status": "completed",
                        "call_summary": "Missed call — no recording available.",
                        "scraped_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                    })
                    results.append(lead)
                    await _emit(lead)
                    await _safe_go_to_list(go_to_list, lead_id)
                    continue

                clicked_text = await page.evaluate("""() => {
                    const all = Array.from(document.querySelectorAll('span, div, button'));
                    const target = all.find(el =>
                        el.children.length === 0 &&
                        el.textContent.trim().toUpperCase().includes('SHOW RECORDING')
                    );
                    if (target) { target.click(); return target.textContent.trim(); }
                    return null;
                }""")

                if not clicked_text:
                    await page.screenshot(path=f"debug_lead_{lead_id}.png")
                    logger.warning(f"Lead {lead_id}: SHOW RECORDING not found")
                    lead.update({"scrape_status": "failed", "error_message": "SHOW RECORDING not found"})
                    results.append(lead)
                    await _emit(lead)
                    await _safe_go_to_list(go_to_list, lead_id)
                    continue

                try:
                    await page.wait_for_selector("audio source[src]", timeout=8000)
                except Exception:
                    pass

                audio_url = await page.evaluate("""() => {
                    const el = document.querySelector('audio source[src]');
                    return el ? el.getAttribute('src') : null;
                }""")

                if not audio_url:
                    html = await page.content()
                    m = re.search(r'(https://ads\.google\.com/localservicesads/attachment/[^\s"\'<>&]+)', html)
                    audio_url = m.group(1) if m else None

                if not audio_url:
                    logger.warning(f"Lead {lead_id}: no audio URL found")
                    lead.update({"scrape_status": "failed", "error_message": "No audio URL found"})
                    results.append(lead)
                    await _emit(lead)
                    await _safe_go_to_list(go_to_list, lead_id)
                    continue

                audio_path = client_audio_dir / f"{lead_id}.mp3"
                response = await page.request.get(audio_url)
                if response.ok:
                    audio_path.write_bytes(await response.body())
                    logger.info(f"Lead {lead_id}: audio saved ({audio_path.stat().st_size} bytes)")
                    r2_key = f"{client['slug']}/{lead_id}.mp3"
                    uploaded = await r2_upload(str(audio_path), r2_key)
                    metadata = await _extract_lead_detail_metadata(page)
                    lead.update({
                        **metadata,
                        "audio_url": r2_key if uploaded else audio_url,
                        "audio_path": str(audio_path),
                        "is_answered": 1,
                        "scrape_status": "completed",
                        "scraped_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                    })
                else:
                    lead.update({"scrape_status": "failed", "error_message": f"Download failed: HTTP {response.status}"})

                results.append(lead)
                await _emit(lead)

            except Exception as e:
                logger.exception(f"Lead {lead_id}: unexpected error: {e}")
                lead.update({"scrape_status": "failed", "error_message": str(e)})
                results.append(lead)
                await _emit(lead)

            await _safe_go_to_list(go_to_list, lead_id)

        await browser.close()

    return results


async def scrape_lead_audio(client: dict, lead_id: str, lead_url: str) -> dict:
    """Scrape audio for a single lead — only clicks that one row."""
    if not await ensure_auth():
        return {"scrape_status": "failed", "error_message": "Not authenticated."}

    client_audio_dir = AUDIO_DIR / client["slug"]
    client_audio_dir.mkdir(parents=True, exist_ok=True)
    lead_list_url = client["lead_list_url"]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            storage_state=AUTH_STATE_PATH,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
        page = await context.new_page()

        try:
            await page.goto(lead_list_url, wait_until="domcontentloaded", timeout=20000)
        except Exception:
            pass
        await page.wait_for_timeout(4000)

        if "accounts.google.com" in page.url or "signin" in page.url.lower():
            await browser.close()
            Path(AUTH_STATE_PATH).unlink(missing_ok=True)
            return {"scrape_status": "failed", "error_message": "Session expired. Re-authenticate."}

        await _ensure_on_lead_list(page, lead_list_url)

        clicked = await page.evaluate("""(leadId) => {
            const rows = document.querySelectorAll('table tbody tr');
            for (const row of rows) {
                const cells = row.querySelectorAll('td');
                const last = cells[cells.length - 1];
                if (last && last.textContent.trim() === leadId) {
                    row.click();
                    return true;
                }
            }
            return false;
        }""", lead_id)

        if not clicked:
            await browser.close()
            return {"scrape_status": "failed", "error_message": "Lead not found in current list."}

        await page.wait_for_timeout(3000)
        result = {"lead_url": page.url}

        page_text = await page.evaluate("() => document.body.innerText.toUpperCase()")
        if "MISSED CALL" in page_text or "NO ANSWER" in page_text:
            await browser.close()
            return {
                **result,
                "is_answered": 0,
                "scrape_status": "completed",
                "transcription_status": "completed",
                "analysis_status": "completed",
                "call_summary": "Missed call — no recording available.",
                "scraped_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            }

        clicked_text = await page.evaluate("""() => {
            const all = Array.from(document.querySelectorAll('span, div, button'));
            const target = all.find(el =>
                el.children.length === 0 &&
                el.textContent.trim().toUpperCase().includes('SHOW RECORDING')
            );
            if (target) { target.click(); return target.textContent.trim(); }
            return null;
        }""")

        if not clicked_text:
            await page.screenshot(path=f"debug_lead_{lead_id}.png")
            await browser.close()
            return {"scrape_status": "failed", "error_message": "SHOW RECORDING not found"}

        try:
            await page.wait_for_selector("audio source[src]", timeout=8000)
        except Exception:
            pass

        audio_url = await page.evaluate("""() => {
            const el = document.querySelector('audio source[src]');
            return el ? el.getAttribute('src') : null;
        }""")

        if not audio_url:
            html = await page.content()
            m = re.search(r'(https://ads\.google\.com/localservicesads/attachment/[^\s"\'<>&]+)', html)
            audio_url = m.group(1) if m else None

        if not audio_url:
            await browser.close()
            return {"scrape_status": "failed", "error_message": "No audio URL found"}

        audio_path = client_audio_dir / f"{lead_id}.mp3"
        response = await page.request.get(audio_url)
        if response.ok:
            audio_path.write_bytes(await response.body())
            r2_key = f"{client['slug']}/{lead_id}.mp3"
            uploaded = await r2_upload(str(audio_path), r2_key)
            metadata = await _extract_lead_detail_metadata(page)
            await browser.close()
            return {
                **result,
                **metadata,
                "audio_url": r2_key if uploaded else audio_url,
                "audio_path": str(audio_path),
                "is_answered": 1,
                "scrape_status": "completed",
                "scraped_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            }
        else:
            await browser.close()
            return {"scrape_status": "failed", "error_message": f"Download failed: HTTP {response.status}"}


async def _extract_message_content(page: Page) -> Optional[str]:
    """
    Extract the full message conversation from an LSA message lead detail page.
    Returns a formatted string with all messages, or None if extraction fails.
    """
    try:
        content = await page.evaluate("""() => {
            // Google LSA message threads are often in divs with "message" in the class.
            // Try several selectors, fall back to full page text.
            const selectors = [
                '[class*="message"]', '[class*="Message"]',
                '[class*="conversation"]', '[class*="thread"]',
                '[class*="chat"]',
            ];
            for (const sel of selectors) {
                const els = Array.from(document.querySelectorAll(sel));
                const texts = els.map(el => el.innerText.trim()).filter(t => t.length > 5);
                if (texts.length >= 2) {
                    return texts.join('\\n---\\n');
                }
            }
            // Fallback: full page text (the AI will extract what's relevant)
            return document.body.innerText;
        }""")
        return content.strip() if content else None
    except Exception as e:
        logger.debug(f"Message content extraction error: {e}")
    return None


def _normalize_call_date(date_str: str) -> str:
    """
    Normalize call date strings to ISO format for consistent sorting.
    Handles:
      - "5/22/26 12:45 PM"        (from list table)
      - "May 21, 2026 at 3:14 PM" (from detail page)
    Returns "2026-05-22T12:45:00" style string, or the original if unparseable.
    """
    if not date_str:
        return date_str
    candidates = [
        (date_str, "%m/%d/%y %I:%M %p"),
        (date_str.replace(" at ", " "), "%B %d, %Y %I:%M %p"),
    ]
    for s, fmt in candidates:
        try:
            return datetime.strptime(s, fmt).isoformat()
        except ValueError:
            continue
    return date_str  # leave as-is if nothing matched


async def _extract_lead_detail_metadata(page: Page) -> dict:
    meta = {}
    try:
        body = await page.evaluate("() => document.body.innerText")
        phone_match = re.search(r'\(?\d{3}\)?[\s\-]\d{3}[\s\-]\d{4}', body)
        if phone_match:
            meta["caller_phone"] = phone_match.group(0)
        date_match = re.search(r'Received on (.+? at \d+:\d+ [AP]M)', body)
        if date_match:
            meta["call_date"] = _normalize_call_date(date_match.group(1))
        duration_match = re.search(r'\b(\d{1,2}:\d{2})\b', body)
        if duration_match:
            meta["call_duration_seconds"] = _parse_duration(duration_match.group(1))
    except Exception as e:
        logger.debug(f"Metadata extraction error: {e}")
    return meta


def _parse_duration(text: str) -> Optional[int]:
    text = text.strip()
    match = re.match(r"(\d+):(\d+)(?::(\d+))?", text)
    if match:
        parts = [int(x) for x in match.groups() if x is not None]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
    match = re.match(r"(\d+)\s*s", text)
    if match:
        return int(match.group(1))
    return None
