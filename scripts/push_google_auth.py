#!/usr/bin/env python3
"""
push_google_auth.py — refresh the app's Google Local Services Ads session.

Google blocks automated/headless login, so the actual sign-in must happen in a
real browser window on your computer. This script:

  1. Opens a visible Chromium window at the Google LSA account picker.
  2. Waits for you to sign in and reach the lead inbox / account picker.
  3. Captures the browser session and uploads it to the deployed app, which
     stores it durably (survives redeploys) — no env vars, no redeploy.

Usage
-----
    cd lsa-call-analyzer
    source .venv/bin/activate
    python scripts/push_google_auth.py

Configuration (env vars, or a .env in the project root):
    APP_URL            Base URL of the deployed app.
                       Default: https://lsa.tripletakemarketing.com
    AUTH_UPLOAD_TOKEN  Bearer token the app accepts for /auth/upload.
                       Defaults to ADMIN_PASSWORD if that's set locally.
    ADMIN_PASSWORD     Used as the token when AUTH_UPLOAD_TOKEN is unset.

Run with --local to also write auth.json locally (handy for local dev).
"""

import asyncio
import os
import sys
import tempfile
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except Exception:
    pass

import httpx
from playwright.async_api import async_playwright

APP_URL = os.getenv("APP_URL", "https://lsa.tripletakemarketing.com").rstrip("/")
UPLOAD_TOKEN = os.getenv("AUTH_UPLOAD_TOKEN", "") or os.getenv("ADMIN_PASSWORD", "")
LSA_URL = "https://ads.google.com/localservices/accountpicker"
WRITE_LOCAL = "--local" in sys.argv


async def main() -> int:
    if not UPLOAD_TOKEN:
        print("ERROR: No upload token. Set AUTH_UPLOAD_TOKEN (or ADMIN_PASSWORD) "
              "in your environment or project .env.")
        return 1

    print(f"Target app: {APP_URL}")
    print("Opening Chromium — sign in to Google and navigate to your LSA leads/account picker.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=100)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(LSA_URL)

        # Block until the operator confirms login is complete.
        print("\n>>> When you have logged in and can see your leads/account picker,")
        await asyncio.get_event_loop().run_in_executor(
            None, input, ">>> press ENTER here to capture and upload the session... ")

        tmp = Path(tempfile.gettempdir()) / "lsa_auth_state.json"
        await context.storage_state(path=str(tmp))
        await browser.close()

    json_text = tmp.read_text()
    cookie_count = json_text.count('"name"')
    print(f"Captured session ({cookie_count} cookies).")

    if WRITE_LOCAL:
        Path("auth.json").write_text(json_text)
        print("Wrote local auth.json (--local).")

    print(f"Uploading to {APP_URL}/auth/upload ...")
    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            resp = await http.post(
                f"{APP_URL}/auth/upload",
                content=json_text.encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {UPLOAD_TOKEN}",
                    "Content-Type": "application/json",
                },
            )
    except Exception as e:
        print(f"ERROR: upload request failed: {e}")
        tmp.unlink(missing_ok=True)
        return 1

    tmp.unlink(missing_ok=True)

    if resp.status_code == 200:
        body = resp.json()
        if body.get("authenticated"):
            print(f"✅ Success — app is now authenticated ({body.get('cookies')} cookies).")
            return 0
        print(f"⚠️  Uploaded, but the app still reports not authenticated: {body}")
        return 1
    print(f"ERROR: upload rejected (HTTP {resp.status_code}): {resp.text[:300]}")
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
