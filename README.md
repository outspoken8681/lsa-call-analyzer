# LSA Call Analyzer

Pulls call recordings from Google Local Services Ads, transcribes them with OpenAI Whisper, and analyzes each call with Claude — all in a self-hosted web dashboard.

## What it does

- Logs into your Google LSA account via Playwright (you sign in once in a real browser window; the session is saved)
- Navigates each lead detail page, intercepts the embedded MP3 audio, and downloads it
- Transcribes the audio with Whisper
- Sends the transcript to Claude for structured analysis:
  - Was the call answered?
  - Lead qualification score (1–5)
  - Caller sentiment (positive / neutral / negative)
  - Call summary
  - Follow-up actions needed
  - Service type requested

## Quick start

### 1. Install dependencies

```bash
cd lsa-call-analyzer
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and add your API keys
```

Required keys in `.env`:
```
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
```

### 3. Run the server

```bash
uvicorn app.main:app --reload --port 8000
```

Open http://localhost:8000

### 4. Connect your Google account

Click **Connect Google Account** in the dashboard. A real browser window opens — sign in to your Google account, navigate to the LSA lead list until it's fully loaded, then press **Enter** in the terminal. Your session is saved to `auth.json` for future runs.

### 5. Sync leads

Click **Sync Leads**. The app scrapes your lead list, downloads audio for each call, transcribes it, and analyzes it in the background. Refresh the dashboard after a minute to see results.

---

## Notes on the Google LSA scraper

The scraper uses two techniques to find the audio URL on each lead page:

1. **Network interception** — intercepts all requests made while the page loads and captures any that look like audio (`.mp3`, `audio/`, `recording`, etc.)
2. **DOM inspection** — searches `<audio>` elements and the raw page HTML for audio URLs

If the scraper can't find audio on a lead, it's likely because:
- Google changed their page structure (update selectors in `app/scraper.py`)
- The call was not recorded (very short calls sometimes aren't)
- Your session expired — reconnect via the dashboard

## Project structure

```
app/
  main.py          FastAPI routes and background task orchestration
  scraper.py       Playwright-based Google LSA scraper
  transcriber.py   OpenAI Whisper transcription
  analyzer.py      Anthropic Claude analysis
  database.py      SQLite storage
  templates/       Jinja2 + Bootstrap 5 dashboard
audio/             Downloaded call recordings (gitignored)
auth.json          Saved Google session (gitignored)
lsa.db             SQLite database (gitignored)
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Dashboard |
| GET | `/leads/{id}` | Lead detail |
| POST | `/auth/login` | Open browser for Google login |
| GET | `/auth/status` | Check if authenticated |
| POST | `/scrape` | Scrape + process all new leads |
| POST | `/leads/{id}/process` | Run full pipeline for one lead |
| POST | `/leads/{id}/reanalyze` | Re-run Claude analysis only |
