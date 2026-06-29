"""
app/database.py — asyncpg-backed, multi-tenant PostgreSQL.

Pool lifecycle:
  Call init_db() once at startup (creates tables + warms pool).
  Call close_db() once at shutdown.

All lead operations require client_id. The leads PK is (id TEXT, client_id INTEGER).
"""

import asyncpg
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def init_db() -> None:
    global _pool
    database_url = os.environ["DATABASE_URL"]
    _pool = await asyncpg.create_pool(
        database_url,
        min_size=2,
        max_size=10,
        command_timeout=60,
    )
    async with _pool.acquire() as conn:
        await conn.execute(_SCHEMA_SQL)
    logger.info("Database pool ready.")


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def _get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialised — call init_db() first.")
    return _pool


# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS clients (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL UNIQUE,
    lead_list_url   TEXT,
    portal_password TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS leads (
    id                      TEXT    NOT NULL,
    client_id               INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    caller_name             TEXT,
    caller_phone            TEXT,
    location                TEXT,
    call_date               TEXT,
    call_duration_seconds   INTEGER,
    charge_status           TEXT,
    job_type                TEXT,
    lead_url                TEXT,
    audio_url               TEXT,
    audio_path              TEXT,
    scrape_status           TEXT NOT NULL DEFAULT 'pending',
    transcription_status    TEXT NOT NULL DEFAULT 'pending',
    analysis_status         TEXT NOT NULL DEFAULT 'pending',
    transcript              TEXT,
    is_answered             INTEGER,
    qualification_score     INTEGER,
    qualification_reason    TEXT,
    call_summary            TEXT,
    analysis_json           TEXT,
    error_message           TEXT,
    scraped_at              TEXT,
    transcribed_at          TEXT,
    analyzed_at             TEXT,
    created_at              TEXT NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    PRIMARY KEY (id, client_id)
);

CREATE INDEX IF NOT EXISTS idx_leads_client_id ON leads (client_id);
CREATE INDEX IF NOT EXISTS idx_leads_call_date ON leads (client_id, call_date DESC NULLS LAST);

ALTER TABLE leads ADD COLUMN IF NOT EXISTS lead_type TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS contact_name TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS portal_password_plain TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS last_synced_at TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS last_sync_new_leads INTEGER;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS webhook_url TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS webhook_secret TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS business_type TEXT;
-- LSA advertiser ID as shown in Google's account picker (NOT the cid in the URL).
ALTER TABLE clients ADD COLUMN IF NOT EXISTS google_account_id TEXT;
-- Rolling 30-day Reports snapshot (spend + charged leads), refreshed on sync.
ALTER TABLE clients ADD COLUMN IF NOT EXISTS r30_spend REAL;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS r30_leads INTEGER;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS r30_updated_at TEXT;

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id              SERIAL PRIMARY KEY,
    lead_id         TEXT    NOT NULL,
    client_id       INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    status          TEXT    NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    next_attempt_at TEXT,
    response_code   INTEGER,
    error_message   TEXT,
    created_at      TEXT NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);
CREATE INDEX IF NOT EXISTS idx_wh_deliveries_status ON webhook_deliveries (status);
CREATE INDEX IF NOT EXISTS idx_wh_deliveries_lead   ON webhook_deliveries (client_id, lead_id);

CREATE TABLE IF NOT EXISTS app_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_metrics (
    client_id   INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    date        TEXT    NOT NULL,         -- YYYY-MM-DD (Eastern business day)
    impressions INTEGER,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (client_id, date)
);
"""

# Key under which the Google/Playwright auth state JSON is stored.
AUTH_STATE_KEY = "google_auth_state"


# ── Client CRUD ───────────────────────────────────────────────────────────────

async def get_all_clients() -> list[dict]:
    async with _get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT * FROM clients ORDER BY name")
        return [dict(r) for r in rows]


async def get_client(client_id: int) -> Optional[dict]:
    async with _get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM clients WHERE id = $1", client_id
        )
        return dict(row) if row else None


async def get_client_by_slug(slug: str) -> Optional[dict]:
    async with _get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM clients WHERE slug = $1", slug
        )
        return dict(row) if row else None


async def create_client(
    name: str,
    slug: str,
    lead_list_url: Optional[str],
    portal_password_hash: Optional[str],
) -> dict:
    async with _get_pool().acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO clients (name, slug, lead_list_url, portal_password)
            VALUES ($1, $2, $3, $4)
            RETURNING *
            """,
            name, slug, lead_list_url, portal_password_hash,
        )
        return dict(row)


async def update_client(client_id: int, updates: dict) -> Optional[dict]:
    if not updates:
        return await get_client(client_id)
    set_parts = []
    values = [client_id]
    for i, (k, v) in enumerate(updates.items(), start=2):
        set_parts.append(f"{k} = ${i}")
        values.append(v)
    sql = f"UPDATE clients SET {', '.join(set_parts)} WHERE id = $1 RETURNING *"
    async with _get_pool().acquire() as conn:
        row = await conn.fetchrow(sql, *values)
        return dict(row) if row else None


async def delete_client(client_id: int) -> None:
    async with _get_pool().acquire() as conn:
        await conn.execute("DELETE FROM clients WHERE id = $1", client_id)


# ── Lead CRUD ─────────────────────────────────────────────────────────────────

async def upsert_lead(client_id: int, lead: dict) -> None:
    data = {**lead, "client_id": client_id}
    cols = list(data.keys())
    vals = list(data.values())
    placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
    col_names = ", ".join(cols)
    update_parts = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in ("id", "client_id")
    )
    sql = f"""
        INSERT INTO leads ({col_names}) VALUES ({placeholders})
        ON CONFLICT (id, client_id) DO UPDATE SET {update_parts}
    """
    async with _get_pool().acquire() as conn:
        await conn.execute(sql, *vals)


async def update_lead(client_id: int, lead_id: str, updates: dict) -> None:
    if not updates:
        return
    set_parts = []
    values = [client_id, lead_id]
    for i, (k, v) in enumerate(updates.items(), start=3):
        set_parts.append(f"{k} = ${i}")
        values.append(v)
    sql = f"UPDATE leads SET {', '.join(set_parts)} WHERE client_id = $1 AND id = $2"
    async with _get_pool().acquire() as conn:
        await conn.execute(sql, *values)


async def get_lead(client_id: int, lead_id: str) -> Optional[dict]:
    async with _get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM leads WHERE client_id = $1 AND id = $2",
            client_id, lead_id,
        )
        return dict(row) if row else None


def _build_lead_where(client_id: int,
                       filter_answered: list[str] | None,
                       filter_charged: list[str] | None,
                       search: str | None = None) -> tuple[str, list]:
    """Build a WHERE clause + params list for lead queries with optional filters."""
    conditions = ["client_id = $1"]
    params: list = [client_id]

    def _p(val):
        params.append(val)
        return f"${len(params)}"

    if search and search.strip():
        like = f"%{search.strip()}%"
        cols = ("caller_name", "contact_name", "caller_phone", "location",
                "job_type", "transcript", "call_summary", "qualification_reason", "id")
        conditions.append("(" + " OR ".join(f"{c} ILIKE {_p(like)}" for c in cols) + ")")

    if filter_answered:
        parts = []
        for v in filter_answered:
            if v == "yes":
                parts.append("(is_answered = 1 AND (lead_type IS NULL OR lead_type != 'message'))")
            elif v == "missed":
                parts.append("(is_answered = 0 AND (lead_type IS NULL OR lead_type != 'message'))")
            elif v == "message":
                parts.append("lead_type = 'message'")
        if parts:
            conditions.append(f"({' OR '.join(parts)})")

    if filter_charged:
        parts = []
        for v in filter_charged:
            if v == "charged":
                parts.append(f"(charge_status ILIKE {_p('%charged%')} AND charge_status NOT ILIKE {_p('%not%')})")
            elif v == "in-review":
                parts.append(f"charge_status ILIKE {_p('%review%')}")
            elif v == "not-charged":
                parts.append(f"charge_status ILIKE {_p('%not%')}")
            elif v == "credited":
                parts.append(f"(charge_status ILIKE {_p('%credit%')} OR charge_status ILIKE {_p('%refund%')})")
        if parts:
            conditions.append(f"({' OR '.join(parts)})")

    return " AND ".join(conditions), params


async def get_all_leads(client_id: int, limit: int = 100, offset: int = 0,
                        filter_answered: list[str] | None = None,
                        filter_charged: list[str] | None = None,
                        search: str | None = None) -> list[dict]:
    where, params = _build_lead_where(client_id, filter_answered, filter_charged, search)
    lim_p = f"${len(params)+1}"
    off_p = f"${len(params)+2}"
    async with _get_pool().acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT * FROM leads
            WHERE {where}
            ORDER BY call_date DESC NULLS LAST, created_at DESC
            LIMIT {lim_p} OFFSET {off_p}
            """,
            *params, limit, offset,
        )
        return [dict(r) for r in rows]


async def get_leads_count(client_id: int,
                          filter_answered: list[str] | None = None,
                          filter_charged: list[str] | None = None,
                          search: str | None = None) -> int:
    where, params = _build_lead_where(client_id, filter_answered, filter_charged, search)
    async with _get_pool().acquire() as conn:
        val = await conn.fetchval(
            f"SELECT COUNT(*) FROM leads WHERE {where}", *params
        )
        return val or 0


async def delete_lead(client_id: int, lead_id: str) -> None:
    async with _get_pool().acquire() as conn:
        await conn.execute(
            "DELETE FROM leads WHERE client_id = $1 AND id = $2",
            client_id, lead_id,
        )


# ── Webhook delivery CRUD ─────────────────────────────────────────────────────

async def create_webhook_delivery(client_id: int, lead_id: str) -> dict:
    async with _get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO webhook_deliveries (client_id, lead_id) VALUES ($1, $2) RETURNING *",
            client_id, lead_id,
        )
        return dict(row)


async def get_webhook_delivery(client_id: int, lead_id: str) -> Optional[dict]:
    """Most recent delivery record for this (client, lead) pair."""
    async with _get_pool().acquire() as conn:
        row = await conn.fetchrow(
            """SELECT * FROM webhook_deliveries
               WHERE client_id = $1 AND lead_id = $2
               ORDER BY created_at DESC LIMIT 1""",
            client_id, lead_id,
        )
        return dict(row) if row else None


async def update_webhook_delivery(delivery_id: int, updates: dict) -> None:
    if not updates:
        return
    set_parts = []
    values    = [delivery_id]
    for i, (k, v) in enumerate(updates.items(), start=2):
        set_parts.append(f"{k} = ${i}")
        values.append(v)
    async with _get_pool().acquire() as conn:
        await conn.execute(
            f"UPDATE webhook_deliveries SET {', '.join(set_parts)} WHERE id = $1",
            *values,
        )


async def get_pending_webhook_retries() -> list[dict]:
    """Retrying deliveries whose next_attempt_at is now or past (UTC ISO comparison).

    next_attempt_at is a fixed-width zero-padded ISO string (YYYY-MM-DDTHH:MM:SS),
    so lexicographic comparison is equivalent to chronological comparison.
    """
    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    async with _get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM webhook_deliveries
               WHERE status = 'retrying'
                 AND next_attempt_at IS NOT NULL
                 AND next_attempt_at <= $1
               ORDER BY next_attempt_at ASC LIMIT 50""",
            now_utc,
        )
    return [dict(r) for r in rows]


async def get_failed_webhook_count(client_id: int) -> int:
    async with _get_pool().acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM webhook_deliveries WHERE client_id = $1 AND status = 'failed'",
            client_id,
        ) or 0


async def get_webhook_deliveries_for_lead(client_id: int, lead_id: str) -> list[dict]:
    async with _get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM webhook_deliveries
               WHERE client_id = $1 AND lead_id = $2
               ORDER BY created_at DESC""",
            client_id, lead_id,
        )
        return [dict(r) for r in rows]


# ── App settings (durable key/value) ──────────────────────────────────────────

async def set_setting(key: str, value: str | None) -> None:
    async with _get_pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO app_settings (key, value, updated_at)
               VALUES ($1, $2, NOW())
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()""",
            key, value,
        )


async def get_setting(key: str) -> Optional[str]:
    async with _get_pool().acquire() as conn:
        return await conn.fetchval("SELECT value FROM app_settings WHERE key = $1", key)


async def get_setting_updated_at(key: str) -> Optional[str]:
    async with _get_pool().acquire() as conn:
        val = await conn.fetchval("SELECT updated_at FROM app_settings WHERE key = $1", key)
        return val.isoformat() if val else None


async def save_auth_state(json_text: str) -> None:
    """Persist the Google/Playwright auth-state JSON so it survives redeploys."""
    await set_setting(AUTH_STATE_KEY, json_text)


async def load_auth_state() -> Optional[str]:
    return await get_setting(AUTH_STATE_KEY)


# ── Daily metrics (ad impressions per day) ────────────────────────────────────

async def upsert_daily_metric(client_id: int, date: str, impressions: Optional[int]) -> None:
    async with _get_pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO daily_metrics (client_id, date, impressions, updated_at)
               VALUES ($1, $2, $3, NOW())
               ON CONFLICT (client_id, date)
               DO UPDATE SET impressions = EXCLUDED.impressions, updated_at = NOW()""",
            client_id, date, impressions,
        )


async def get_daily_metrics(client_id: int, start: str, end: str) -> dict[str, int]:
    """Return {date: impressions} for client between start..end (inclusive, YYYY-MM-DD)."""
    async with _get_pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT date, impressions FROM daily_metrics
               WHERE client_id = $1 AND date >= $2 AND date <= $3
                 AND impressions IS NOT NULL""",
            client_id, start, end,
        )
        return {r["date"]: r["impressions"] for r in rows}
