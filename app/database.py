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
"""


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
                       filter_charged: list[str] | None) -> tuple[str, list]:
    """Build a WHERE clause + params list for lead queries with optional filters."""
    conditions = ["client_id = $1"]
    params: list = [client_id]

    def _p(val):
        params.append(val)
        return f"${len(params)}"

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
                        filter_charged: list[str] | None = None) -> list[dict]:
    where, params = _build_lead_where(client_id, filter_answered, filter_charged)
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
                          filter_charged: list[str] | None = None) -> int:
    where, params = _build_lead_where(client_id, filter_answered, filter_charged)
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
