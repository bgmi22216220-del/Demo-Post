"""
database.py — Neon PostgreSQL via psycopg2
Tables: users, settings, fetched_content, broadcast_jobs
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
from config import DATABASE_URL, CYCLE_DAYS

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Connection helper
# ─────────────────────────────────────────────────────────────────────────────

def get_conn():
    """Return a new psycopg2 connection to Neon PostgreSQL."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")


# ─────────────────────────────────────────────────────────────────────────────
# Schema bootstrap
# ─────────────────────────────────────────────────────────────────────────────

def init_db():
    """Create all tables if they don't exist."""
    ddl = """
    -- Users table
    CREATE TABLE IF NOT EXISTS users (
        user_id     BIGINT PRIMARY KEY,
        username    TEXT,
        first_name  TEXT,
        joined_at   TIMESTAMPTZ DEFAULT NOW(),
        last_fetch  TIMESTAMPTZ
    );

    -- Settings table  (key-value store for caption / contact)
    CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );

    -- Insert defaults only if rows don't already exist
    INSERT INTO settings (key, value)
    VALUES
        ('caption',  'Aur videos dekhne ke liye admin se contact karein.'),
        ('contact',  'https://t.me/youradmin')
    ON CONFLICT (key) DO NOTHING;

    -- Fetched content per user  (stores list of forwarded message-ids)
    CREATE TABLE IF NOT EXISTS fetched_content (
        id          SERIAL PRIMARY KEY,
        user_id     BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
        message_ids JSONB  NOT NULL DEFAULT '[]',
        warning_msg_id BIGINT,
        fetched_at  TIMESTAMPTZ DEFAULT NOW()
    );

    -- Broadcast jobs  (track which broadcast msg to delete after 24h)
    CREATE TABLE IF NOT EXISTS broadcast_jobs (
        id          SERIAL PRIMARY KEY,
        user_id     BIGINT  NOT NULL,
        message_id  BIGINT  NOT NULL,
        delete_at   TIMESTAMPTZ NOT NULL,
        deleted     BOOLEAN DEFAULT FALSE
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    logger.info("Database schema ready.")


# ─────────────────────────────────────────────────────────────────────────────
# Users
# ─────────────────────────────────────────────────────────────────────────────

def upsert_user(user_id: int, username: Optional[str], first_name: Optional[str]):
    sql = """
    INSERT INTO users (user_id, username, first_name)
    VALUES (%s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE
        SET username   = EXCLUDED.username,
            first_name = EXCLUDED.first_name;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, username, first_name))
        conn.commit()


def get_all_user_ids() -> list[int]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users;")
            return [row[0] for row in cur.fetchall()]


def update_last_fetch(user_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET last_fetch = NOW() WHERE user_id = %s;",
                (user_id,),
            )
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────────────────────────────────────

def get_setting(key: str) -> str:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s;", (key,))
            row = cur.fetchone()
            return row[0] if row else ""


def set_setting(key: str, value: str):
    sql = """
    INSERT INTO settings (key, value) VALUES (%s, %s)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (key, value))
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Fetched Content  (7-day cycle)
# ─────────────────────────────────────────────────────────────────────────────

def get_user_content(user_id: int) -> Optional[dict]:
    """
    Return the latest fetched_content row for this user if it is
    still within the 7-day cycle window, else None.
    """
    cutoff = datetime.utcnow() - timedelta(days=CYCLE_DAYS)
    sql = """
    SELECT id, message_ids, warning_msg_id, fetched_at
    FROM   fetched_content
    WHERE  user_id = %s AND fetched_at > %s
    ORDER  BY fetched_at DESC
    LIMIT  1;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, (user_id, cutoff))
            row = cur.fetchone()
            if row:
                return dict(row)
    return None


def save_user_content(user_id: int, message_ids: list[int], warning_msg_id: int):
    """Delete old rows for this user and insert fresh ones."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM fetched_content WHERE user_id = %s;", (user_id,))
            cur.execute(
                """
                INSERT INTO fetched_content (user_id, message_ids, warning_msg_id)
                VALUES (%s, %s, %s);
                """,
                (user_id, json.dumps(message_ids), warning_msg_id),
            )
        conn.commit()


def update_warning_msg_id(user_id: int, warning_msg_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE fetched_content
                SET    warning_msg_id = %s
                WHERE  user_id = %s
                AND    id = (
                    SELECT id FROM fetched_content
                    WHERE  user_id = %s
                    ORDER  BY fetched_at DESC LIMIT 1
                );
                """,
                (warning_msg_id, user_id, user_id),
            )
        conn.commit()


def reset_user_content(user_id: int):
    """Force-delete cached content so next /start fetches fresh videos."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM fetched_content WHERE user_id = %s;", (user_id,))
        conn.commit()


def reset_all_content():
    """Admin /reset — wipe ALL cached content for every user."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM fetched_content;")
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Broadcast Jobs
# ─────────────────────────────────────────────────────────────────────────────

def save_broadcast_job(user_id: int, message_id: int):
    delete_at = datetime.utcnow() + timedelta(hours=24)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO broadcast_jobs (user_id, message_id, delete_at)
                VALUES (%s, %s, %s);
                """,
                (user_id, message_id, delete_at),
            )
        conn.commit()


def get_pending_broadcast_deletes() -> list[dict]:
    """Return all broadcast jobs due for deletion."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT id, user_id, message_id
                FROM   broadcast_jobs
                WHERE  deleted = FALSE AND delete_at <= NOW();
                """
            )
            return [dict(row) for row in cur.fetchall()]


def mark_broadcast_deleted(job_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE broadcast_jobs SET deleted = TRUE WHERE id = %s;",
                (job_id,),
            )
        conn.commit()
