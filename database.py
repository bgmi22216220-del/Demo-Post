"""
database.py — Neon PostgreSQL via pg8000 (pure Python, no system libs needed)
Tables: users, settings, fetched_content, broadcast_jobs, channel_videos
"""

import json
import logging
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional

import pg8000.dbapi
from config import DATABASE_URL, CYCLE_DAYS

logger = logging.getLogger(__name__)


def get_conn():
    """Return a new pg8000 connection to Neon PostgreSQL."""
    r = urllib.parse.urlparse(DATABASE_URL)
    return pg8000.dbapi.connect(
        host=r.hostname,
        port=r.port or 5432,
        database=r.path.lstrip("/"),
        user=r.username,
        password=r.password,
        ssl_context=True,
    )


def _to_dict(cur, row) -> dict:
    cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


def init_db():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     BIGINT PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                joined_at   TIMESTAMPTZ DEFAULT NOW(),
                last_fetch  TIMESTAMPTZ
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        """)
        cur.execute("""
            INSERT INTO settings (key, value)
            VALUES ('caption', 'Aur videos dekhne ke liye admin se contact karein.')
            ON CONFLICT (key) DO NOTHING;
        """)
        cur.execute("""
            INSERT INTO settings (key, value)
            VALUES ('contact', 'https://t.me/youradmin')
            ON CONFLICT (key) DO NOTHING;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fetched_content (
                id             SERIAL PRIMARY KEY,
                user_id        BIGINT NOT NULL,
                message_ids    TEXT   NOT NULL DEFAULT '[]',
                warning_msg_id BIGINT,
                fetched_at     TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_jobs (
                id         SERIAL PRIMARY KEY,
                user_id    BIGINT  NOT NULL,
                message_id BIGINT  NOT NULL,
                delete_at  TIMESTAMPTZ NOT NULL,
                deleted    BOOLEAN DEFAULT FALSE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS channel_videos (
                message_id BIGINT PRIMARY KEY,
                media_type TEXT,
                indexed_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
        logger.info("Database schema ready.")
    finally:
        conn.close()


# ── Users ─────────────────────────────────────────────────────────────────────

def upsert_user(user_id: int, username, first_name):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (user_id, username, first_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
                SET username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name;
        """, (user_id, username, first_name))
        conn.commit()
    finally:
        conn.close()


def get_all_user_ids():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users;")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def update_last_fetch(user_id: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET last_fetch = NOW() WHERE user_id = %s;", (user_id,))
        conn.commit()
    finally:
        conn.close()


# ── Settings ──────────────────────────────────────────────────────────────────

def get_setting(key: str) -> str:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key = %s;", (key,))
        row = cur.fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


def set_setting(key: str, value: str):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
        """, (key, value))
        conn.commit()
    finally:
        conn.close()


# ── Fetched Content ───────────────────────────────────────────────────────────

def get_user_content(user_id: int):
    cutoff = datetime.utcnow() - timedelta(days=CYCLE_DAYS)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, message_ids, warning_msg_id, fetched_at
            FROM   fetched_content
            WHERE  user_id = %s AND fetched_at > %s
            ORDER  BY fetched_at DESC LIMIT 1;
        """, (user_id, cutoff))
        row = cur.fetchone()
        return _to_dict(cur, row) if row else None
    finally:
        conn.close()


def save_user_content(user_id: int, message_ids: list, warning_msg_id: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM fetched_content WHERE user_id = %s;", (user_id,))
        cur.execute("""
            INSERT INTO fetched_content (user_id, message_ids, warning_msg_id)
            VALUES (%s, %s, %s);
        """, (user_id, json.dumps(message_ids), warning_msg_id))
        conn.commit()
    finally:
        conn.close()


def reset_all_content():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM fetched_content;")
        conn.commit()
    finally:
        conn.close()


# ── Broadcast Jobs ────────────────────────────────────────────────────────────

def save_broadcast_job(user_id: int, message_id: int):
    delete_at = datetime.utcnow() + timedelta(hours=24)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO broadcast_jobs (user_id, message_id, delete_at)
            VALUES (%s, %s, %s);
        """, (user_id, message_id, delete_at))
        conn.commit()
    finally:
        conn.close()


def get_pending_broadcast_deletes():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_id, message_id FROM broadcast_jobs
            WHERE deleted = FALSE AND delete_at <= NOW();
        """)
        rows = cur.fetchall()
        return [_to_dict(cur, row) for row in rows]
    finally:
        conn.close()


def mark_broadcast_deleted(job_id: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE broadcast_jobs SET deleted = TRUE WHERE id = %s;", (job_id,))
        conn.commit()
    finally:
        conn.close()


# ── Channel Videos ────────────────────────────────────────────────────────────

def save_channel_video(message_id: int, media_type: str = "video"):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO channel_videos (message_id, media_type)
            VALUES (%s, %s) ON CONFLICT (message_id) DO NOTHING;
        """, (message_id, media_type))
        conn.commit()
    finally:
        conn.close()


def get_all_channel_video_ids():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT message_id FROM channel_videos ORDER BY RANDOM() LIMIT 100;")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def clear_channel_videos():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM channel_videos;")
        conn.commit()
    finally:
        conn.close()
