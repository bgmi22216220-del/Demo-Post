"""
Configuration — loads all settings from environment variables.
Used by Railway.app / .env locally.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Core ──────────────────────────────────────────────────────────────────────
BOT_TOKEN: str = os.environ["BOT_TOKEN"]
ADMIN_ID: int = int(os.environ["ADMIN_ID"])
DATABASE_URL: str = os.environ["DATABASE_URL"]

# ── Channel ───────────────────────────────────────────────────────────────────
# Your private channel ID (e.g. -1001234567890)
CHANNEL_ID: int = int(os.environ["CHANNEL_ID"])

# ── Timings ───────────────────────────────────────────────────────────────────
VIDEO_DELETE_SECONDS: int = 5 * 60          # 5 minutes
BROADCAST_DELETE_SECONDS: int = 24 * 60 * 60  # 24 hours
CYCLE_DAYS: int = 7                          # 7-day video cycle

# ── Limits ────────────────────────────────────────────────────────────────────
VIDEOS_PER_SESSION: int = 20
