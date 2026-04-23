"""
Advanced Telegram Bot — Main Entry Point
Features:
  • /start      → 20 random videos (protect_content) + 5-min auto-delete
  • /reset      → Admin: force-refresh video cache
  • /setcaption → Admin: update final message caption
  • /setcontact → Admin: update contact button URL
  • /broadcast  → Admin: send to all users, auto-delete after 24h
  • /index      → Admin: scan & index private channel video IDs
"""

import logging
from telegram.ext import Application, CommandHandler
from config import BOT_TOKEN
from database import init_db
from handlers.user_handlers import start_command
from handlers.admin_handlers import (
    reset_command,
    setcaption_command,
    setcontact_command,
    broadcast_command,
    _delete_broadcast_job,
)
from utils.channel_indexer import index_command, init_channel_videos_table

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def _on_startup(app: Application):
    """Called once after the bot starts — schedule background jobs."""
    app.job_queue.run_repeating(
        _delete_broadcast_job,
        interval=300,   # check every 5 minutes
        first=10,
        name="broadcast_cleaner",
    )
    logger.info("✅ Broadcast cleaner job scheduled.")


def main():
    # ── Database setup ───────────────────────────────────────────────────────
    init_db()
    init_channel_videos_table()
    logger.info("✅ Database tables ready.")

    # ── Build Application ────────────────────────────────────────────────────
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(_on_startup)
        .build()
    )

    # ── Register Handlers ────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",      start_command))
    app.add_handler(CommandHandler("reset",      reset_command))
    app.add_handler(CommandHandler("setcaption", setcaption_command))
    app.add_handler(CommandHandler("setcontact", setcontact_command))
    app.add_handler(CommandHandler("broadcast",  broadcast_command))
    app.add_handler(CommandHandler("index",      index_command))

    logger.info("🤖 Bot polling started…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
