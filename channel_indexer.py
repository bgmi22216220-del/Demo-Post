"""
utils/channel_indexer.py

Admin utility: /index command
Scans the private channel and stores all video message IDs
in the `channel_videos` table so the bot can reliably fetch them.

This is the RECOMMENDED approach over range-scanning, as it is
faster and more accurate.

Add to bot.py:
    from utils.channel_indexer import index_command
    app.add_handler(CommandHandler("index", index_command))
"""

import logging
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import TelegramError
import psycopg2
from config import ADMIN_ID, CHANNEL_ID
from database import get_conn

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Ensure channel_videos table exists
# ─────────────────────────────────────────────────────────────────────────────

def init_channel_videos_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS channel_videos (
        message_id  BIGINT PRIMARY KEY,
        media_type  TEXT,
        indexed_at  TIMESTAMPTZ DEFAULT NOW()
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def save_channel_video(message_id: int, media_type: str = "video"):
    sql = """
    INSERT INTO channel_videos (message_id, media_type)
    VALUES (%s, %s)
    ON CONFLICT (message_id) DO NOTHING;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (message_id, media_type))
        conn.commit()


def get_all_channel_video_ids() -> list[int]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT message_id FROM channel_videos ORDER BY RANDOM() LIMIT 100;"
            )
            return [row[0] for row in cur.fetchall()]


def clear_channel_videos():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM channel_videos;")
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# /index command — admin scans channel
# ─────────────────────────────────────────────────────────────────────────────

async def index_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /index <start_id> <end_id>
    Example: /index 1 300

    Scans channel message IDs from start_id to end_id,
    indexes all video/document messages found.
    """
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("🚫 Unauthorized.")
        return

    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "❌ Usage: `/index <start_id> <end_id>`\n"
            "Example: `/index 1 500`",
            parse_mode="Markdown",
        )
        return

    try:
        start_id = int(args[0])
        end_id   = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ ID numbers galat hain.")
        return

    init_channel_videos_table()
    clear_channel_videos()

    status_msg = await update.message.reply_text(
        f"🔍 Channel scan shuru... `{start_id}` se `{end_id}` tak",
        parse_mode="Markdown",
    )

    found = 0
    for msg_id in range(start_id, end_id + 1):
        try:
            fwd = await context.bot.forward_message(
                chat_id=ADMIN_ID,
                from_chat_id=CHANNEL_ID,
                message_id=msg_id,
                disable_notification=True,
            )
            media_type = None
            if fwd.video:
                media_type = "video"
            elif fwd.document:
                media_type = "document"
            elif fwd.photo:
                media_type = "photo"

            if media_type:
                save_channel_video(msg_id, media_type)
                found += 1

            # Delete the temp forward
            try:
                await context.bot.delete_message(
                    chat_id=ADMIN_ID,
                    message_id=fwd.message_id,
                )
            except TelegramError:
                pass

        except TelegramError:
            # Message doesn't exist or not accessible — skip
            continue

    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=status_msg.message_id,
        text=(
            f"✅ *Index complete!*\n\n"
            f"📹 Videos/Docs found: *{found}*\n"
            f"📊 Range scanned: `{start_id}` – `{end_id}`\n\n"
            f"Ab `/start` karने par yahi indexed videos use hongi."
        ),
        parse_mode="Markdown",
    )
    logger.info(f"Channel indexed: {found} videos found in range {start_id}-{end_id}")
