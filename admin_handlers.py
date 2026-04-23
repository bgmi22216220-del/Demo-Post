"""
handlers/admin_handlers.py
Admin-only commands:
  /reset        — force refresh all users' video cache
  /setcaption   — update global caption
  /setcontact   — update contact button URL
  /broadcast    — send image+text to all users (auto-deleted after 24h)
"""

import logging
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import TelegramError

from config import ADMIN_ID, BROADCAST_DELETE_SECONDS
from database import (
    reset_all_content,
    set_setting,
    get_all_user_ids,
    save_broadcast_job,
    get_pending_broadcast_deletes,
    mark_broadcast_deleted,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Admin guard decorator
# ─────────────────────────────────────────────────────────────────────────────

def admin_only(func):
    """Decorator — silently ignore non-admin users."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_ID:
            await update.message.reply_text(
                "🚫 Aap is command ko use karne ke liye authorized nahi hain."
            )
            return
        return await func(update, context)
    return wrapper


# ─────────────────────────────────────────────────────────────────────────────
# Scheduled job: delete broadcast messages after 24h
# ─────────────────────────────────────────────────────────────────────────────

async def _delete_broadcast_job(context: ContextTypes.DEFAULT_TYPE):
    """Runs every 5 min — cleans up due broadcast messages."""
    jobs = get_pending_broadcast_deletes()
    for job in jobs:
        try:
            await context.bot.delete_message(
                chat_id=job["user_id"],
                message_id=job["message_id"],
            )
        except TelegramError as e:
            logger.warning(f"Broadcast delete failed for job {job['id']}: {e}")
        finally:
            mark_broadcast_deleted(job["id"])

    if jobs:
        logger.info(f"Cleaned up {len(jobs)} broadcast messages.")


# ─────────────────────────────────────────────────────────────────────────────
# /reset
# ─────────────────────────────────────────────────────────────────────────────

@admin_only
async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /reset — Wipe all fetched_content rows so every user gets
    fresh videos on their next /start.
    """
    reset_all_content()
    await update.message.reply_text(
        "✅ *Database reset ho gaya!*\n\n"
        "Ab har user ke next `/start` par channel ke latest videos fetch honge.",
        parse_mode="Markdown",
    )
    logger.info(f"Admin {update.effective_user.id} triggered /reset")


# ─────────────────────────────────────────────────────────────────────────────
# /setcaption [text]
# ─────────────────────────────────────────────────────────────────────────────

@admin_only
async def setcaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setcaption <your caption text>
    Sets the global caption shown in the final (permanent) message.
    """
    if not context.args:
        await update.message.reply_text(
            "❌ Usage: `/setcaption Aapka caption text yahan likhein`",
            parse_mode="Markdown",
        )
        return

    caption = " ".join(context.args)
    set_setting("caption", caption)
    await update.message.reply_text(
        f"✅ *Caption update ho gaya!*\n\n📌 New caption:\n_{caption}_",
        parse_mode="Markdown",
    )
    logger.info(f"Admin set caption: {caption}")


# ─────────────────────────────────────────────────────────────────────────────
# /setcontact [username or URL]
# ─────────────────────────────────────────────────────────────────────────────

@admin_only
async def setcontact_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setcontact @username  OR  /setcontact https://t.me/username
    Sets the URL for the 'Contact Admin' button.
    """
    if not context.args:
        await update.message.reply_text(
            "❌ Usage: `/setcontact @yourusername`",
            parse_mode="Markdown",
        )
        return

    raw = context.args[0].strip()

    # Convert @username → https://t.me/username
    if raw.startswith("@"):
        url = f"https://t.me/{raw[1:]}"
    elif raw.startswith("https://") or raw.startswith("http://"):
        url = raw
    else:
        url = f"https://t.me/{raw}"

    set_setting("contact", url)
    await update.message.reply_text(
        f"✅ *Contact link update ho gaya!*\n\n🔗 {url}",
        parse_mode="Markdown",
    )
    logger.info(f"Admin set contact: {url}")


# ─────────────────────────────────────────────────────────────────────────────
# /broadcast  (reply to an image with caption, or text-only)
# ─────────────────────────────────────────────────────────────────────────────

@admin_only
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Usage (two ways):
      A) Reply to an image with: /broadcast Your caption here
      B) /broadcast Your text message here  (text-only)

    Sends to ALL registered users.
    Each message is auto-deleted after 24 hours.
    """
    message      = update.message
    bot          = context.bot
    user_ids     = get_all_user_ids()
    caption_text = " ".join(context.args) if context.args else None

    if not user_ids:
        await message.reply_text("⚠️ Koi registered user nahi mila.")
        return

    # ── Determine what to broadcast ─────────────────────────────────────────
    is_photo    = bool(message.reply_to_message and message.reply_to_message.photo)
    is_doc      = bool(message.reply_to_message and message.reply_to_message.document)
    is_video    = bool(message.reply_to_message and message.reply_to_message.video)

    success_count = 0
    fail_count    = 0

    await message.reply_text(
        f"📢 Broadcast shuru ho raha hai... *{len(user_ids)}* users ko bhej raha hoon.",
        parse_mode="Markdown",
    )

    for uid in user_ids:
        try:
            sent_msg = None

            if is_photo:
                photo   = message.reply_to_message.photo[-1].file_id
                sent_msg = await bot.send_photo(
                    chat_id=uid,
                    photo=photo,
                    caption=caption_text,
                    parse_mode="HTML" if caption_text else None,
                )
            elif is_doc:
                doc      = message.reply_to_message.document.file_id
                sent_msg = await bot.send_document(
                    chat_id=uid,
                    document=doc,
                    caption=caption_text,
                    parse_mode="HTML" if caption_text else None,
                )
            elif is_video:
                video    = message.reply_to_message.video.file_id
                sent_msg = await bot.send_video(
                    chat_id=uid,
                    video=video,
                    caption=caption_text,
                    parse_mode="HTML" if caption_text else None,
                )
            else:
                # Text-only broadcast
                text     = caption_text or (message.text.split(None, 1)[1]
                                             if len(message.text.split()) > 1
                                             else "")
                if not text:
                    await message.reply_text(
                        "❌ Text ya image provide karein broadcast ke liye.\n"
                        "Usage: `/broadcast Your message`  ya image reply karein.",
                        parse_mode="Markdown",
                    )
                    return
                sent_msg = await bot.send_message(
                    chat_id=uid,
                    text=text,
                    parse_mode="HTML",
                    disable_web_page_preview=False,
                )

            if sent_msg:
                # Schedule 24-hour auto-delete
                save_broadcast_job(uid, sent_msg.message_id)
                success_count += 1

        except TelegramError as e:
            logger.warning(f"Broadcast failed for user {uid}: {e}")
            fail_count += 1

    # ── Schedule the background cleaner (runs every 5 min) ──────────────────
    # Only register once — check if already running
    existing = context.job_queue.get_jobs_by_name("broadcast_cleaner")
    if not existing:
        context.job_queue.run_repeating(
            _delete_broadcast_job,
            interval=300,   # every 5 min
            first=10,
            name="broadcast_cleaner",
        )

    await message.reply_text(
        f"✅ *Broadcast complete!*\n\n"
        f"📨 Bheja: {success_count}\n"
        f"❌ Failed: {fail_count}\n"
        f"⏳ Yeh messages 24 ghante baad auto-delete ho jayenge.",
        parse_mode="Markdown",
    )
    logger.info(
        f"Broadcast sent by admin {update.effective_user.id}: "
        f"{success_count} success, {fail_count} fail"
    )
