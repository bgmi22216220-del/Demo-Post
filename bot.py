"""
Single-file Telegram Bot — Railway.app compatible
All handlers merged into one file to avoid module import issues.
"""

import json
import logging
import random
from datetime import datetime

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError

from config import (
    BOT_TOKEN, ADMIN_ID, CHANNEL_ID,
    VIDEOS_PER_SESSION, VIDEO_DELETE_SECONDS, BROADCAST_DELETE_SECONDS,
)
from database import (
    upsert_user, update_last_fetch, get_all_user_ids,
    get_user_content, save_user_content, reset_all_content,
    get_setting, set_setting,
    save_broadcast_job, get_pending_broadcast_deletes, mark_broadcast_deleted,
    save_channel_video, get_all_channel_video_ids, clear_channel_videos,
    init_db,
)
from utils.channel_indexer import init_channel_videos_table

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

async def _safe_delete(bot, chat_id: int, msg_ids: list):
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except TelegramError:
            pass


def _is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULED JOBS
# ═══════════════════════════════════════════════════════════════════════════════

async def _delete_videos_job(context: ContextTypes.DEFAULT_TYPE):
    data    = context.job.data
    await _safe_delete(context.bot, data["chat_id"], data["msg_ids"])
    logger.info(f"Auto-deleted {len(data['msg_ids'])} msgs for chat {data['chat_id']}")


async def _delete_broadcast_job(context: ContextTypes.DEFAULT_TYPE):
    jobs = get_pending_broadcast_deletes()
    for job in jobs:
        try:
            await context.bot.delete_message(
                chat_id=job["user_id"],
                message_id=job["message_id"],
            )
        except TelegramError as e:
            logger.warning(f"Broadcast delete failed: {e}")
        finally:
            mark_broadcast_deleted(job["id"])
    if jobs:
        logger.info(f"Cleaned {len(jobs)} broadcast messages.")


# ═══════════════════════════════════════════════════════════════════════════════
# /start  — User command
# ═══════════════════════════════════════════════════════════════════════════════

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    chat_id = update.effective_chat.id
    bot     = context.bot

    # Register user
    upsert_user(user.id, user.username, user.first_name)

    # Delete previous session messages
    prev_key = f"session_{user.id}"
    if prev_key in context.bot_data:
        await _safe_delete(bot, chat_id, context.bot_data[prev_key])
        del context.bot_data[prev_key]

    # Cancel old delete jobs
    for job in context.job_queue.get_jobs_by_name(f"del_{user.id}"):
        job.schedule_removal()

    # Send warning
    warning_msg = await bot.send_message(
        chat_id=chat_id,
        text=(
            "⚠️ *Yeh videos 5 minute baad auto-delete ho jayenge.*\n\n"
            "📥 Download ya Forward disabled hai."
        ),
        parse_mode="Markdown",
    )

    all_delete_ids = [warning_msg.message_id]

    # Check 7-day cache
    cached = get_user_content(user.id)

    if cached:
        stored_ids = json.loads(cached["message_ids"]) \
            if isinstance(cached["message_ids"], str) else cached["message_ids"]
        sent_ids = []
        for ch_id in stored_ids:
            try:
                sent = await bot.copy_message(
                    chat_id=chat_id, from_chat_id=CHANNEL_ID,
                    message_id=ch_id, protect_content=True,
                    disable_notification=True,
                )
                sent_ids.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Cached video {ch_id} failed: {e}")
        all_delete_ids.extend(sent_ids)
    else:
        # Fetch from indexed channel videos
        channel_ids = get_all_channel_video_ids()
        if not channel_ids:
            await bot.send_message(
                chat_id=chat_id,
                text="❌ Pehle admin se /index command chalwao.",
            )
            return

        sample = random.sample(channel_ids, min(VIDEOS_PER_SESSION, len(channel_ids)))
        sent_ids = []
        for ch_id in sample:
            try:
                sent = await bot.copy_message(
                    chat_id=chat_id, from_chat_id=CHANNEL_ID,
                    message_id=ch_id, protect_content=True,
                    disable_notification=True,
                )
                sent_ids.append(sent.message_id)
            except TelegramError as e:
                logger.warning(f"Video {ch_id} failed: {e}")

        all_delete_ids.extend(sent_ids)
        save_user_content(user.id, sample, warning_msg.message_id)
        update_last_fetch(user.id)

    # Save session
    context.bot_data[prev_key] = all_delete_ids

    # Final caption + contact button (permanent — NOT deleted)
    caption = get_setting("caption")
    contact = get_setting("contact")
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📩 Contact Admin", url=contact)]]
    )
    await bot.send_message(
        chat_id=chat_id,
        text=f"📌 *{caption}*",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )

    # Schedule 5-min auto-delete
    context.job_queue.run_once(
        _delete_videos_job,
        when=VIDEO_DELETE_SECONDS,
        data={"chat_id": chat_id, "msg_ids": all_delete_ids},
        name=f"del_{user.id}",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Unauthorized.")
        return
    reset_all_content()
    await update.message.reply_text(
        "✅ *Database reset!* Sab users ko next /start par fresh videos milenge.",
        parse_mode="Markdown",
    )


async def setcaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Unauthorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/setcaption Your caption here`", parse_mode="Markdown")
        return
    caption = " ".join(context.args)
    set_setting("caption", caption)
    await update.message.reply_text(f"✅ Caption set:\n_{caption}_", parse_mode="Markdown")


async def setcontact_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Unauthorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/setcontact @username`", parse_mode="Markdown")
        return
    raw = context.args[0].strip()
    url = f"https://t.me/{raw[1:]}" if raw.startswith("@") else raw
    set_setting("contact", url)
    await update.message.reply_text(f"✅ Contact set: {url}")


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Unauthorized.")
        return

    message   = update.message
    user_ids  = get_all_user_ids()
    caption   = " ".join(context.args) if context.args else None

    if not user_ids:
        await message.reply_text("⚠️ Koi registered user nahi.")
        return

    reply     = message.reply_to_message
    is_photo  = bool(reply and reply.photo)
    is_video  = bool(reply and reply.video)
    is_doc    = bool(reply and reply.document)

    await message.reply_text(f"📢 Broadcasting to *{len(user_ids)}* users...", parse_mode="Markdown")

    ok = fail = 0
    for uid in user_ids:
        try:
            if is_photo:
                sent = await context.bot.send_photo(chat_id=uid, photo=reply.photo[-1].file_id, caption=caption, parse_mode="HTML" if caption else None)
            elif is_video:
                sent = await context.bot.send_video(chat_id=uid, video=reply.video.file_id, caption=caption, parse_mode="HTML" if caption else None)
            elif is_doc:
                sent = await context.bot.send_document(chat_id=uid, document=reply.document.file_id, caption=caption, parse_mode="HTML" if caption else None)
            else:
                text = caption or (message.text.split(None, 1)[1] if len(message.text.split()) > 1 else "")
                if not text:
                    await message.reply_text("❌ Message ya image do broadcast ke liye.")
                    return
                sent = await context.bot.send_message(chat_id=uid, text=text, parse_mode="HTML")
            save_broadcast_job(uid, sent.message_id)
            ok += 1
        except TelegramError as e:
            logger.warning(f"Broadcast fail uid {uid}: {e}")
            fail += 1

    await message.reply_text(
        f"✅ *Done!*\n📨 Sent: {ok}\n❌ Failed: {fail}\n⏳ 24h baad auto-delete.",
        parse_mode="Markdown",
    )


async def index_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Unauthorized.")
        return

    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: `/index <start_id> <end_id>`\nExample: `/index 1 300`",
            parse_mode="Markdown",
        )
        return

    try:
        start_id, end_id = int(args[0]), int(args[1])
    except ValueError:
        await update.message.reply_text("❌ Valid numbers do.")
        return

    clear_channel_videos()
    status = await update.message.reply_text(f"🔍 Scanning {start_id}–{end_id}...")

    found = 0
    for msg_id in range(start_id, end_id + 1):
        try:
            fwd = await context.bot.forward_message(
                chat_id=ADMIN_ID, from_chat_id=CHANNEL_ID,
                message_id=msg_id, disable_notification=True,
            )
            mtype = ("video" if fwd.video else
                     "document" if fwd.document else
                     "photo" if fwd.photo else None)
            if mtype:
                save_channel_video(msg_id, mtype)
                found += 1
            try:
                await context.bot.delete_message(chat_id=ADMIN_ID, message_id=fwd.message_id)
            except TelegramError:
                pass
        except TelegramError:
            continue

    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=status.message_id,
        text=(
            f"✅ *Index complete!*\n"
            f"📹 Found: *{found}* videos\n"
            f"📊 Range: `{start_id}` – `{end_id}`"
        ),
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP & MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def _on_startup(app: Application):
    app.job_queue.run_repeating(
        _delete_broadcast_job, interval=300, first=10, name="broadcast_cleaner"
    )
    logger.info("✅ Broadcast cleaner scheduled.")


def main():
    init_db()
    init_channel_videos_table()
    logger.info("✅ DB ready.")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(_on_startup)
        .build()
    )

    app.add_handler(CommandHandler("start",      start_command))
    app.add_handler(CommandHandler("reset",      reset_command))
    app.add_handler(CommandHandler("setcaption", setcaption_command))
    app.add_handler(CommandHandler("setcontact", setcontact_command))
    app.add_handler(CommandHandler("broadcast",  broadcast_command))
    app.add_handler(CommandHandler("index",      index_command))

    logger.info("🤖 Bot started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
